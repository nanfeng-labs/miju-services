//! Control and observation of a started application.
//!
//! The lifecycle driver runs independently. Waiting on a handle, or dropping
//! the wait, never affects it. Only two things end a wait for completion: the
//! final [`Report`], or the loss of the driver.
#![forbid(unsafe_code)]

use crate::{
    cleanup::Cleanup,
    report::{Health, Outstanding, Phase, Report, ServicePhase, ServiceSnapshot, Snapshot, Status},
    service::ServiceCell,
};
use std::{
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

/// How many outstanding items one service lists in a snapshot.
const LISTED: usize = 64;

/// What the driver pushes on lifecycle transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Published {
    pub(crate) status: Status,
    /// A required service has failed.
    pub(crate) required_failed: bool,
}

/// State shared by the driver, the handles and every service context.
pub(crate) struct Core {
    pub(crate) published: watch::Sender<Published>,
    stop: AtomicBool,
    /// The driver's private wake. `notify_one` stores a permit if it is busy.
    wake: Arc<Notify>,
    pub(crate) services: Vec<Arc<ServiceCell>>,
    pub(crate) cleanup: Cleanup,
    pub(crate) report: OnceLock<Arc<Report>>,
    lost: AtomicBool,
    pub(crate) started: Instant,
}

impl Core {
    pub(crate) fn new(services: Vec<Arc<ServiceCell>>, wake: Arc<Notify>) -> Arc<Self> {
        let (published, _) = watch::channel(Published {
            status: Status {
                phase: Phase::Starting,
                ready: false,
                health: Health::Unknown,
            },
            required_failed: false,
        });
        Arc::new(Self {
            published,
            stop: AtomicBool::new(false),
            wake,
            services,
            cleanup: Cleanup::default(),
            report: OnceLock::new(),
            lost: AtomicBool::new(false),
            started: Instant::now(),
        })
    }

    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    pub(crate) fn wake_handle(&self) -> &Notify {
        &self.wake
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// Record the request once, withdraw readiness at once, wake the driver.
    pub(crate) fn request_stop(&self) {
        if self.stop.swap(true, Ordering::AcqRel) {
            return;
        }
        self.published.send_if_modified(|published| {
            let status = &mut published.status;
            let before = *status;
            status.ready = false;
            if matches!(status.phase, Phase::Starting | Phase::Running) {
                status.phase = Phase::Stopping;
            }
            before != *status
        });
        self.wake();
    }

    pub(crate) fn mark_lost(&self) {
        self.lost.store(true, Ordering::Release);
        self.published.send_modify(|published| {
            published.status.phase = Phase::Lost;
            published.status.ready = false;
        });
    }

    fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    pub(crate) fn status(&self) -> Status {
        self.published.borrow().status
    }

    pub(crate) fn service_snapshot(cell: &ServiceCell) -> ServiceSnapshot {
        let drained = cell.scope.is_drained();
        let stopping = cell.scope.stop_token().is_cancelled();
        let listed = cell.scope.list(LISTED);
        let total = cell.scope.count();
        let state = cell.state();
        let phase = if state.not_started || (state.started && drained) {
            ServicePhase::Done
        } else if !state.started {
            ServicePhase::Waiting
        } else if state.root.is_some() {
            ServicePhase::Draining
        } else if stopping {
            ServicePhase::Stopping
        } else if state.initialized {
            ServicePhase::Running
        } else {
            ServicePhase::Starting
        };
        ServiceSnapshot {
            name: cell.name.clone(),
            required: cell.required,
            finite: cell.finite,
            phase,
            initialized: state.initialized,
            health: state.health,
            outcome: if state.not_started {
                Some(crate::report::Outcome::NotStarted)
            } else {
                state.root.as_ref().map(|root| root.outcome.clone())
            },
            failures: state.failures.clone(),
            work: cell.scope.counters(),
            recent_faults: cell.scope.recent_faults(),
            forced: cell.scope.was_forced(),
            outstanding: listed
                .into_iter()
                .map(|item| Outstanding {
                    service: cell.name.clone(),
                    kind: item.kind,
                    name: item.label,
                    started: item.started,
                    unconfirmed: item.unconfirmed,
                })
                .collect(),
            outstanding_total: total,
            detail: cell
                .detail
                .lock()
                .expect("detail runs no user code")
                .clone(),
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let (cleanup, current_cleanup, pending_cleanup) = self.cleanup.observe();
        Snapshot {
            status: self.status(),
            services: self
                .services
                .iter()
                .map(|cell| Self::service_snapshot(cell))
                .collect(),
            cleanup,
            current_cleanup,
            pending_cleanup,
        }
    }
}

/// The driver is gone and no report exists. Completion is unknown; this is
/// the last state that could be observed.
#[derive(Clone, Debug, thiserror::Error)]
#[error("the lifecycle driver was lost before completion")]
pub struct Lost {
    /// The last observable state. Outstanding items may still be held.
    pub snapshot: Box<Snapshot>,
}

/// Why the application will not become ready.
#[derive(Clone, Debug, thiserror::Error)]
pub enum NotReady {
    /// A required service failed first.
    #[error("a required service failed before the application was ready")]
    Failed(Box<Snapshot>),
    /// Stop was requested, or the application ended, first.
    #[error("the application stopped before it was ready")]
    Stopped(Box<Snapshot>),
    /// The driver is gone.
    #[error(transparent)]
    Lost(#[from] Lost),
}

/// Observes an application without holding a control lease on it.
#[derive(Clone)]
pub struct Observer {
    core: Arc<Core>,
}

impl fmt::Debug for Observer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observer")
            .field("status", &self.status())
            .finish()
    }
}

impl Observer {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Phase, readiness and health, as last pushed by the driver.
    pub fn status(&self) -> Status {
        self.core.status()
    }

    /// A detailed view assembled now, including what is still outstanding.
    pub fn snapshot(&self) -> Snapshot {
        self.core.snapshot()
    }

    /// Resolves when the status next changes.
    pub async fn changed(&self) -> Status {
        let mut published = self.core.published.subscribe();
        published.mark_unchanged();
        let _ = published.changed().await;
        self.status()
    }

    /// Resolves when the application is ready, or with the reason it will not
    /// be. Dropping the wait has no effect; bound it with a timeout if needed.
    pub async fn ready(&self) -> Result<(), NotReady> {
        let mut published = self.core.published.subscribe();
        loop {
            let current = *published.borrow_and_update();
            if self.core.is_lost() {
                return Err(self.lost().into());
            }
            if current.status.ready {
                return Ok(());
            }
            if current.required_failed {
                return Err(NotReady::Failed(Box::new(self.snapshot())));
            }
            if !matches!(current.status.phase, Phase::Starting | Phase::Running) {
                return Err(NotReady::Stopped(Box::new(self.snapshot())));
            }
            if published.changed().await.is_err() {
                return Err(self.lost().into());
            }
        }
    }

    /// Resolves only at actual completion, or when the driver is lost.
    ///
    /// A report is not a verdict: check [`Report::is_success`]. Dropping the
    /// wait changes nothing; a later call resolves the same way.
    pub async fn wait(&self) -> Result<Arc<Report>, Lost> {
        let mut published = self.core.published.subscribe();
        loop {
            published.mark_unchanged();
            if let Some(report) = self.core.report.get() {
                return Ok(report.clone());
            }
            if self.core.is_lost() {
                return Err(self.lost());
            }
            if published.changed().await.is_err() {
                return Err(self.lost());
            }
        }
    }

    fn lost(&self) -> Lost {
        Lost {
            snapshot: Box::new(self.snapshot()),
        }
    }
}

struct Control {
    core: Arc<Core>,
}

impl Drop for Control {
    fn drop(&mut self) {
        self.core.request_stop();
    }
}

/// Controls a started application. Dropping the last clone requests stop.
#[derive(Clone)]
pub struct AppHandle {
    control: Arc<Control>,
}

impl fmt::Debug for AppHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppHandle")
            .field("status", &self.status())
            .finish()
    }
}

impl AppHandle {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self {
            control: Arc::new(Control { core }),
        }
    }

    /// A handle that observes without keeping the application running.
    pub fn observer(&self) -> Observer {
        Observer::new(self.control.core.clone())
    }

    /// See [`Observer::status`].
    pub fn status(&self) -> Status {
        self.control.core.status()
    }

    /// See [`Observer::snapshot`].
    pub fn snapshot(&self) -> Snapshot {
        self.control.core.snapshot()
    }

    /// See [`Observer::changed`].
    pub async fn changed(&self) -> Status {
        self.observer().changed().await
    }

    /// See [`Observer::ready`].
    pub async fn ready(&self) -> Result<(), NotReady> {
        self.observer().ready().await
    }

    /// Request dependency-ordered stop. Readiness is withdrawn at once.
    /// Repeated calls change nothing. This is a request, not completion.
    pub fn stop(&self) {
        self.control.core.request_stop();
    }

    /// See [`Observer::wait`].
    pub async fn wait(&self) -> Result<Arc<Report>, Lost> {
        self.observer().wait().await
    }

    /// Request stop and wait for actual completion. No deadline.
    pub async fn shutdown(&self) -> Result<Arc<Report>, Lost> {
        self.stop();
        self.wait().await
    }
}
