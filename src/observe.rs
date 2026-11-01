//! Cancellation-safe observation without owning the lifecycle driver.
#![forbid(unsafe_code)]

use crate::{
    AppSnapshot, Criticality, DrainObservation, ExitReport, Phase, ShutdownReport, util::lock,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

/// The lifecycle driver disappeared without producing an actual exit report.
#[derive(Clone, Debug, thiserror::Error)]
#[error("application lifecycle driver lost")]
pub struct DriverLost;

/// Initialization observation did not find a usable application.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadyError {
    /// Stop was requested before this observation found readiness.
    #[error("application stopped before readiness")]
    StoppedBeforeReady,
    /// A required service failed.
    #[error("required service {0} failed initialization")]
    ServiceFailed(crate::ServiceId),
    /// The observation deadline elapsed; initialization continues.
    #[error("readiness observation timed out")]
    Timeout,
    /// No driver remains to advance initialization.
    #[error(transparent)]
    DriverLost(#[from] DriverLost),
}

#[derive(Clone, Debug)]
pub(crate) struct Observation {
    pub snapshot: Arc<AppSnapshot>,
    pub budget: Option<Arc<ShutdownReport>>,
    pub exit: Option<Arc<ExitReport>>,
    pub lost: bool,
}

pub(crate) struct Shared {
    pub sender: watch::Sender<Arc<Observation>>,
    pub stop: Mutex<Option<(Instant, Arc<str>)>>,
    pub notify: Arc<Notify>,
}

impl Shared {
    pub(crate) fn request_stop(&self, reason: Arc<str>) {
        let mut stop = lock(&self.stop);
        if stop.is_none() {
            *stop = Some((Instant::now(), reason));
        }
        drop(stop);
        self.notify.notify_one();
    }

    pub(crate) fn stop_info(&self) -> Option<(Instant, Arc<str>)> {
        lock(&self.stop).clone()
    }

    pub(crate) fn publish(&self, value: Observation) {
        let previous = self.sender.send_replace(Arc::new(value));
        drop(previous); // User errors and other captured values are released outside watch's lock.
    }
}

/// Read-only observation that does not keep an application control lease alive.
#[derive(Clone)]
pub struct AppObserver {
    pub(crate) shared: Arc<Shared>,
    pub(crate) receiver: watch::Receiver<Arc<Observation>>,
}

impl AppObserver {
    /// Read an owned current snapshot; stop is immediately reflected in readiness.
    pub fn snapshot(&self) -> Arc<AppSnapshot> {
        let current = self.receiver.borrow().snapshot.clone();
        if self.shared.stop_info().is_some() && current.ready {
            let mut value = (*current).clone();
            value.ready = false;
            Arc::new(value)
        } else {
            current
        }
    }

    /// Subscribe to coalesced owned snapshots, without exposing a watch lock guard.
    pub fn subscribe(&self) -> SnapshotReceiver {
        SnapshotReceiver {
            observer: self.clone(),
            receiver: self.receiver.clone(),
        }
    }

    /// Observe readiness with a separate observation budget; does not cancel startup.
    pub async fn ready(&self, within: Duration) -> Result<Arc<AppSnapshot>, ReadyError> {
        let mut receiver = self.receiver.clone();
        let wait = async {
            loop {
                let state = receiver.borrow_and_update().clone();
                let snapshot = self.snapshot();
                if let Some(failed) = snapshot
                    .services
                    .iter()
                    .find(|s| s.criticality == Criticality::Required && s.failure.is_some())
                {
                    return Err(ReadyError::ServiceFailed(failed.id.clone()));
                }
                if self.shared.stop_info().is_some()
                    || matches!(
                        snapshot.phase,
                        Phase::Stopping | Phase::Cleaning | Phase::Terminated
                    )
                {
                    return Err(ReadyError::StoppedBeforeReady);
                }
                if snapshot.ready {
                    return Ok(snapshot);
                }
                if state.lost {
                    return Err(DriverLost.into());
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| ReadyError::DriverLost(DriverLost))?;
            }
        };
        tokio::time::timeout(within, wait)
            .await
            .unwrap_or(Err(ReadyError::Timeout))
    }

    /// Wait for actual termination. Cancelling this future does not cancel shutdown.
    pub async fn wait(&self) -> Result<Arc<ExitReport>, DriverLost> {
        let mut receiver = self.receiver.clone();
        loop {
            let state = receiver.borrow_and_update().clone();
            if let Some(exit) = &state.exit {
                return Ok(exit.clone());
            }
            if state.lost {
                return Err(DriverLost);
            }
            receiver.changed().await.map_err(|_| DriverLost)?;
        }
    }

    /// Observe actual drain for a bounded duration without initiating stop.
    pub async fn drain(&self, within: Duration) -> DrainObservation {
        match tokio::time::timeout(within, self.wait()).await {
            Ok(Ok(exit)) => DrainObservation::Complete(exit),
            Ok(Err(_)) => DrainObservation::DriverLost(self.snapshot()),
            Err(_) => DrainObservation::Pending(self.snapshot()),
        }
    }

    pub(crate) async fn budget(&self) -> Result<Arc<ShutdownReport>, DriverLost> {
        let mut receiver = self.receiver.clone();
        loop {
            let state = receiver.borrow_and_update().clone();
            if let Some(budget) = &state.budget {
                return Ok(budget.clone());
            }
            if state.lost {
                return Err(DriverLost);
            }
            receiver.changed().await.map_err(|_| DriverLost)?;
        }
    }
}

/// Coalesced change observation; slow readers never block lifecycle progress.
pub struct SnapshotReceiver {
    observer: AppObserver,
    receiver: watch::Receiver<Arc<Observation>>,
}

impl SnapshotReceiver {
    /// Latest owned snapshot.
    pub fn current(&self) -> Arc<AppSnapshot> {
        self.observer.snapshot()
    }

    /// Next publication, or None after a terminal/lost publication was consumed.
    pub async fn changed(&mut self) -> Option<Arc<AppSnapshot>> {
        if !self.receiver.has_changed().unwrap_or(false) {
            let current = self.receiver.borrow();
            if current.exit.is_some() || current.lost {
                return None;
            }
        }
        self.receiver.changed().await.ok()?;
        Some(self.current())
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("stopping", &self.stop_info().is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for AppObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppObserver")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SnapshotReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotReceiver")
            .field("current", &self.current())
            .finish_non_exhaustive()
    }
}
