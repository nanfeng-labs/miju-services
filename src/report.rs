//! Observations and the final report.
//!
//! A [`Snapshot`] is assembled when it is asked for and says what is true at
//! that moment. A [`Report`] exists only after every service has drained and
//! every cleanup action has finished; it is the one completion certificate.
#![forbid(unsafe_code)]

use crate::{
    SharedError,
    scope::{Fault, Kind, Label, WorkCounters},
};
use std::{any::Any, fmt, sync::Arc, time::Duration};

/// What a service says about its own ability to work. Independent of phase.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Health {
    /// Nothing has been declared yet.
    #[default]
    Unknown,
    /// Working normally.
    Healthy,
    /// Working with reduced capability.
    Degraded,
    /// Unable to do its job.
    Unhealthy,
}

/// Where the application is in its lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    /// Required services are still initializing.
    Starting,
    /// Every required service is initialized.
    Running,
    /// Stop was requested; services drain in dependency order.
    Stopping,
    /// Every service has drained; cleanup actions are running.
    Cleaning,
    /// Cleanup has finished and the report exists.
    Terminated,
    /// The lifecycle driver is gone. Completion is unknown.
    Lost,
}

/// The application's state in three values, pushed on lifecycle transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Status {
    /// Lifecycle phase.
    pub phase: Phase,
    /// Running, with every required service initialized and healthy.
    pub ready: bool,
    /// Aggregate health.
    pub health: Health,
}

/// Where one service is in its lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePhase {
    /// Its providers are not initialized yet.
    Waiting,
    /// Its root is running and has not reported ready.
    Starting,
    /// Initialized.
    Running,
    /// Asked to stop; its root is still running.
    Stopping,
    /// Its root has finished; work or leases remain.
    Draining,
    /// Its root, its work and its leases have all released.
    Done,
}

/// How a service's root ended.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// It returned `Ok`.
    Succeeded,
    /// It returned this error.
    Failed(SharedError),
    /// It panicked.
    Panicked(Arc<str>),
    /// It was aborted after its grace period.
    Aborted,
    /// It was never invoked: stop or a provider's failure came first.
    NotStarted,
}

/// Something that makes a service's lifecycle unsuccessful.
#[derive(Clone, Debug)]
pub enum Failure {
    /// The root returned this error.
    Error(SharedError),
    /// The root panicked.
    Panic(Arc<str>),
    /// A resident root returned before it was asked to stop.
    ExitedEarly,
    /// A resident root did not report ready in time.
    StartupTimeout,
    /// Critical work failed.
    Work(Fault),
    /// A destructor panicked while the root was being released.
    ReleasePanic,
    /// The root was aborted after its grace period.
    Forced,
}

/// One responsibility that has not been released.
#[derive(Clone, Debug)]
pub struct Outstanding {
    /// The service that owns it.
    pub service: Arc<str>,
    /// What kind of holder it is.
    pub kind: Kind,
    /// Its name.
    pub name: Label,
    /// For blocking work, whether the closure has started.
    pub started: bool,
    /// Set when the holder stated it cannot prove release.
    pub unconfirmed: Option<Arc<str>>,
}

impl fmt::Display for Outstanding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?} `{}`", self.service, self.kind, self.name)?;
        if self.kind == Kind::Blocking {
            f.write_str(if self.started {
                " (started)"
            } else {
                " (queued)"
            })?;
        }
        if let Some(reason) = &self.unconfirmed {
            write!(f, " (unconfirmed: {reason})")?;
        }
        Ok(())
    }
}

/// Typed observations an adapter or a service published.
#[derive(Clone, Default)]
pub(crate) struct Detail(pub(crate) Option<Arc<dyn Any + Send + Sync>>);

impl fmt::Debug for Detail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "Detail(..)"
        } else {
            "Detail(None)"
        })
    }
}

/// One service, as observed or as it ended.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceSnapshot {
    /// The registered name.
    pub name: Arc<str>,
    /// Whether a failure of this service stops the application.
    pub required: bool,
    /// Whether the root is expected to finish by itself.
    pub finite: bool,
    /// Lifecycle position.
    pub phase: ServicePhase,
    /// Initialization has completed. For a finite service: it succeeded and drained.
    pub initialized: bool,
    /// Declared health.
    pub health: Health,
    /// How the root ended, once it has.
    pub outcome: Option<Outcome>,
    /// Everything that makes this service's lifecycle unsuccessful.
    pub failures: Vec<Failure>,
    /// How its accepted work has ended so far.
    pub work: WorkCounters,
    /// The most recent work faults, oldest first.
    pub recent_faults: Vec<Fault>,
    /// Whether its grace period expired and an abort was sent.
    pub forced: bool,
    /// Some of the responsibilities still held; see `outstanding_total`.
    pub outstanding: Vec<Outstanding>,
    /// The authoritative number of responsibilities still held.
    pub outstanding_total: u64,
    pub(crate) detail: Detail,
}

impl ServiceSnapshot {
    /// An observation published with `Context::publish`, if it has this type.
    pub fn detail<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.detail.0.as_deref()?.downcast_ref()
    }
}

/// The result of one cleanup action that actually finished.
#[derive(Clone, Debug)]
pub struct CleanupReport {
    /// The name given to `defer`.
    pub name: Label,
    /// The service that registered it.
    pub service: Arc<str>,
    /// The error it returned, if any.
    pub error: Option<SharedError>,
    /// Its panic, if it panicked.
    pub panic: Option<Arc<str>>,
}

impl CleanupReport {
    /// Whether the action returned `Ok` without panicking.
    pub fn is_success(&self) -> bool {
        self.error.is_none() && self.panic.is_none()
    }
}

/// What is true now. Per service it is coherent; across services it is not an
/// atomic cut while the application is running.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Snapshot {
    /// Phase, readiness and health.
    pub status: Status,
    /// Every service, in registration order.
    pub services: Vec<ServiceSnapshot>,
    /// Cleanup actions that have finished.
    pub cleanup: Vec<CleanupReport>,
    /// The cleanup action running now.
    pub current_cleanup: Option<(Arc<str>, Label)>,
    /// Cleanup actions registered and not yet run, in the order they will run.
    pub pending_cleanup: Vec<(Arc<str>, Label)>,
}

impl Snapshot {
    /// Responsibilities not yet released: service work first, then cleanup.
    /// Each service lists a bounded number; `outstanding_total` counts all.
    pub fn outstanding(&self) -> Vec<Outstanding> {
        let cleanup = self
            .current_cleanup
            .iter()
            .map(|action| (action, true))
            .chain(self.pending_cleanup.iter().map(|action| (action, false)))
            .map(|((service, name), started)| Outstanding {
                service: service.clone(),
                kind: Kind::Cleanup,
                name: name.clone(),
                started,
                unconfirmed: None,
            });
        self.services
            .iter()
            .flat_map(|service| service.outstanding.iter().cloned())
            .chain(cleanup)
            .collect()
    }

    /// The authoritative number of responsibilities not yet released.
    pub fn outstanding_total(&self) -> u64 {
        let services: u64 = self
            .services
            .iter()
            .map(|service| service.outstanding_total)
            .sum();
        services + self.pending_cleanup.len() as u64 + u64::from(self.current_cleanup.is_some())
    }

    /// Look a service up by name.
    pub fn service(&self, name: &str) -> Option<&ServiceSnapshot> {
        self.services.iter().find(|service| &*service.name == name)
    }
}

/// Proof that every service drained and every cleanup action finished.
/// Whether that went well is a separate question: see [`Report::is_success`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Report {
    /// Every service as it ended, in registration order.
    pub services: Vec<ServiceSnapshot>,
    /// Every cleanup action, in the order it ran.
    pub cleanup: Vec<CleanupReport>,
    /// From start to the end of cleanup.
    pub elapsed: Duration,
}

impl Report {
    /// True when every required service ended without a failure, every
    /// cleanup action succeeded, and nothing had to be aborted.
    pub fn is_success(&self) -> bool {
        self.cleanup.iter().all(CleanupReport::is_success)
            && self.services.iter().all(|service| {
                !service.forced && (!service.required || service.failures.is_empty())
            })
    }

    /// Every recorded failure with the service it belongs to.
    pub fn failures(&self) -> Vec<(Arc<str>, Failure)> {
        self.services
            .iter()
            .flat_map(|service| {
                service
                    .failures
                    .iter()
                    .map(|failure| (service.name.clone(), failure.clone()))
            })
            .collect()
    }

    /// Look a service up by name.
    pub fn service(&self, name: &str) -> Option<&ServiceSnapshot> {
        self.services.iter().find(|service| &*service.name == name)
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} after {:?}: {} service(s), {} cleanup action(s)",
            if self.is_success() {
                "completed"
            } else {
                "completed with failures"
            },
            self.elapsed,
            self.services.len(),
            self.cleanup.len()
        )?;
        for (service, failure) in self.failures() {
            write!(f, "; {service}: {failure:?}")?;
        }
        for action in self.cleanup.iter().filter(|action| !action.is_success()) {
            write!(f, "; cleanup `{}` failed", action.name)?;
        }
        Ok(())
    }
}
