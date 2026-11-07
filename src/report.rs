//! Immutable state and completion results. A stop request is never a completion certificate.
#![forbid(unsafe_code)]

use crate::{
    SharedError,
    cleanup::CleanupReport,
    service::{Criticality, Lifetime, ServiceId},
    work::{ScopeSnapshot, WorkReport},
};
use std::{sync::Arc, time::Duration};

/// Application-reported health, independent of initialization and phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Health {
    /// No usable health declaration yet.
    #[default]
    Unknown,
    /// Ready for normal operation.
    Healthy,
    /// Operational with reduced capability.
    Degraded,
    /// Unable to perform the intended operation.
    Unhealthy,
}

/// Current application lifecycle phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Required initialization has not completed.
    Starting,
    /// All required initialization gates have completed.
    Running,
    /// Stop was requested; dependency-ordered drain is in progress.
    Stopping,
    /// All service responsibility released; cleanup is in progress.
    Cleaning,
    /// Final report published after all cleanup actually finished.
    Terminated,
}

/// Current position of a service root and its work scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServicePhase {
    /// Dependencies have not admitted the entry, or stop froze it before admission.
    NotStarted,
    /// The entry is initializing.
    Starting,
    /// Initialization completed; the root may still run.
    Running,
    /// Local stop is active or the released root still owns work.
    Draining,
    /// The root and all accepted responsibility have released.
    Completed,
}

/// Root execution result, published only after its captures release.
#[derive(Clone, Debug)]
pub enum ServiceOutcome {
    /// The entry returned Ok.
    Succeeded,
    /// The original business error.
    Failed(SharedError),
    /// The entry unwound.
    Panicked(Arc<str>),
    /// Stop arrived before the entry was invoked.
    CancelledBeforeStart,
    /// A started root was forcibly dropped after grace.
    Aborted,
}

/// A lifecycle fault associated with a service.
#[derive(Clone, Debug)]
pub enum ServiceFailure {
    /// Root execution failed.
    Error(SharedError),
    /// Root invocation or polling panicked.
    Panic(Arc<str>),
    /// A resident returned before its own stop request.
    UnexpectedExit,
    /// A Resident did not report readiness within the configured budget.
    StartupTimeout,
    /// A FailService work entry failed.
    Work(Arc<WorkReport>),
    /// Releasing the root's captures panicked.
    ReleasePanic(Arc<str>),
    /// The root needed forced cancellation.
    Aborted,
}

/// One service's latest state, also retained in the final report.
#[derive(Clone, Debug)]
pub struct ServiceSnapshot {
    /// Periodic invocation counters when this service uses the periodic adapter.
    #[cfg(feature = "periodic")]
    pub periodic: Option<crate::periodic::PeriodicSnapshot>,
    /// Nonfatal miju job errors observed, saturated at u64::MAX.
    #[cfg(feature = "miju")]
    pub execution_errors: u64,
    /// Recent miju job errors with their original source chains.
    #[cfg(feature = "miju")]
    pub recent_execution_errors: Vec<SharedError>,
    /// Registered identity.
    pub id: ServiceId,
    /// Failure propagation policy.
    pub criticality: Criticality,
    /// Expected root lifetime.
    pub lifetime: Lifetime,
    /// Root/scope progress.
    pub phase: ServicePhase,
    /// Latched initialization; finite services require successful actual drain.
    pub initialized: bool,
    /// Effective health; successful finite services contribute Healthy.
    pub health: Health,
    /// Root result, when its captures have released.
    pub outcome: Option<ServiceOutcome>,
    /// Lifecycle faults, separate from nonfatal recorded work errors.
    pub failures: Vec<ServiceFailure>,
    /// Work still owned by this service and its bounded recent results.
    pub work: ScopeSnapshot,
    /// Whether local stop has reached this service.
    pub stopping: bool,
    /// Whether this service has ever exceeded its stop confirmation budget.
    pub stop_budget_exceeded: bool,
}

/// A coherent application observation. It does not authorize resource reuse.
#[derive(Clone, Debug)]
pub struct AppSnapshot {
    /// Lifecycle phase.
    pub phase: Phase,
    /// Current traffic-admission readiness.
    pub ready: bool,
    /// Aggregate health, distinct from readiness and phase.
    pub health: Health,
    /// Registered services in registration order.
    pub services: Vec<ServiceSnapshot>,
    /// Cleanup action currently executing, if any.
    pub current_cleanup: Option<String>,
    /// Cleanup actions not yet started, in execution order.
    pub pending_cleanup: Vec<String>,
    /// Completed cleanup actions.
    pub cleanup: Vec<CleanupReport>,
    /// First-stop confirmation budget was exceeded; this fact never resets.
    pub budget_exceeded: bool,
    /// The runtime dropped the independent lifecycle driver before final completion.
    pub driver_lost: bool,
}

/// Final actual completion certificate for this application instance.
#[derive(Clone, Debug)]
pub struct ExitReport {
    /// Terminal snapshot after all owned work and cleanup release.
    pub snapshot: Arc<AppSnapshot>,
    /// Total elapsed application lifetime.
    pub elapsed: Duration,
}

impl ExitReport {
    /// A final report is emitted only after actual drain, including cleanup.
    pub fn is_drained(&self) -> bool {
        true
    }

    /// Whether required lifecycles and every cleanup succeeded without forced abort.
    ///
    /// Ordinary optional faults and recorded work errors remain visible without
    /// failing the whole lifecycle. Historical budget overrun is not erased.
    pub fn is_success(&self) -> bool {
        self.snapshot.cleanup.iter().all(CleanupReport::is_success)
            && self.snapshot.services.iter().all(|service| {
                (service.criticality == Criticality::Optional || service.failures.is_empty())
                    && !matches!(service.outcome, Some(ServiceOutcome::Aborted))
                    && !service.work.was_aborted
            })
    }
}
