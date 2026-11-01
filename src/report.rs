//! Owned snapshots and immutable shutdown results.
#![forbid(unsafe_code)]

use crate::{
    SharedError,
    cleanup::CleanupReport,
    service::{Criticality, ServiceId},
    work::ScopeSnapshot,
};
use std::sync::Arc;

/// Current application availability, independent of initialization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Health {
    /// No health assertion is available.
    #[default]
    Unknown,
    /// The declared health condition holds.
    Healthy,
    /// Usable with a known degradation.
    Degraded,
    /// The declared health condition does not hold.
    Unhealthy,
}

/// Application lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    /// Required initialization is pending.
    Starting,
    /// Required initialization has completed.
    Running,
    /// Services are being stopped in dependency order.
    Stopping,
    /// Business work is drained; registered cleanup is executing.
    Cleaning,
    /// All owned work and cleanup have ended.
    Terminated,
}

/// The actual result of a root execution body.
#[derive(Clone, Debug)]
pub enum RootOutcome {
    /// The root returned successfully.
    Succeeded,
    /// The root returned an application error.
    Failed(SharedError),
    /// The root unwound.
    Panicked(Arc<str>),
    /// The root was forcibly cancelled.
    Aborted,
    /// Stop prevented the root from starting.
    NotStarted,
}

/// Snapshot of one service and all its work responsibilities.
#[derive(Clone, Debug)]
pub struct ServiceSnapshot {
    /// Stable service identity.
    pub id: ServiceId,
    /// Failure propagation policy.
    pub criticality: Criticality,
    /// Initialization has met the service's lifetime-specific gate.
    pub initialized: bool,
    /// Last application health assertion.
    pub health: Health,
    /// Stage-specific stop has been requested.
    pub stopping: bool,
    /// Root execution result, available after its captures are released.
    pub outcome: Option<RootOutcome>,
    /// Forced root cancellation occurred, even if destruction subsequently panicked.
    pub forced_abort: bool,
    /// First lifecycle fault, including startup or work failures.
    pub failure: Option<SharedError>,
    /// Owned background responsibilities and bounded recent results.
    pub work: ScopeSnapshot,
    /// A local stopping budget has elapsed.
    pub stop_budget_exceeded: bool,
    /// A backend's asynchronous cleanup was not confirmed.
    pub backend_cleanup_unconfirmed: bool,
    /// Execution-chain errors observed without automatically failing the service.
    pub execution_errors: u64,
    /// Bounded recent execution errors with their source chains.
    pub recent_execution_errors: Vec<SharedError>,
}

/// An owned point-in-time view; updates may be coalesced.
#[derive(Clone, Debug)]
pub struct AppSnapshot {
    /// Monotonically increasing publication revision.
    pub revision: u64,
    /// Current lifecycle phase.
    pub phase: Phase,
    /// Required initialization and current health permit use.
    pub ready: bool,
    /// Aggregated current health.
    pub health: Health,
    /// Per-service responsibility snapshots.
    pub services: Vec<ServiceSnapshot>,
    /// Label of the active cleanup, if any.
    pub current_cleanup: Option<String>,
    /// Number of cleanup actions not yet started.
    pub pending_cleanup: usize,
    /// A shutdown budget expired before actual completion.
    pub budget_exceeded: bool,
    /// The independent lifecycle driver was lost.
    pub driver_lost: bool,
}

/// Actual termination after all tracked execution and cleanup ended.
#[derive(Clone, Debug)]
pub struct ExitReport {
    /// First global stop reason; later failures remain in service reports.
    pub reason: Arc<str>,
    /// Final service results.
    pub services: Vec<ServiceSnapshot>,
    /// Cleanup results in execution order.
    pub cleanup: Vec<CleanupReport>,
    /// Whether a budget conclusion was previously incomplete.
    pub budget_exceeded: bool,
    pub(crate) successful: bool,
}

impl ExitReport {
    /// All tracked execution has ended and all aggregate responsibility was confirmed.
    pub fn is_drained(&self) -> bool {
        true
    }

    /// No required lifecycle fault, forced abort, or cleanup failure occurred.
    pub fn is_success(&self) -> bool {
        self.successful
    }
}

/// Immutable historical conclusion of the shared shutdown budget.
#[derive(Clone, Debug)]
pub enum ShutdownReport {
    /// Actual termination was observed within the budget.
    Completed(Arc<ExitReport>),
    /// The driver continues to own the displayed responsibilities.
    Incomplete(Arc<AppSnapshot>),
}

/// A bounded observation of actual termination; it does not initiate stop.
#[derive(Clone, Debug)]
pub enum DrainObservation {
    /// Actual termination was observed.
    Complete(Arc<ExitReport>),
    /// Work or cleanup remains owned by the driver.
    Pending(Arc<AppSnapshot>),
    /// The driver was lost; this is not a completion certificate.
    DriverLost(Arc<AppSnapshot>),
}
