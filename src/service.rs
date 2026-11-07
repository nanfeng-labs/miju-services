//! Service entry points, initialization, and explicit dependency policy.
//!
//! A service owns its context and accepted [`WorkScope`]. Pass settings and resources
//! through an ordinary constructor or closure; this module does not look them up.
#![forbid(unsafe_code)]

use crate::{
    BoxError,
    cleanup::{CleanupId, CleanupRejected, Registry},
    observe::AppObserver,
    report::Health,
    work::WorkScope,
};
use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// A service entry point consumed once by an [`App`](crate::App).
pub trait Service: Send + 'static {
    /// Business or initialization failure retained in the final report.
    type Error: Into<BoxError> + Send + 'static;

    /// Initialize, report readiness, and run until the service's work is finished.
    fn run(self, context: ServiceContext) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A function adapted to [`Service`], retaining its concrete error type.
pub struct ServiceFn<F>(F);

impl<F> fmt::Debug for ServiceFn<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceFn").finish_non_exhaustive()
    }
}

/// Adapt a named function or closure whose error converts into [`BoxError`].
pub fn service_fn<F, Fut, E>(run: F) -> ServiceFn<F>
where
    F: FnOnce(ServiceContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    ServiceFn(run)
}

impl<F, Fut, E> Service for ServiceFn<F>
where
    F: FnOnce(ServiceContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    type Error = E;

    fn run(self, context: ServiceContext) -> impl Future<Output = Result<(), E>> + Send {
        (self.0)(context)
    }
}

/// A stable name within one application instance.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceId(pub(crate) Arc<str>);

impl ServiceId {
    /// The registered name.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Whether a fault stops the entire application or only this service.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Criticality {
    /// Faults request application stop; only required services may be providers.
    #[default]
    Required,
    /// Faults locally stop this service and degrade application health.
    Optional,
}

/// The expected lifetime of a service root.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Lifetime {
    /// Returning before stop is an unexpected exit.
    #[default]
    Resident,
    /// Successful root and work completion initialize dependents.
    Finite,
}

/// Advanced registration policy. A plain name supplies all defaults.
#[derive(Clone, Debug)]
pub struct ServiceSpec {
    pub(crate) id: ServiceId,
    pub(crate) criticality: Criticality,
    pub(crate) lifetime: Lifetime,
    pub(crate) dependencies: Vec<String>,
    pub(crate) max_work: NonZeroUsize,
}

impl ServiceSpec {
    /// Register a required resident with capacity for 1024 work responsibilities.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: ServiceId(Arc::from(name.into())),
            criticality: Criticality::Required,
            lifetime: Lifetime::Resident,
            dependencies: Vec::new(),
            max_work: NonZeroUsize::new(1024).expect("1024 is nonzero"),
        }
    }

    /// Choose whether this service is required for application operation.
    #[must_use]
    pub fn criticality(mut self, criticality: Criticality) -> Self {
        self.criticality = criticality;
        self
    }

    /// Choose a resident or finite root.
    #[must_use]
    pub fn lifetime(mut self, lifetime: Lifetime) -> Self {
        self.lifetime = lifetime;
        self
    }

    /// Require the named provider's initialization and retain it until this service drains.
    #[must_use]
    pub fn depends_on(mut self, provider: impl Into<String>) -> Self {
        self.dependencies.push(provider.into());
        self
    }

    /// Bound queued, running, and unconfirmed work together.
    #[must_use]
    pub fn max_work(mut self, capacity: NonZeroUsize) -> Self {
        self.max_work = capacity;
        self
    }
}

impl From<&str> for ServiceSpec {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for ServiceSpec {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

pub(crate) struct ContextState {
    pub(crate) initialized: bool,
    pub(crate) health: Health,
    pub(crate) declared_health: bool,
    pub(crate) root_released: bool,
    #[cfg(feature = "periodic")]
    pub(crate) periodic: Option<crate::periodic::PeriodicSnapshot>,
    #[cfg(feature = "miju")]
    pub(crate) execution_errors: u64,
    #[cfg(feature = "miju")]
    pub(crate) recent_execution_errors: std::collections::VecDeque<crate::SharedError>,
}

pub(crate) struct ContextShared {
    pub(crate) state: Mutex<ContextState>,
    pub(crate) stop: CancellationToken,
    pub(crate) changed: Arc<Notify>,
    #[cfg(feature = "miju")]
    pub(crate) history_limit: usize,
}

/// Readiness, local stop, work ownership, and initialization cleanup for one service.
#[derive(Clone)]
pub struct ServiceContext {
    pub(crate) id: ServiceId,
    pub(crate) shared: Arc<ContextShared>,
    pub(crate) work: WorkScope,
    pub(crate) observer: AppObserver,
    pub(crate) cleanup: Arc<Registry>,
}

impl fmt::Debug for ServiceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceContext")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl ServiceContext {
    #[cfg(feature = "periodic")]
    pub(crate) fn update_periodic(&self, snapshot: crate::periodic::PeriodicSnapshot) {
        self.shared.state.lock().unwrap().periodic = Some(snapshot);
        self.shared.changed.notify_waiters();
    }

    #[cfg(feature = "miju")]
    pub(crate) fn record_execution_error(&self, error: crate::SharedError) {
        let mut state = self.shared.state.lock().unwrap();
        state.execution_errors = state.execution_errors.saturating_add(1);
        state.recent_execution_errors.push_back(error);
        let evicted = if state.recent_execution_errors.len() > self.shared.history_limit {
            state.recent_execution_errors.pop_front()
        } else {
            None
        };
        drop(state);
        drop(evicted);
        self.shared.changed.notify_waiters();
    }

    /// This service's identity.
    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    /// Observe the application without keeping a control lease alive.
    pub fn observer(&self) -> AppObserver {
        self.observer.clone()
    }

    /// Confirm real initialization; use Healthy unless health was explicitly declared.
    ///
    /// Repeated calls are harmless. Notifications after local stop or root release
    /// are ignored, including normal stop races during initialization.
    pub fn ready(&self) {
        let mut state = self.shared.state.lock().unwrap();
        if self.shared.stop.is_cancelled() || state.root_released {
            return;
        }
        state.initialized = true;
        if !state.declared_health {
            state.health = Health::Healthy;
        }
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Update current health without implicitly stopping or restarting services.
    pub fn set_health(&self, health: Health) {
        let mut state = self.shared.state.lock().unwrap();
        if self.shared.stop.is_cancelled() || state.root_released {
            return;
        }
        state.declared_health = true;
        state.health = health;
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Whether local stop has been requested.
    pub fn is_stopping(&self) -> bool {
        self.shared.stop.is_cancelled()
    }

    /// Wait for local stop; cancellation of this observation has no side effect.
    pub async fn stopping(&self) {
        self.shared.stop.cancelled().await;
    }

    /// Work admitted under this service's lifetime and capacity.
    pub fn work(&self) -> &WorkScope {
        &self.work
    }

    /// Transfer cleanup during initialization, including interrupted initialization.
    ///
    /// After readiness or root release this returns the original closure. The
    /// caller retains responsibility for rejected cleanup; no async close runs here.
    pub fn defer_cleanup<F, Fut, E>(
        &self,
        label: impl Into<String>,
        close: F,
    ) -> Result<CleanupId, CleanupRejected<F>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let label = label.into();
        let state = self.shared.state.lock().unwrap();
        if state.initialized || state.root_released {
            drop(state);
            return Err(CleanupRejected::new(label, close));
        }
        let result = self.cleanup.register(label, close);
        drop(state);
        result
    }
}
