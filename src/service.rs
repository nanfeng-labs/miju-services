//! Root services, explicit initialization, and dependency declarations.
#![forbid(unsafe_code)]

use crate::{
    AppObserver, BoxError, Health, WorkScope,
    cleanup::{CleanupId, CleanupRejected, Stack},
    util::lock,
};
use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Stable name within one application instance.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServiceId(pub(crate) Arc<str>);

impl ServiceId {
    /// Service name supplied during assembly.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Expected lifetime of a root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifetime {
    /// Must remain alive until asked to stop.
    Resident,
    /// Completes after a finite operation and its child work.
    Finite,
}

/// Failure propagation and readiness participation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Criticality {
    /// Failure requests application shutdown; may provide dependencies.
    Required,
    /// Failure stops only this service; cannot be a dependency target.
    Optional,
}

/// Validated by the application builder before anything starts.
#[derive(Clone, Debug)]
pub struct ServiceSpec {
    pub(crate) id: ServiceId,
    pub(crate) lifetime: Lifetime,
    pub(crate) criticality: Criticality,
    pub(crate) dependencies: Vec<String>,
    pub(crate) max_work: usize,
}

impl ServiceSpec {
    /// A required resident service with capacity for 1024 work responsibilities.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: ServiceId(Arc::from(name.into())),
            lifetime: Lifetime::Resident,
            criticality: Criticality::Required,
            dependencies: Vec::new(),
            max_work: 1024,
        }
    }

    /// Set expected lifetime.
    #[must_use]
    pub fn lifetime(mut self, value: Lifetime) -> Self {
        self.lifetime = value;
        self
    }

    /// Set failure propagation policy.
    #[must_use]
    pub fn criticality(mut self, value: Criticality) -> Self {
        self.criticality = value;
        self
    }

    /// Keep this provider alive until this service and all its work finish.
    #[must_use]
    pub fn depends_on(mut self, name: impl Into<String>) -> Self {
        self.dependencies.push(name.into());
        self
    }

    /// Bound accepted work, including unresolved aggregate responsibility.
    #[must_use]
    pub fn max_work(mut self, limit: NonZeroUsize) -> Self {
        self.max_work = limit.get();
        self
    }
}

/// A once-owned application root; distinct from a Tower request service.
pub trait Service: Send + 'static {
    /// Application error preserved across the supervision boundary.
    type Error: Into<BoxError> + Send + 'static;

    /// Initialize, explicitly report readiness, and cooperatively finish on stop.
    fn run(self, context: ServiceContext) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Adapt a single-context asynchronous function, capturing dependencies explicitly.
pub fn service_fn<F, Fut, E>(run: F) -> impl Service<Error = E>
where
    F: FnOnce(ServiceContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    Function(run)
}

struct Function<F>(F);

impl<F, Fut, E> Service for Function<F>
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

/// The root has finished or entered its stopping stage.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("service context is closed")]
pub struct ContextClosed;

pub(crate) struct ContextState {
    pub ready: bool,
    pub health: Health,
    pub finished: bool,
    pub stopping: bool,
    pub backend_unconfirmed: bool,
    pub execution_errors: u64,
    pub recent_errors: std::collections::VecDeque<crate::SharedError>,
    #[cfg(feature = "miju")]
    pub recent_limit: usize,
}

impl fmt::Debug for ContextState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextState")
            .field("ready", &self.ready)
            .field("health", &self.health)
            .field("finished", &self.finished)
            .field("stopping", &self.stopping)
            .field("execution_errors", &self.execution_errors)
            .finish_non_exhaustive()
    }
}

/// Initialization reporting and owned work for a single root.
#[derive(Clone, Debug)]
pub struct ServiceContext {
    pub(crate) id: ServiceId,
    pub(crate) state: Arc<Mutex<ContextState>>,
    pub(crate) stop: CancellationToken,
    pub(crate) scope: WorkScope,
    pub(crate) cleanup: Arc<Stack>,
    pub(crate) observer: AppObserver,
    pub(crate) notify: Arc<Notify>,
}

impl ServiceContext {
    /// Root identity.
    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    /// Read-only application observation; does not retain a control lease.
    pub fn observer(&self) -> AppObserver {
        self.observer.clone()
    }

    /// Latch initialization; reject late calls after stopping or root release.
    pub fn ready(&self) -> Result<(), ContextClosed> {
        let mut state = lock(&self.state);
        if state.finished || state.stopping || self.scope.is_stopping() {
            return Err(ContextClosed);
        }
        state.ready = true;
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    /// Report current health without changing the initialization latch.
    pub fn set_health(&self, health: Health) -> Result<(), ContextClosed> {
        let mut state = lock(&self.state);
        if state.finished || state.stopping || self.scope.is_stopping() {
            return Err(ContextClosed);
        }
        state.health = health;
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    /// Whether this root's dependency-ordered stop stage has begun.
    pub fn is_stopping(&self) -> bool {
        self.scope.is_stopping()
    }

    /// Wait for this root's stop stage, not an unrelated provider's stage.
    pub async fn stopping(&self) {
        self.stop.cancelled().await;
    }

    /// Scope for all dynamic work owned by this root.
    pub fn work(&self) -> &WorkScope {
        &self.scope
    }

    /// Transfer cleanup during initialization, including interrupted initialization.
    ///
    /// Rejection returns the original closure; callers must execute it or retain it.
    pub fn defer_cleanup<F, Fut, E>(
        &self,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<CleanupId, CleanupRejected<F>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let label = label.into();
        let state = lock(&self.state);
        if state.finished || state.ready {
            drop(state);
            return Err(CleanupRejected { cleanup });
        }
        let id = self.cleanup.push(label, cleanup);
        drop(state);
        self.notify.notify_one();
        Ok(id)
    }

    #[cfg(feature = "miju")]
    pub(crate) fn backend_unconfirmed(&self, value: bool) {
        lock(&self.state).backend_unconfirmed = value;
        self.notify.notify_one();
    }

    #[cfg(feature = "miju")]
    pub(crate) fn record_execution_error(&self, error: crate::SharedError) {
        let removed = {
            let mut state = lock(&self.state);
            state.execution_errors = state.execution_errors.saturating_add(1);
            if state.recent_limit == 0 {
                Some(error)
            } else {
                state.recent_errors.push_back(error);
                if state.recent_errors.len() > state.recent_limit {
                    state.recent_errors.pop_front()
                } else {
                    None
                }
            }
        };
        drop(removed);
        self.notify.notify_one();
    }

    pub(crate) fn begin_stop(&self) {
        // Close admission before publishing the stage-specific stop notification.
        self.scope.stop();
        lock(&self.state).stopping = true;
        self.stop.cancel();
        self.notify.notify_one();
    }

    pub(crate) fn finish(&self) {
        lock(&self.state).finished = true;
        self.scope.close();
    }
}
