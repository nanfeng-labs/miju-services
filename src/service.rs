//! Service entry points, their context, and typed resources.
//!
//! A service is an asynchronous function of its [`Context`]. It reports ready
//! after real initialization, runs until it is asked to stop, and returns.
//! Settings and resources reach it as ordinary captured values, or as a
//! [`Resource`] published by another service.
#![forbid(unsafe_code)]

use crate::{
    BoxError,
    observe::{Core, Observer},
    report::{Detail, Failure, Health, Outcome},
    scope::{Label, Lease, Rejected, Scope, Work},
};
use std::{
    any::Any,
    fmt,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
};
use tokio_util::sync::CancellationToken;

/// A service entry point, consumed once when its providers are initialized.
///
/// Closures and `async fn`s taking a [`Context`] and returning
/// [`crate::Result`] implement it. Implement it on a type to keep a concrete
/// error or to validate settings at assembly with [`Service::check`].
pub trait Service: Send + 'static {
    /// What the root returns on failure; kept in the final report.
    type Error: Into<BoxError> + Send + 'static;

    /// Initialize, report ready, and run until asked to stop.
    fn run(self, ctx: Context) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Validate at assembly, before any service runs.
    fn check(&self) -> Result<(), BoxError> {
        Ok(())
    }
}

impl<F, Fut> Service for F
where
    F: FnOnce(Context) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    type Error = BoxError;

    fn run(self, ctx: Context) -> impl Future<Output = Result<(), BoxError>> + Send {
        self(ctx)
    }
}

/// How the root ended, recorded after its future was dropped.
pub(crate) struct RootRecord {
    pub(crate) outcome: Outcome,
    pub(crate) stopping: bool,
    pub(crate) release_panic: bool,
}

#[derive(Default)]
pub(crate) struct CellState {
    pub(crate) started: bool,
    pub(crate) not_started: bool,
    pub(crate) initialized: bool,
    pub(crate) health: Health,
    pub(crate) declared_health: bool,
    pub(crate) root: Option<RootRecord>,
    pub(crate) failures: Vec<Failure>,
}

/// One service's shared state. Its lifetime is unrelated to responsibility:
/// responsibility is the scope's count, not this `Arc`'s.
pub(crate) struct ServiceCell {
    pub(crate) name: Arc<str>,
    pub(crate) required: bool,
    pub(crate) finite: bool,
    pub(crate) rank: usize,
    pub(crate) providers: Vec<Arc<str>>,
    pub(crate) scope: Scope,
    pub(crate) state: Mutex<CellState>,
    pub(crate) detail: Mutex<Detail>,
}

impl ServiceCell {
    pub(crate) fn state(&self) -> std::sync::MutexGuard<'_, CellState> {
        self.state.lock().expect("service state runs no user code")
    }
}

/// What a service uses to talk to its lifecycle.
#[derive(Clone)]
pub struct Context {
    cell: Arc<ServiceCell>,
    core: Arc<Core>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("service", &self.cell.name)
            .finish()
    }
}

impl Context {
    pub(crate) fn new(cell: Arc<ServiceCell>, core: Arc<Core>) -> Self {
        Self { cell, core }
    }

    /// The name this service was registered under.
    pub fn name(&self) -> &str {
        &self.cell.name
    }

    /// Report that real initialization has finished.
    ///
    /// Health becomes `Healthy` unless it was declared explicitly. Repeated
    /// calls change nothing. Calls after stop was requested, or after the root
    /// has returned, are ignored.
    pub fn ready(&self) {
        {
            let mut state = self.cell.state();
            if self.is_stopping() || state.root.is_some() {
                return;
            }
            state.initialized = true;
            if !state.declared_health {
                state.health = Health::Healthy;
            }
        }
        self.core.wake();
    }

    /// Declare current health. Nothing is stopped or restarted because of it.
    pub fn set_health(&self, health: Health) {
        {
            let mut state = self.cell.state();
            if self.is_stopping() || state.root.is_some() {
                return;
            }
            state.declared_health = true;
            state.health = health;
        }
        self.core.wake();
    }

    /// Whether this service has been asked to stop.
    pub fn is_stopping(&self) -> bool {
        self.cell.scope.stop_token().is_cancelled()
    }

    /// Resolves when this service is asked to stop.
    pub async fn stopping(&self) {
        self.cell.scope.stop_token().cancelled().await;
    }

    /// A token cancelled when this service is asked to stop, for frameworks
    /// that take one. Cancelling it yourself does not stop the service.
    pub fn stop_token(&self) -> CancellationToken {
        self.cell.scope.stop_token().child_token()
    }

    /// Accept asynchronous work; see [`Scope::spawn`].
    pub fn spawn<F, T, E>(&self, name: impl Into<Label>, work: F) -> Result<Work<T>, Rejected>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        self.cell.scope.spawn(name, work)
    }

    /// Accept blocking work; see [`Scope::spawn_blocking`].
    pub fn spawn_blocking<F, T, E>(
        &self,
        name: impl Into<Label>,
        work: F,
    ) -> Result<Work<T>, Rejected>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        self.cell.scope.spawn_blocking(name, work)
    }

    /// Take a lease for something the library does not run; see [`Scope::lease`].
    pub fn lease(&self, name: impl Into<Label>) -> Result<Lease, Rejected> {
        self.cell.scope.lease(name)
    }

    /// This service's scope, to hand to handlers and frameworks.
    pub fn scope(&self) -> Scope {
        self.cell.scope.clone()
    }

    /// Register a cleanup action for a resource this service just acquired.
    ///
    /// The driver runs it after every service has drained, whatever happens in
    /// between. Call it from the code that completed the acquisition, before
    /// the value crosses an await point or a work handle.
    ///
    /// Accepted while this service has not drained. Afterwards the future is
    /// handed back in the error.
    pub fn defer<F, E>(&self, name: impl Into<Label>, close: F) -> Result<(), DeferRejected<F>>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let label = name.into();
        let mut close = Some(close);
        let accepted = self.core.cleanup.push(
            self.cell.rank,
            label.clone(),
            self.cell.name.clone(),
            || self.cell.scope.is_drained(),
            || {
                let close = close.take().expect("called once");
                Box::pin(crate::contain::KeepUntilDropped { inner: close })
            },
        );
        match close {
            None if accepted => Ok(()),
            close => Err(DeferRejected {
                label,
                close: Mutex::new(close),
            }),
        }
    }

    /// Publish a typed observation; it appears in this service's snapshots.
    pub fn publish<D: Any + Send + Sync>(&self, detail: Arc<D>) {
        let previous = {
            let mut slot = self.cell.detail.lock().expect("detail runs no user code");
            std::mem::replace(&mut *slot, Detail(Some(detail)))
        };
        crate::contain::drop_contained(previous);
    }

    /// Observe the application without holding a control lease on it.
    pub fn app(&self) -> Observer {
        Observer::new(self.core.clone())
    }

    pub(crate) fn declares(&self, provider: &str) -> bool {
        self.cell
            .providers
            .iter()
            .any(|declared| &**declared == provider)
    }
}

/// A cleanup action that was not accepted: its service had already drained.
/// The future is still here; the caller remains responsible for the resource.
pub struct DeferRejected<F> {
    label: Label,
    // Behind a mutex so the error is `Sync` and `?` converts it to `BoxError`.
    close: Mutex<Option<F>>,
}

impl<F> DeferRejected<F> {
    /// The name the action was registered under.
    pub fn label(&self) -> &Label {
        &self.label
    }

    /// Take the action back.
    pub fn into_inner(self) -> Option<F> {
        self.close.into_inner().ok().flatten()
    }
}

impl<F> fmt::Debug for DeferRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeferRejected")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl<F> fmt::Display for DeferRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cleanup `{}` was registered after its service had drained",
            self.label
        )
    }
}

impl<F> std::error::Error for DeferRejected<F> {}

struct ResourceInner<T> {
    name: Arc<str>,
    value: OnceLock<T>,
}

/// A value one service acquires and others use, tied to the dependency edge.
///
/// Created by [`crate::App::resource`]. A consumer declares the edge with
/// `Spec::after(&resource)` and reads the value with [`Resource::get`].
pub struct Resource<T>(Arc<ResourceInner<T>>);

impl<T> Clone for Resource<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> fmt::Debug for Resource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resource")
            .field("name", &self.0.name)
            .finish_non_exhaustive()
    }
}

impl<T> Resource<T> {
    pub(crate) fn new(name: Arc<str>) -> Self {
        Self(Arc::new(ResourceInner {
            name,
            value: OnceLock::new(),
        }))
    }

    pub(crate) fn set(&self, value: T) {
        let _ = self.0.value.set(value);
    }

    /// The name of the service that provides it.
    pub fn name(&self) -> &str {
        &self.0.name
    }
}

impl<T: Clone> Resource<T> {
    /// Read the value from a service that declared the dependency.
    ///
    /// The check is against the edges fixed at assembly, so a service that
    /// forgot `.after(&resource)` fails every time, whichever service happened
    /// to initialize first.
    pub fn get(&self, ctx: &Context) -> Result<T, ResourceError> {
        if !ctx.declares(&self.0.name) {
            return Err(ResourceError::Undeclared {
                consumer: ctx.name().into(),
                provider: self.0.name.clone(),
            });
        }
        self.0
            .value
            .get()
            .cloned()
            .ok_or_else(|| ResourceError::NotInitialized {
                provider: self.0.name.clone(),
            })
    }
}

/// Why a resource could not be read.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResourceError {
    /// The calling service did not declare the dependency.
    #[error("service `{consumer}` uses resource `{provider}` without declaring `.after(..)` on it")]
    Undeclared {
        /// The service that called `get`.
        consumer: Arc<str>,
        /// The service that provides the resource.
        provider: Arc<str>,
    },
    /// The provider has not produced a value. A declared consumer is not
    /// started before it has, so this is reachable only from the provider
    /// itself or from code that kept a context it should not have.
    #[error("resource `{provider}` has not been initialized")]
    NotInitialized {
        /// The service that provides the resource.
        provider: Arc<str>,
    },
}
