//! Dependency-ordered application startup, shutdown, and cleanup.
#![forbid(unsafe_code)]

use crate::{
    AppObserver, AppSnapshot, BoxError, Criticality, DriverLost, ExitReport, Health, Lifetime,
    Phase, RootOutcome, Service, ServiceContext, ServiceSnapshot, ServiceSpec, SharedError,
    ShutdownReport, WorkScope,
    cleanup::{CleanupReport, Stack, diagnostic},
    observe::{Observation, Shared},
    service::ContextState,
    util::lock,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{Notify, watch},
    task::{AbortHandle, JoinError},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Core timing and result-retention parameters; business configuration stays in the host.
#[derive(Clone, Debug)]
pub struct AppOptions {
    /// Per-service initialization budget; defaults to 30 seconds.
    pub startup_timeout: Duration,
    /// Shared shutdown cooperation budget; defaults to 30 seconds.
    pub shutdown_grace: Duration,
    /// Additional shared abort confirmation budget; defaults to one second.
    pub abort_wait: Duration,
    /// Recent work result count per scope; defaults to 64, zero is allowed.
    pub recent_results: usize,
}

impl Default for AppOptions {
    fn default() -> Self {
        Self {
            startup_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
            abort_wait: Duration::from_secs(1),
            recent_results: 64,
        }
    }
}

/// Invalid assembly rejected before any service starts.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// At least one named service is required.
    #[error("application needs at least one named service")]
    Empty,
    /// Service names must be unique.
    #[error("duplicate service: {0}")]
    Duplicate(String),
    /// A dependency is unknown, duplicated, or optional.
    #[error("invalid dependency {dependency} on service {service}")]
    Dependency {
        /// The dependent service.
        service: String,
        /// Invalid dependency name.
        dependency: String,
    },
    /// The dependency graph has a cycle.
    #[error("service dependency cycle")]
    Cycle,
    /// A duration is zero where prohibited or cannot form a deadline.
    #[error("invalid application timing budget")]
    Budget,
}

type Factory = Box<dyn FnOnce(ServiceContext) -> BoxFuture<'static, Result<(), BoxError>> + Send>;

struct Definition {
    spec: ServiceSpec,
    run: Factory,
}

/// Collects service definitions without executing application code.
#[derive(Default)]
pub struct ApplicationBuilder {
    options: AppOptions,
    definitions: Vec<Definition>,
}

impl fmt::Debug for ApplicationBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationBuilder")
            .field("options", &self.options)
            .field("services", &self.definitions.len())
            .finish()
    }
}

impl ApplicationBuilder {
    /// Set core lifecycle budgets and retention.
    #[must_use]
    pub fn options(mut self, options: AppOptions) -> Self {
        self.options = options;
        self
    }

    /// Register a once-owned root and its dependency policy.
    #[must_use]
    pub fn service<S: Service>(mut self, spec: ServiceSpec, service: S) -> Self {
        self.definitions.push(Definition {
            spec,
            run: Box::new(move |context| {
                Box::pin(async move { service.run(context).await.map_err(Into::into) })
            }),
        });
        self
    }

    /// Validate identities, timing, and the static required-provider DAG.
    pub fn build(self) -> Result<Application, BuildError> {
        let now = Instant::now();
        if self.options.startup_timeout.is_zero()
            || self.options.shutdown_grace.is_zero()
            || now.checked_add(self.options.startup_timeout).is_none()
            || self
                .options
                .shutdown_grace
                .checked_add(self.options.abort_wait)
                .and_then(|d| now.checked_add(d))
                .is_none()
        {
            return Err(BuildError::Budget);
        }
        if self.definitions.is_empty()
            || self
                .definitions
                .iter()
                .any(|d| d.spec.id.as_str().trim().is_empty())
        {
            return Err(BuildError::Empty);
        }
        let mut names = HashMap::new();
        for (i, d) in self.definitions.iter().enumerate() {
            if names.insert(d.spec.id.as_str(), i).is_some() {
                return Err(BuildError::Duplicate(d.spec.id.to_string()));
            }
        }
        let mut dependencies = Vec::new();
        for d in &self.definitions {
            let mut seen = HashSet::new();
            let mut edges = Vec::new();
            for name in &d.spec.dependencies {
                let index = names.get(name.as_str()).copied();
                if !seen.insert(name)
                    || index.is_none()
                    || index.is_some_and(|i| {
                        self.definitions[i].spec.criticality == Criticality::Optional
                    })
                {
                    return Err(BuildError::Dependency {
                        service: d.spec.id.to_string(),
                        dependency: name.clone(),
                    });
                }
                edges.push(index.expect("validated dependency"));
            }
            dependencies.push(edges);
        }
        let mut resolved = HashSet::new();
        loop {
            let before = resolved.len();
            for (i, edges) in dependencies.iter().enumerate() {
                if edges.iter().all(|d| resolved.contains(d)) {
                    resolved.insert(i);
                }
            }
            if resolved.len() == dependencies.len() {
                break;
            }
            if resolved.len() == before {
                return Err(BuildError::Cycle);
            }
        }
        Ok(Application {
            options: self.options,
            definitions: self.definitions,
            dependencies,
        })
    }
}

/// A validated, one-use application assembly.
pub struct Application {
    options: AppOptions,
    definitions: Vec<Definition>,
    dependencies: Vec<Vec<usize>>,
}

impl fmt::Debug for Application {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Application")
            .field("services", &self.definitions.len())
            .field("options", &self.options)
            .finish()
    }
}

/// Starting requires an active Tokio runtime; retains the unstarted assembly.
#[derive(Debug, thiserror::Error)]
#[error("start requires an active Tokio runtime with time enabled")]
pub struct StartError {
    application: Application,
}

impl StartError {
    /// Recover the unstarted assembly and retry inside a supported runtime.
    pub fn into_application(self) -> Application {
        self.application
    }
}

impl Application {
    /// Assemble services without creating a runtime or installing process hooks.
    pub fn builder() -> ApplicationBuilder {
        ApplicationBuilder::default()
    }

    /// Start an independent lifecycle driver on the current runtime.
    ///
    /// The runtime must enable time and remain alive until actual drain. Disabling
    /// time is a host precondition violation and may panic under Tokio's contract.
    pub fn start(self) -> Result<AppHandle, StartError> {
        let Ok(runtime) = Handle::try_current() else {
            return Err(StartError { application: self });
        };
        let notify = Arc::new(Notify::new());
        let initial = Arc::new(AppSnapshot {
            revision: 0,
            phase: Phase::Starting,
            ready: false,
            health: Health::Unknown,
            services: Vec::new(),
            current_cleanup: None,
            pending_cleanup: 0,
            budget_exceeded: false,
            driver_lost: false,
        });
        let (sender, receiver) = watch::channel(Arc::new(Observation {
            snapshot: initial,
            budget: None,
            exit: None,
            lost: false,
        }));
        let shared = Arc::new(Shared {
            sender,
            stop: Mutex::new(None),
            notify: notify.clone(),
        });
        let observer = AppObserver {
            shared: shared.clone(),
            receiver,
        };
        let cleanup = Arc::new(Stack::default());
        let ids = Arc::new(AtomicU64::new(1));
        let roots = self
            .definitions
            .into_iter()
            .map(|d| {
                let context = ServiceContext {
                    id: d.spec.id.clone(),
                    state: Arc::new(Mutex::new(ContextState {
                        ready: false,
                        health: Health::Unknown,
                        finished: false,
                        stopping: false,
                        backend_unconfirmed: false,
                        execution_errors: 0,
                        recent_errors: std::collections::VecDeque::new(),
                        #[cfg(feature = "miju")]
                        recent_limit: self.options.recent_results,
                    })),
                    stop: CancellationToken::new(),
                    scope: WorkScope::new(
                        d.spec.id.clone(),
                        d.spec.max_work,
                        self.options.recent_results,
                        runtime.clone(),
                        notify.clone(),
                        ids.clone(),
                    ),
                    cleanup: cleanup.clone(),
                    observer: observer.clone(),
                    notify: notify.clone(),
                };
                Root {
                    spec: d.spec,
                    context,
                    run: Some(d.run),
                    started: None,
                    outcome: None,
                    failure: None,
                    abort: None,
                    local_stop: None,
                    aborted: false,
                    forced_abort: false,
                    local_exceeded: false,
                }
            })
            .collect();
        let driver = Driver {
            roots,
            dependencies: self.dependencies,
            options: self.options,
            shared: shared.clone(),
            cleanup,
            joins: FuturesUnordered::new(),
            revision: 0,
            budget: None,
            phase: Phase::Starting,
            cleanup_results: Vec::new(),
        };
        driver.publish(None);
        let guard = LostGuard(shared.clone());
        runtime.spawn(async move {
            let _guard = guard;
            driver.run().await;
        });
        Ok(AppHandle {
            lease: Arc::new(ControlLease { shared }),
            observer,
        })
    }
}

#[derive(Debug)]
struct ControlLease {
    shared: Arc<Shared>,
}

impl Drop for ControlLease {
    fn drop(&mut self) {
        self.shared
            .request_stop(Arc::from("last control handle dropped"));
    }
}

/// Clonable control lease; dropping the last lease requests stop but does not wait.
#[derive(Clone, Debug)]
pub struct AppHandle {
    lease: Arc<ControlLease>,
    observer: AppObserver,
}

impl AppHandle {
    /// Obtain read-only observation without extending control ownership.
    pub fn observer(&self) -> AppObserver {
        self.observer.clone()
    }

    /// Request dependency-ordered shutdown once; repeated calls keep the original budget.
    pub fn stop(&self) {
        self.lease.shared.request_stop(Arc::from("requested"));
    }

    /// Request stop and wait for its immutable shared budget conclusion.
    pub async fn shutdown(&self) -> Result<Arc<ShutdownReport>, DriverLost> {
        self.stop();
        self.observer.budget().await
    }
}

struct LostGuard(Arc<Shared>);

impl Drop for LostGuard {
    fn drop(&mut self) {
        let previous = self.0.sender.borrow().clone();
        if previous.exit.is_none() {
            let mut snapshot = (*previous.snapshot).clone();
            snapshot.driver_lost = true;
            snapshot.ready = false;
            self.0.publish(Observation {
                snapshot: Arc::new(snapshot),
                budget: previous.budget.clone(),
                exit: None,
                lost: true,
            });
        }
    }
}

struct RootGuard(ServiceContext);

impl Drop for RootGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

struct Root {
    spec: ServiceSpec,
    context: ServiceContext,
    run: Option<Factory>,
    started: Option<Instant>,
    outcome: Option<RootOutcome>,
    failure: Option<SharedError>,
    abort: Option<AbortHandle>,
    local_stop: Option<Instant>,
    aborted: bool,
    forced_abort: bool,
    local_exceeded: bool,
}

impl Root {
    fn drained(&self) -> bool {
        self.outcome.is_some() && self.context.scope.is_drained()
    }

    fn initialized(&self) -> bool {
        match self.spec.lifetime {
            Lifetime::Resident => lock(&self.context.state).ready,
            Lifetime::Finite => {
                matches!(self.outcome, Some(RootOutcome::Succeeded))
                    && self.drained()
                    && self.failure.is_none()
            }
        }
    }
}

type Joined = BoxFuture<'static, (usize, Result<Option<RootOutcome>, JoinError>)>;

struct Driver {
    roots: Vec<Root>,
    dependencies: Vec<Vec<usize>>,
    options: AppOptions,
    shared: Arc<Shared>,
    cleanup: Arc<Stack>,
    joins: FuturesUnordered<Joined>,
    revision: u64,
    budget: Option<Arc<ShutdownReport>>,
    phase: Phase,
    cleanup_results: Vec<CleanupReport>,
}

impl Driver {
    fn service_snapshots(&self) -> Vec<ServiceSnapshot> {
        self.roots
            .iter()
            .map(|root| {
                let state = lock(&root.context.state);
                let health = state.health;
                let backend = state.backend_unconfirmed;
                let execution_errors = state.execution_errors;
                let recent_execution_errors = state.recent_errors.iter().cloned().collect();
                drop(state);
                ServiceSnapshot {
                    id: root.spec.id.clone(),
                    criticality: root.spec.criticality,
                    initialized: root.initialized(),
                    health,
                    stopping: root.context.is_stopping(),
                    outcome: root.outcome.clone(),
                    forced_abort: root.forced_abort,
                    failure: root.failure.clone(),
                    work: root.context.scope.snapshot(),
                    stop_budget_exceeded: root.local_exceeded,
                    backend_cleanup_unconfirmed: backend,
                    execution_errors,
                    recent_execution_errors,
                }
            })
            .collect()
    }

    fn snapshot(&self, current_cleanup: Option<String>) -> Arc<AppSnapshot> {
        let services = self.service_snapshots();
        let effective = |s: &ServiceSnapshot| {
            let i = self
                .roots
                .iter()
                .position(|r| r.spec.id == s.id)
                .expect("known service");
            if self.roots[i].drained() && s.failure.is_none() {
                Health::Healthy
            } else if s.health == Health::Unhealthy {
                Health::Unhealthy
            } else if !s.initialized {
                Health::Unknown
            } else {
                s.health
            }
        };
        let health = if services.iter().any(|s| {
            s.criticality == Criticality::Required
                && (s.failure.is_some()
                    || matches!(s.outcome, Some(RootOutcome::Aborted))
                    || effective(s) == Health::Unhealthy)
        }) {
            Health::Unhealthy
        } else if services
            .iter()
            .any(|s| s.criticality == Criticality::Required && effective(s) == Health::Unknown)
        {
            Health::Unknown
        } else if services.iter().any(|s| {
            if s.criticality == Criticality::Required {
                effective(s) == Health::Degraded
            } else {
                s.failure.is_some() || effective(s) != Health::Healthy
            }
        }) {
            Health::Degraded
        } else {
            Health::Healthy
        };
        let ready = self.shared.stop_info().is_none()
            && self.phase == Phase::Running
            && services
                .iter()
                .filter(|s| s.criticality == Criticality::Required)
                .all(|s| s.initialized && s.failure.is_none() && effective(s) == Health::Healthy);
        Arc::new(AppSnapshot {
            revision: self.revision,
            phase: self.phase,
            ready,
            health,
            services,
            current_cleanup,
            pending_cleanup: self.cleanup.len(),
            budget_exceeded: matches!(self.budget.as_deref(), Some(ShutdownReport::Incomplete(_))),
            driver_lost: false,
        })
    }

    fn publish(&self, current_cleanup: Option<String>) {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            revision = self.revision,
            phase = ?self.phase,
            cleanup = ?current_cleanup,
            "application snapshot"
        );
        self.shared.publish(Observation {
            snapshot: self.snapshot(current_cleanup),
            budget: self.budget.clone(),
            exit: None,
            lost: false,
        });
    }

    fn fault(&mut self, index: usize, error: SharedError) {
        #[cfg(feature = "tracing")]
        tracing::warn!(service = %self.roots[index].spec.id, error = %error, "service lifecycle failure");
        let root = &mut self.roots[index];
        if root.failure.is_none() {
            root.failure = Some(error);
        }
        if root.spec.criticality == Criticality::Required {
            self.shared.request_stop(Arc::from(format!(
                "required service {} failed",
                root.spec.id
            )));
        } else if root.local_stop.is_none() {
            root.local_stop = Some(Instant::now());
            root.context.begin_stop();
        }
    }

    fn start_roots(&mut self) {
        if self.shared.stop_info().is_some() {
            for root in &mut self.roots {
                if let Some(run) = root.run.take() {
                    drop(run);
                    root.outcome = Some(RootOutcome::NotStarted);
                    root.context.finish();
                }
            }
            return;
        }
        for i in 0..self.roots.len() {
            if self.roots[i].run.is_some()
                && self.dependencies[i]
                    .iter()
                    .all(|&p| self.roots[p].initialized())
            {
                let root = &mut self.roots[i];
                let run = root.run.take().expect("unstarted root");
                let context = root.context.clone();
                let shared = self.shared.clone();
                root.started = Some(Instant::now());
                let guard = RootGuard(context.clone());
                let task = tokio::spawn(async move {
                    let _guard = guard;
                    if shared.stop_info().is_some() {
                        drop(run);
                        return None;
                    }
                    let outcome = std::panic::AssertUnwindSafe(async move { run(context).await })
                        .catch_unwind()
                        .await;
                    Some(match outcome {
                        Ok(Ok(())) => RootOutcome::Succeeded,
                        Ok(Err(error)) => RootOutcome::Failed(Arc::from(error)),
                        Err(payload) => RootOutcome::Panicked(panic_text(payload)),
                    })
                });
                root.abort = Some(task.abort_handle());
                self.joins.push(Box::pin(async move { (i, task.await) }));
            }
        }
    }

    fn check_faults(&mut self) {
        for i in 0..self.roots.len() {
            if self.roots[i].failure.is_some() {
                continue;
            }
            if let Some(error) = self.roots[i].context.scope.snapshot().service_failure {
                self.fault(i, error);
                continue;
            }
            let root = &self.roots[i];
            if !root.context.is_stopping()
                && !root.initialized()
                && root
                    .started
                    .is_some_and(|t| t.elapsed() >= self.options.startup_timeout)
            {
                self.fault(i, diagnostic("service initialization timed out"));
            }
        }
    }

    fn progress_stop(&mut self) {
        let global = self.shared.stop_info();
        if global.is_some() {
            self.phase = Phase::Stopping;
        }
        for i in 0..self.roots.len() {
            let permitted = (0..self.roots.len())
                .filter(|&c| self.dependencies[c].contains(&i))
                .all(|c| self.roots[c].drained());
            if global.is_some() && permitted && !self.roots[i].context.is_stopping() {
                self.roots[i].context.begin_stop();
            }
            let root = &mut self.roots[i];
            if !root.drained()
                && root.local_stop.is_some_and(|t| {
                    Instant::now() >= t + self.options.shutdown_grace + self.options.abort_wait
                })
            {
                root.local_exceeded = true;
            }
            let global_deadline = global
                .as_ref()
                .filter(|_| root.context.is_stopping())
                .map(|(t, _)| *t + self.options.shutdown_grace);
            let local_deadline = root.local_stop.map(|t| t + self.options.shutdown_grace);
            let deadline = match (global_deadline, local_deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if deadline.is_some_and(|t| Instant::now() >= t) && !root.aborted {
                root.aborted = true;
                if let Some(abort) = &root.abort {
                    abort.abort();
                }
                root.context.scope.abort();
            }
        }
    }

    fn budget_deadline(&self) -> Option<Instant> {
        self.shared
            .stop_info()
            .map(|(t, _)| t + self.options.shutdown_grace + self.options.abort_wait)
    }

    fn check_budget(&mut self, current: Option<String>) {
        if self.budget.is_none() && self.budget_deadline().is_some_and(|t| Instant::now() >= t) {
            let mut snapshot = (*self.snapshot(current)).clone();
            snapshot.budget_exceeded = true;
            self.budget = Some(Arc::new(ShutdownReport::Incomplete(Arc::new(snapshot))));
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let mut deadlines = Vec::new();
        if self.budget.is_none() {
            deadlines.extend(self.budget_deadline());
        }
        for root in &self.roots {
            if !root.local_exceeded && !root.drained() {
                deadlines.extend(
                    root.local_stop
                        .map(|t| t + self.options.shutdown_grace + self.options.abort_wait),
                );
            }
            if root.failure.is_none() && !root.initialized() && !root.context.is_stopping() {
                deadlines.extend(root.started.map(|t| t + self.options.startup_timeout));
            }
            if !root.aborted {
                deadlines.extend(root.local_stop.map(|t| t + self.options.shutdown_grace));
                if root.context.is_stopping() {
                    deadlines.extend(
                        self.shared
                            .stop_info()
                            .map(|(t, _)| t + self.options.shutdown_grace),
                    );
                }
            }
        }
        deadlines.into_iter().min()
    }

    fn complete_root(&mut self, i: usize, result: Result<Option<RootOutcome>, JoinError>) {
        self.roots[i].forced_abort = self.roots[i].aborted && result.is_err();
        let outcome = match result {
            Ok(Some(outcome)) => outcome,
            Ok(None) => RootOutcome::NotStarted,
            Err(error) if error.is_cancelled() => RootOutcome::Aborted,
            Err(error) => RootOutcome::Panicked(panic_text(error.into_panic())),
        };
        let failure = match &outcome {
            RootOutcome::Failed(e) => Some(e.clone()),
            RootOutcome::Panicked(s) => Some(diagnostic(s.to_string())),
            RootOutcome::Aborted => Some(diagnostic("root forcibly aborted")),
            RootOutcome::Succeeded
                if self.roots[i].spec.lifetime == Lifetime::Resident
                    && !self.roots[i].context.is_stopping() =>
            {
                Some(diagnostic("resident service exited unexpectedly"))
            }
            _ => None,
        };
        self.roots[i].context.finish();
        self.roots[i].outcome = Some(outcome);
        self.roots[i].abort = None;
        if let Some(error) = failure {
            self.fault(i, error);
        }
    }

    async fn run(mut self) {
        loop {
            let notify = self.shared.notify.clone();
            let changed = notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check_faults();
            self.start_roots();
            self.progress_stop();
            if self.roots.iter().all(Root::drained) {
                break;
            }
            if self.shared.stop_info().is_none()
                && self
                    .roots
                    .iter()
                    .filter(|r| r.spec.criticality == Criticality::Required)
                    .all(Root::initialized)
            {
                self.phase = Phase::Running;
            }
            self.check_budget(None);
            self.revision += 1;
            self.publish(None);
            let deadline = self.next_deadline();
            tokio::select! {
                Some((i, result)) = self.joins.next(), if !self.joins.is_empty() =>
                    self.complete_root(i, result),
                _ = &mut changed => {},
                _ = wait_deadline(deadline) => {},
            }
        }
        self.phase = Phase::Cleaning;
        while let Some(entry) = self.cleanup.pop() {
            let label = entry.label.clone();
            let id = entry.id;
            let future =
                std::panic::AssertUnwindSafe(async move { (entry.run)().await }).catch_unwind();
            tokio::pin!(future);
            let result = loop {
                self.check_budget(Some(label.clone()));
                self.revision += 1;
                self.publish(Some(label.clone()));
                let deadline = if self.budget.is_none() {
                    self.budget_deadline()
                } else {
                    None
                };
                tokio::select! {
                    result = &mut future => break result,
                    _ = wait_deadline(deadline) => {},
                    _ = self.shared.notify.notified() => {}
                }
            };
            self.check_budget(Some(label.clone()));
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(Arc::from(e)),
                Err(_) => Some(diagnostic("cleanup panicked")),
            };
            self.cleanup_results
                .push(CleanupReport { id, label, error });
        }
        self.check_budget(None);
        self.phase = Phase::Terminated;
        self.revision += 1;
        let services = self.service_snapshots();
        let successful = services.iter().all(|s| {
            (s.criticality != Criticality::Required || s.failure.is_none())
                && !s.forced_abort
                && !matches!(s.outcome, Some(RootOutcome::Aborted))
                && s.work.aborted == 0
                && !s.backend_cleanup_unconfirmed
        }) && self.cleanup_results.iter().all(|c| c.error.is_none());
        let exit = Arc::new(ExitReport {
            reason: self
                .shared
                .stop_info()
                .map(|(_, r)| r)
                .unwrap_or_else(|| Arc::from("all services completed")),
            services,
            cleanup: std::mem::take(&mut self.cleanup_results),
            budget_exceeded: matches!(self.budget.as_deref(), Some(ShutdownReport::Incomplete(_))),
            successful,
        });
        if self.budget.is_none() {
            self.budget = Some(Arc::new(ShutdownReport::Completed(exit.clone())));
        }
        self.shared.publish(Observation {
            snapshot: self.snapshot(None),
            budget: self.budget.clone(),
            exit: Some(exit),
            lost: false,
        });
    }
}

fn panic_text(payload: Box<dyn std::any::Any + Send>) -> Arc<str> {
    if let Some(text) = payload.downcast_ref::<String>() {
        Arc::from(text.as_str())
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        Arc::from(*text)
    } else {
        Arc::from("non-string panic payload")
    }
}

async fn wait_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DrainObservation, service_fn};

    #[test]
    fn destroying_runtime_reports_driver_lost_to_pending_and_late_observers() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let control = runtime.block_on(async {
            let control = Application::builder()
                .service(
                    ServiceSpec::new("pending"),
                    service_fn(|_| async { std::future::pending::<Result<(), BoxError>>().await }),
                )
                .build()
                .unwrap()
                .start()
                .unwrap();
            tokio::task::yield_now().await;
            control
        });
        let observer = control.observer();
        drop(runtime);
        let another = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        another.block_on(async {
            assert!(observer.wait().await.is_err());
            assert!(control.shutdown().await.is_err());
            assert!(matches!(
                observer.drain(Duration::from_secs(1)).await,
                DrainObservation::DriverLost(_)
            ));
            assert!(observer.snapshot().driver_lost);
        });
    }

    #[test]
    fn completed_report_survives_runtime_destruction() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (control, exit) = runtime.block_on(async {
            let control = Application::builder()
                .service(
                    ServiceSpec::new("finite").lifetime(Lifetime::Finite),
                    service_fn(|_| async { Ok::<(), BoxError>(()) }),
                )
                .build()
                .unwrap()
                .start()
                .unwrap();
            let exit = control.observer().wait().await.unwrap();
            (control, exit)
        });
        drop(runtime);
        let another = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        another.block_on(async {
            assert!(Arc::ptr_eq(
                &exit,
                &control.observer().wait().await.unwrap()
            ));
            assert!(matches!(
                &*control.shutdown().await.unwrap(),
                ShutdownReport::Completed(_)
            ));
        });
    }

    #[test]
    fn incomplete_history_survives_later_driver_loss() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let control = runtime.block_on(async {
            let control = Application::builder()
                .options(AppOptions {
                    shutdown_grace: Duration::from_millis(1),
                    abort_wait: Duration::ZERO,
                    ..AppOptions::default()
                })
                .service(
                    ServiceSpec::new("cleanup"),
                    service_fn(|context| async move {
                        context
                            .defer_cleanup("never", || async {
                                std::future::pending::<Result<(), BoxError>>().await
                            })
                            .unwrap();
                        context.set_health(Health::Healthy)?;
                        context.ready()?;
                        context.stopping().await;
                        Ok::<(), BoxError>(())
                    }),
                )
                .build()
                .unwrap()
                .start()
                .unwrap();
            control
                .observer()
                .ready(Duration::from_secs(1))
                .await
                .unwrap();
            assert!(matches!(
                &*control.shutdown().await.unwrap(),
                ShutdownReport::Incomplete(_)
            ));
            control
        });
        let observer = control.observer();
        drop(runtime);
        let another = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        another.block_on(async {
            assert!(matches!(
                &*control.shutdown().await.unwrap(),
                ShutdownReport::Incomplete(_)
            ));
            assert!(observer.wait().await.is_err());
        });
    }
}
