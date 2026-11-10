//! Assemble once, then run on the application's Tokio runtime.
//!
//! [`App::run_until`] handles ordinary applications. Embedded hosts can retain an
//! [`AppHandle`] for readiness, bounded observation, and repeated actual-completion
//! waits. The runtime must remain alive until the final report arrives.
#![forbid(unsafe_code)]

mod driver;

use crate::{
    BoxError,
    cleanup::{Close, Registry},
    observe::{AppObserver, DriverLost, ReadyError, ShutdownProgress},
    report::{AppSnapshot, ExitReport, Health, Phase},
    service::{Criticality, Service, ServiceContext, ServiceSpec},
    task,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{runtime::Handle, sync::Notify, time::Instant};

type Entry = Box<
    dyn FnOnce(ServiceContext) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>> + Send,
>;

struct Registration {
    spec: ServiceSpec,
    run: Entry,
}

/// Per-service initialization and first-stop budgets.
#[derive(Clone, Debug)]
pub struct AppOptions {
    /// Time from Resident service admission until `ready()`.
    ///
    /// Finite services use successful completion and actual drain as their
    /// initialization gate; their business duration is not bounded by this value.
    pub startup_timeout: Duration,
    /// Shared cooperative stop interval, starting at the first stop request.
    ///
    /// Providers receive only the remainder after their consumers drain, possibly
    /// no cooperative turn. Register required asynchronous teardown as cleanup;
    /// this application-wide budget is not renewed for each provider.
    pub shutdown_grace: Duration,
    /// Extra confirmation interval before latching historical budget overrun.
    pub abort_wait: Duration,
    /// Number of recent completed work reports retained per service.
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

/// An invalid assembly; no service entry was invoked.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum BuildError {
    /// An application must register at least one service.
    #[error("no services registered")]
    Empty,
    /// Names must contain a non-whitespace character.
    #[error("service name is empty")]
    EmptyName,
    /// Service identities must be unique within one application.
    #[error("duplicate service: {0}")]
    Duplicate(String),
    /// A dependency refers to a name that was not registered.
    #[error("{service} depends on unknown service {provider}")]
    UnknownDependency {
        /// Dependent name.
        service: String,
        /// Missing provider name.
        provider: String,
    },
    /// One registration repeats the same dependency.
    #[error("{service} repeats dependency {provider}")]
    DuplicateDependency {
        /// Dependent name.
        service: String,
        /// Repeated provider name.
        provider: String,
    },
    /// Optional services cannot provide a required startup gate.
    #[error("optional service cannot be a dependency provider: {0}")]
    OptionalProvider(String),
    /// Dependency cycle, including self-dependency.
    #[error("service dependencies contain a cycle")]
    Cycle,
    /// A zero or unrepresentable budget cannot define a valid deadline.
    #[error("invalid lifecycle budget: {0}")]
    InvalidBudget(&'static str),
}

/// Service and cleanup assembly. Building does not invoke user entries.
pub struct AppBuilder {
    options: AppOptions,
    services: Vec<Registration>,
    cleanup: Vec<(String, Close)>,
}

impl fmt::Debug for AppBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppBuilder")
            .field("options", &self.options)
            .field(
                "services",
                &self.services.iter().map(|s| &s.spec).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl AppBuilder {
    /// Override lifecycle budgets; [`build`](Self::build) validates them together.
    #[must_use]
    pub fn options(mut self, options: AppOptions) -> Self {
        self.options = options;
        self
    }

    /// Register an object or generic function adapter with a name or advanced policy.
    #[must_use]
    pub fn service<S: Service>(mut self, spec: impl Into<ServiceSpec>, service: S) -> Self {
        self.services.push(Registration {
            spec: spec.into(),
            run: Box::new(move |context| task::boxed(service.run(context))),
        });
        self
    }

    /// Register a function with inferred `Ok(())` and the common [`BoxError`] boundary.
    #[must_use]
    pub fn service_fn<F, Fut>(self, spec: impl Into<ServiceSpec>, run: F) -> Self
    where
        F: FnOnce(ServiceContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.service(spec, crate::service::service_fn(run))
    }

    /// Register existing-resource close in the same LIFO stack as initialization cleanup.
    ///
    /// Until start succeeds, bootstrap must retain independent rollback access.
    /// Build rejection or dropping an unstarted App does not await these actions.
    #[must_use]
    pub fn cleanup<F, Fut>(mut self, label: impl Into<String>, close: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.cleanup
            .push((label.into(), Box::new(move || task::boxed(close()))));
        self
    }

    /// Validate names, dependency DAG, provider policy, and time budgets.
    pub fn build(self) -> Result<App, BuildError> {
        if self.services.is_empty() {
            return Err(BuildError::Empty);
        }
        let now = Instant::now();
        for (name, duration) in [
            ("startup_timeout", self.options.startup_timeout),
            ("shutdown_grace", self.options.shutdown_grace),
        ] {
            if duration.is_zero() || now.checked_add(duration).is_none() {
                return Err(BuildError::InvalidBudget(name));
            }
        }
        if self
            .options
            .shutdown_grace
            .checked_add(self.options.abort_wait)
            .and_then(|duration| now.checked_add(duration))
            .is_none()
        {
            return Err(BuildError::InvalidBudget("shutdown_grace + abort_wait"));
        }
        let mut names = BTreeMap::new();
        for (index, entry) in self.services.iter().enumerate() {
            let name = entry.spec.id.name();
            if name.trim().is_empty() {
                return Err(BuildError::EmptyName);
            }
            if names.insert(name, index).is_some() {
                return Err(BuildError::Duplicate(name.into()));
            }
        }
        let mut dependencies = Vec::new();
        for entry in &self.services {
            let mut seen = BTreeSet::new();
            let mut providers = Vec::new();
            for provider in &entry.spec.dependencies {
                let service = entry.spec.id.name();
                let Some(&index) = names.get(provider.as_str()) else {
                    return Err(BuildError::UnknownDependency {
                        service: service.into(),
                        provider: provider.clone(),
                    });
                };
                if !seen.insert(index) {
                    return Err(BuildError::DuplicateDependency {
                        service: service.into(),
                        provider: provider.clone(),
                    });
                }
                if self.services[index].spec.criticality == Criticality::Optional {
                    return Err(BuildError::OptionalProvider(provider.clone()));
                }
                providers.push(index);
            }
            dependencies.push(providers);
        }
        let mut visited = BTreeSet::new();
        while visited.len() < self.services.len() {
            let before = visited.len();
            for (index, providers) in dependencies.iter().enumerate() {
                if providers.iter().all(|p| visited.contains(p)) {
                    visited.insert(index);
                }
            }
            if before == visited.len() {
                return Err(BuildError::Cycle);
            }
        }
        Ok(App {
            options: self.options,
            services: self.services,
            cleanup: self.cleanup,
            dependencies,
        })
    }
}

/// A validated, unstarted application, consumed at successful start.
pub struct App {
    options: AppOptions,
    services: Vec<Registration>,
    cleanup: Vec<(String, Close)>,
    dependencies: Vec<Vec<usize>>,
}

impl fmt::Debug for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("App")
            .field(
                "services",
                &self.services.iter().map(|s| &s.spec).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Startup rejection retaining the complete unstarted assembly for recovery.
#[derive(Debug, thiserror::Error)]
#[error("cannot start App without a current Tokio runtime")]
pub struct StartError {
    #[source]
    source: tokio::runtime::TryCurrentError,
    app: Mutex<Box<App>>,
}

impl StartError {
    /// Recover the unstarted App; retry on an active, time-enabled Tokio runtime.
    pub fn into_app(self) -> App {
        *self.app.into_inner().unwrap()
    }
}

/// Run failure retaining original errors and any actual final report.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// No runtime accepted the assembly; the App can be recovered.
    #[error(transparent)]
    Start(#[from] StartError),
    /// No final certificate exists; an already observed stop-source error is retained.
    #[error("lifecycle driver lost")]
    DriverLost {
        /// Driver loss with the last known snapshot.
        #[source]
        source: DriverLost,
        /// Original stop-source failure if already observed.
        stop_source: Option<BoxError>,
    },
    /// Lifecycle completed but required work or cleanup failed.
    #[error("application lifecycle failed; inspect the final report")]
    Failed {
        /// Actual completion with all errors preserved.
        report: Arc<ExitReport>,
    },
    /// The host stop future failed; actual cleanup still completed.
    #[error("host stop source failed: {source}")]
    StopSource {
        /// Original host error.
        #[source]
        source: BoxError,
        /// Actual final report, including any service failures.
        report: Arc<ExitReport>,
    },
}

impl RunError {
    /// Read the final report when actual completion was observed.
    pub fn report(&self) -> Option<&Arc<ExitReport>> {
        match self {
            Self::Failed { report } | Self::StopSource { report, .. } => Some(report),
            _ => None,
        }
    }
}

impl App {
    /// Begin an assembly with default lifecycle policy.
    pub fn builder() -> AppBuilder {
        AppBuilder {
            options: AppOptions::default(),
            services: Vec::new(),
            cleanup: Vec::new(),
        }
    }

    /// Start an independent lifecycle driver on the current Tokio runtime.
    ///
    /// The host must enable time and keep the runtime alive through actual completion.
    /// This checks only for a current runtime. A custom runtime without
    /// `enable_time()` violates that precondition and can lose the driver after
    /// service admission; it is not a recoverable [`StartError`].
    pub fn start(self) -> Result<AppHandle, StartError> {
        let runtime = match Handle::try_current() {
            Ok(runtime) => runtime,
            Err(source) => {
                return Err(StartError {
                    source,
                    app: Mutex::new(Box::new(self)),
                });
            }
        };
        let wake = Arc::new(Notify::new());
        let shared = crate::observe::Shared::new(
            AppSnapshot {
                phase: Phase::Starting,
                ready: false,
                health: Health::Unknown,
                services: Vec::new(),
                current_cleanup: None,
                pending_cleanup: Vec::new(),
                cleanup: Vec::new(),
                budget_exceeded: false,
                driver_lost: false,
            },
            wake,
        );
        let observer = AppObserver {
            shared: shared.clone(),
        };
        let registry = Arc::new(Registry::default());
        for (label, close) in self.cleanup {
            registry
                .register(label, close)
                .expect("cleanup identity fits in memory");
        }
        let driver = driver::Driver::new(
            self.options,
            self.services,
            self.dependencies,
            registry,
            observer.clone(),
            runtime.clone(),
        );
        runtime.spawn(driver.run());
        Ok(AppHandle {
            control: Arc::new(Control { observer }),
        })
    }

    /// Run until actual termination or the host's stop signal, then await all cleanup.
    ///
    /// This wait has no deadline. Unconfirmed aggregate work, started blocking
    /// work or pending cleanup can keep it waiting indefinitely. For bounded
    /// observation, use [`start`](Self::start) and [`AppHandle::shutdown_within`];
    /// a Pending result leaves responsibility owned by the driver and requires
    /// the host to retain its runtime or choose an explicit process-exit policy.
    pub async fn run_until<F, E>(self, stop: F) -> Result<Arc<ExitReport>, RunError>
    where
        F: Future<Output = Result<(), E>> + Send,
        E: Into<BoxError> + Send + 'static,
    {
        let handle = self.start()?;
        tokio::pin!(stop);
        let stop_source = tokio::select! {
            biased;
            result = handle.wait() => return classify(result, None),
            result = &mut stop => result.err().map(Into::into),
        };
        handle.stop();
        classify(handle.wait().await, stop_source)
    }
}

fn classify(
    result: Result<Arc<ExitReport>, DriverLost>,
    stop_source: Option<BoxError>,
) -> Result<Arc<ExitReport>, RunError> {
    match result {
        Err(source) => Err(RunError::DriverLost {
            source,
            stop_source,
        }),
        Ok(report) => match stop_source {
            Some(source) => Err(RunError::StopSource { source, report }),
            None if !report.is_success() => Err(RunError::Failed { report }),
            None => Ok(report),
        },
    }
}

struct Control {
    observer: AppObserver,
}

impl Drop for Control {
    fn drop(&mut self) {
        self.observer.shared.stop();
    }
}

/// Cloneable application control; the final control lease drop requests stop.
#[derive(Clone)]
pub struct AppHandle {
    control: Arc<Control>,
}

impl fmt::Debug for AppHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl AppHandle {
    /// Observe without retaining a control lease.
    pub fn observer(&self) -> AppObserver {
        self.control.observer.clone()
    }

    /// Read current readiness, health, and pending responsibility.
    pub fn snapshot(&self) -> Arc<AppSnapshot> {
        self.control.observer.snapshot()
    }

    /// Wait for current readiness within this observation interval.
    pub async fn ready(&self, within: Duration) -> Result<Arc<AppSnapshot>, ReadyError> {
        self.control.observer.ready(within).await
    }

    /// Request dependency-ordered stop once, immediately withdrawing readiness.
    pub fn stop(&self) {
        self.control.observer.shared.stop();
    }

    /// Wait for actual completion without classifying business failures.
    pub async fn wait(&self) -> Result<Arc<ExitReport>, DriverLost> {
        self.control.observer.wait().await
    }

    /// Request stop and wait for a successful actual-completion report.
    pub async fn shutdown(&self) -> Result<Arc<ExitReport>, RunError> {
        self.stop();
        classify(self.wait().await, None)
    }

    /// Request stop and observe for a bounded interval; Pending retains responsibility.
    ///
    /// This does not bound Tokio runtime destruction: runtime drop can still wait
    /// for blocking work that has started or is still queued and may start during
    /// shutdown. The host owns its runtime and process-exit policy.
    pub async fn shutdown_within(&self, within: Duration) -> Result<ShutdownProgress, DriverLost> {
        self.stop();
        self.control.observer.drain(within).await
    }
}
