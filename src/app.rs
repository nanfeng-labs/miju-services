//! Assembling and running an application.
//!
//! Register services, then run. Nothing a service owns is acquired before
//! start is accepted, so an unstarted [`App`] holds no resource that needs an
//! asynchronous close through this library.
#![forbid(unsafe_code)]

use crate::{
    BoxError,
    driver::{Driver, Entry},
    observe::{AppHandle, Core, Lost},
    report::Report,
    scope::Scope,
    service::{Context, Resource, Service, ServiceCell},
};
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{runtime::Handle, sync::Notify, time::Instant};

/// How a service is registered. A plain name gives every default: required,
/// resident, no dependencies, room for 1024 pieces of work.
#[derive(Clone, Debug)]
pub struct Spec {
    name: Arc<str>,
    after: Vec<Arc<str>>,
    optional: bool,
    finite: bool,
    capacity: NonZeroUsize,
}

impl Spec {
    /// A required, resident service with the default work capacity.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into()),
            after: Vec::new(),
            optional: false,
            finite: false,
            capacity: NonZeroUsize::new(1024).expect("1024 is not zero"),
        }
    }

    /// Start after `provider` is initialized, and keep it running until this
    /// service has drained. Accepts a service name or a [`Resource`].
    #[must_use]
    pub fn after(mut self, provider: impl DependsOn) -> Self {
        self.after.push(provider.provider());
        self
    }

    /// A failure stops and drains only this service, and degrades health.
    #[must_use]
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    /// The root is a one-off step: it is initialized by succeeding and
    /// draining, and has no startup timeout.
    #[must_use]
    pub fn finite(mut self) -> Self {
        self.finite = true;
        self
    }

    /// How much work and how many leases may be outstanding at once.
    #[must_use]
    pub fn capacity(mut self, work: NonZeroUsize) -> Self {
        self.capacity = work;
        self
    }
}

impl From<&str> for Spec {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for Spec {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

/// Something a service can be registered `after`.
pub trait DependsOn {
    /// The provider's registered name.
    fn provider(&self) -> Arc<str>;
}

impl DependsOn for &str {
    fn provider(&self) -> Arc<str> {
        Arc::from(*self)
    }
}

impl DependsOn for String {
    fn provider(&self) -> Arc<str> {
        Arc::from(self.as_str())
    }
}

impl<T> DependsOn for &Resource<T> {
    fn provider(&self) -> Arc<str> {
        Arc::from(self.name())
    }
}

/// Why an assembly cannot be started. No service was invoked.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AssemblyError {
    /// No service was registered.
    #[error("no services registered")]
    Empty,
    /// A name is empty or only whitespace.
    #[error("a service name is empty")]
    EmptyName,
    /// Two services share a name.
    #[error("duplicate service: {0}")]
    Duplicate(String),
    /// A dependency names a service that was not registered.
    #[error("{service} depends on unknown service {provider}")]
    UnknownDependency {
        /// The dependent service.
        service: String,
        /// The missing provider.
        provider: String,
    },
    /// A service lists the same provider twice.
    #[error("{service} repeats dependency {provider}")]
    DuplicateDependency {
        /// The dependent service.
        service: String,
        /// The repeated provider.
        provider: String,
    },
    /// An optional service cannot be something others wait for.
    #[error("optional service cannot be a dependency provider: {0}")]
    OptionalProvider(String),
    /// The dependencies form a cycle.
    #[error("service dependencies contain a cycle")]
    Cycle,
    /// A budget is zero or cannot be added to the clock.
    #[error("invalid lifecycle budget: {0}")]
    InvalidBudget(&'static str),
    /// A service rejected its own settings.
    #[error("service {service} is misconfigured: {error}")]
    Service {
        /// The service's name.
        service: String,
        /// What its `check` reported.
        error: String,
    },
}

struct Registration {
    spec: Spec,
    entry: Entry,
    checked: Result<(), String>,
}

struct Plan {
    providers: Vec<Vec<usize>>,
    ranks: Vec<usize>,
}

/// An application being assembled.
pub struct App {
    services: Vec<Registration>,
    startup_timeout: Duration,
    shutdown_grace: Duration,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("App")
            .field(
                "services",
                &self.services.iter().map(|s| &s.spec).collect::<Vec<_>>(),
            )
            .field("startup_timeout", &self.startup_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

impl App {
    /// An empty assembly with 30-second startup and stop budgets.
    pub fn new() -> Self {
        Self {
            services: Vec::new(),
            startup_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
        }
    }

    /// Register an `async fn`, an adapter, or a type implementing [`Service`].
    pub fn service(&mut self, spec: impl Into<Spec>, service: impl Service) -> &mut Self {
        let checked = service.check().map_err(|error| error.to_string());
        self.services.push(Registration {
            spec: spec.into(),
            entry: Box::new(move |ctx| {
                Box::pin(async move { service.run(ctx).await.map_err(Into::into) })
            }),
            checked,
        });
        self
    }

    /// Register a closure. The bound is on this method, which is what lets
    /// `|ctx| async move { .. }` infer its parameter type.
    pub fn service_fn<F, Fut>(&mut self, spec: impl Into<Spec>, run: F) -> &mut Self
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.service(spec, run)
    }

    /// Register a finite service that acquires a value for others to use.
    ///
    /// The value is acquired only after start is accepted. Register its
    /// cleanup with [`Context::defer`] right where it is acquired.
    pub fn resource<T, F, Fut>(&mut self, spec: impl Into<Spec>, acquire: F) -> Resource<T>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, BoxError>> + Send + 'static,
    {
        let spec = spec.into().finite();
        let resource = Resource::new(spec.name.clone());
        let published = resource.clone();
        self.service_fn(spec, move |ctx| async move {
            published.set(acquire(ctx).await?);
            Ok(())
        });
        resource
    }

    /// How long a resident service may take to call `ready()`. Default 30 s.
    pub fn startup_timeout(&mut self, within: Duration) -> &mut Self {
        self.startup_timeout = within;
        self
    }

    /// How long each service has, from its own stop request, before its root
    /// and its asynchronous work are aborted. Default 30 s.
    pub fn shutdown_grace(&mut self, within: Duration) -> &mut Self {
        self.shutdown_grace = within;
        self
    }

    /// Check names, dependencies, budgets and each service's own settings.
    pub fn validate(&self) -> Result<(), AssemblyError> {
        self.plan().map(|_| ())
    }

    fn plan(&self) -> Result<Plan, AssemblyError> {
        if self.services.is_empty() {
            return Err(AssemblyError::Empty);
        }
        let now = Instant::now();
        for (name, budget) in [
            ("startup_timeout", self.startup_timeout),
            ("shutdown_grace", self.shutdown_grace),
        ] {
            if budget.is_zero() || now.checked_add(budget).is_none() {
                return Err(AssemblyError::InvalidBudget(name));
            }
        }
        let mut names = BTreeMap::new();
        for (index, service) in self.services.iter().enumerate() {
            let name = &*service.spec.name;
            if name.trim().is_empty() {
                return Err(AssemblyError::EmptyName);
            }
            if names.insert(name, index).is_some() {
                return Err(AssemblyError::Duplicate(name.into()));
            }
        }
        let mut providers = Vec::with_capacity(self.services.len());
        for service in &self.services {
            let mut declared = Vec::new();
            for provider in &service.spec.after {
                let Some(&index) = names.get(&**provider) else {
                    return Err(AssemblyError::UnknownDependency {
                        service: service.spec.name.to_string(),
                        provider: provider.to_string(),
                    });
                };
                if declared.contains(&index) {
                    return Err(AssemblyError::DuplicateDependency {
                        service: service.spec.name.to_string(),
                        provider: provider.to_string(),
                    });
                }
                if self.services[index].spec.optional {
                    return Err(AssemblyError::OptionalProvider(provider.to_string()));
                }
                declared.push(index);
            }
            providers.push(declared);
        }
        // Rank: the length of the longest chain of providers below a service.
        let mut ranks: Vec<Option<usize>> = vec![None; self.services.len()];
        let mut remaining = self.services.len();
        while remaining > 0 {
            let before = remaining;
            for index in 0..self.services.len() {
                if ranks[index].is_some() {
                    continue;
                }
                let below: Option<Vec<usize>> = providers[index]
                    .iter()
                    .map(|&provider| ranks[provider])
                    .collect();
                if let Some(below) = below {
                    ranks[index] = Some(below.into_iter().max().map_or(0, |rank| rank + 1));
                    remaining -= 1;
                }
            }
            if remaining == before {
                return Err(AssemblyError::Cycle);
            }
        }
        for service in &self.services {
            if let Err(error) = &service.checked {
                return Err(AssemblyError::Service {
                    service: service.spec.name.to_string(),
                    error: error.clone(),
                });
            }
        }
        Ok(Plan {
            providers,
            ranks: ranks
                .into_iter()
                .map(|rank| rank.expect("every rank is set"))
                .collect(),
        })
    }

    /// Start the application on the current Tokio runtime.
    ///
    /// The runtime must stay alive until the report arrives. On refusal the
    /// unstarted `App` comes back in the error.
    pub fn start(self) -> Result<AppHandle, StartError> {
        let plan = match self.plan() {
            Ok(plan) => plan,
            Err(error) => {
                return Err(StartError::Invalid {
                    error,
                    app: Unstarted::new(self),
                });
            }
        };
        let Ok(runtime) = Handle::try_current() else {
            return Err(StartError::NoRuntime {
                app: Unstarted::new(self),
            });
        };
        if !has_time_driver(&runtime) {
            return Err(StartError::NoTimeDriver {
                app: Unstarted::new(self),
            });
        }
        let wake = Arc::new(Notify::new());
        let mut cells = Vec::with_capacity(self.services.len());
        let mut entries = Vec::with_capacity(self.services.len());
        for ((registration, providers), rank) in self
            .services
            .into_iter()
            .zip(plan.providers)
            .zip(plan.ranks)
        {
            let spec = registration.spec;
            cells.push(Arc::new(ServiceCell {
                name: spec.name,
                required: !spec.optional,
                finite: spec.finite,
                rank,
                providers: spec.after,
                scope: Scope::new(spec.capacity.get(), runtime.clone(), wake.clone()),
                state: Mutex::default(),
                detail: Mutex::default(),
            }));
            entries.push((registration.entry, providers));
        }
        let core = Core::new(cells, wake);
        let driver = Driver::new(
            core.clone(),
            entries,
            self.startup_timeout,
            self.shutdown_grace,
        );
        runtime.spawn(driver.run());
        Ok(AppHandle::new(core))
    }

    /// Run until the application ends by itself. No deadline.
    pub async fn run(self) -> Result<Arc<Report>, RunError> {
        self.run_until(std::future::pending::<Result<(), std::convert::Infallible>>())
            .await
    }

    /// Run until the application ends or `stop` resolves, then wait for every
    /// service to drain and every cleanup action to finish. No deadline.
    ///
    /// For a bounded wait, use [`App::start`] and put a timeout around
    /// [`AppHandle::wait`]: an elapsed timeout ends the observation, nothing else.
    pub async fn run_until<S, E>(self, stop: S) -> Result<Arc<Report>, RunError>
    where
        S: Future<Output = Result<(), E>> + Send,
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
    result: Result<Arc<Report>, Lost>,
    stop_source: Option<BoxError>,
) -> Result<Arc<Report>, RunError> {
    match (result, stop_source) {
        (Err(lost), stop_source) => Err(RunError::Lost { lost, stop_source }),
        (Ok(report), Some(source)) => Err(RunError::StopSource { source, report }),
        (Ok(report), None) if !report.is_success() => Err(RunError::Failed { report }),
        (Ok(report), None) => Ok(report),
    }
}

/// Tokio has no query for its time driver; the only signal is the panic it
/// raises when a timer is created without one. Where panics abort, that
/// cannot be caught, so the runtime is taken at its word.
fn has_time_driver(runtime: &Handle) -> bool {
    use std::{
        panic::catch_unwind,
        pin::pin,
        task::{Context as TaskContext, Waker},
    };
    if cfg!(panic = "abort") {
        return true;
    }
    let _context = runtime.enter();
    catch_unwind(|| {
        let mut sleep = pin!(tokio::time::sleep(Duration::ZERO));
        let _ = sleep
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()));
    })
    .is_ok()
}

/// An `App` that was refused at start, kept so it can be recovered.
pub struct Unstarted(Mutex<Option<Box<App>>>);

impl Unstarted {
    fn new(app: App) -> Self {
        Self(Mutex::new(Some(Box::new(app))))
    }
}

impl fmt::Debug for Unstarted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Unstarted(..)")
    }
}

/// Why an application was not started. Nothing ran; the `App` is in the error.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The assembly is invalid.
    #[error("invalid assembly: {error}")]
    Invalid {
        /// What is wrong with it.
        #[source]
        error: AssemblyError,
        /// The unstarted application.
        app: Unstarted,
    },
    /// `start` was not called inside a Tokio runtime.
    #[error("cannot start without a current Tokio runtime")]
    NoRuntime {
        /// The unstarted application.
        app: Unstarted,
    },
    /// The runtime was built without `enable_time()`.
    #[error("the Tokio runtime has no time driver; build it with enable_time()")]
    NoTimeDriver {
        /// The unstarted application.
        app: Unstarted,
    },
}

impl StartError {
    /// Recover the application that was not started.
    pub fn into_app(self) -> App {
        let (Self::Invalid { app, .. } | Self::NoRuntime { app } | Self::NoTimeDriver { app }) =
            self;
        *app.0
            .into_inner()
            .ok()
            .flatten()
            .expect("the application is taken once")
    }
}

/// Why `run` or `run_until` did not return a successful report.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The application was not started.
    #[error(transparent)]
    Start(#[from] StartError),
    /// Everything finished, and something failed. The report says what.
    #[error("the application finished with failures: {report}")]
    Failed {
        /// The final report.
        report: Arc<Report>,
    },
    /// The stop source itself failed. Everything still finished.
    #[error("the stop source failed: {source}")]
    StopSource {
        /// The stop source's error.
        #[source]
        source: BoxError,
        /// The final report.
        report: Arc<Report>,
    },
    /// The driver was lost. There is no report.
    #[error("the lifecycle driver was lost")]
    Lost {
        /// The last observable state.
        #[source]
        lost: Lost,
        /// The stop source's error, if it had already failed.
        stop_source: Option<BoxError>,
    },
}

impl RunError {
    /// The final report, when the application did finish.
    pub fn report(&self) -> Option<&Arc<Report>> {
        match self {
            Self::Failed { report } | Self::StopSource { report, .. } => Some(report),
            Self::Start(_) | Self::Lost { .. } => None,
        }
    }
}
