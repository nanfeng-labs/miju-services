use miju_services::{App, BoxError, BuildError, ExitReport, RunError, Service, ServiceContext};
use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Settings {
    pub(crate) interval: Duration,
    pub(crate) increment: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            increment: 1,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct CounterStore {
    count: AtomicU64,
    closed: AtomicBool,
}

impl CounterStore {
    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) async fn close(&self) -> io::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CountingService {
    settings: Settings,
    store: Arc<CounterStore>,
}

impl CountingService {
    pub(crate) fn new(settings: Settings, store: Arc<CounterStore>) -> Self {
        Self { settings, store }
    }
}

impl Service for CountingService {
    type Error = io::Error;

    async fn run(self, context: ServiceContext) -> Result<(), Self::Error> {
        if self.settings.interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interval must be positive",
            ));
        }
        context.ready();
        loop {
            tokio::select! {
                _ = context.stopping() => break,
                _ = tokio::time::sleep(self.settings.interval) => {
                    self.store.count.fetch_add(self.settings.increment, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn assemble(settings: Settings, store: Arc<CounterStore>) -> Result<App, BuildError> {
    let cleanup = store.clone();
    App::builder()
        .cleanup("counter-store", move || async move {
            cleanup.close().await?;
            Ok(())
        })
        .service("counter", CountingService::new(settings, store))
        .build()
}

/// Bootstrap retains access for rollback until the lifecycle driver accepts the app.
pub(crate) async fn run<F, E>(
    settings: Settings,
    store: Arc<CounterStore>,
    stop: F,
) -> Result<Arc<ExitReport>, BootstrapError>
where
    F: Future<Output = Result<(), E>> + Send,
    E: Into<BoxError> + Send + 'static,
{
    let app = match assemble(settings, store.clone()) {
        Ok(app) => app,
        Err(error) => {
            return Err(BootstrapError {
                source: error.into(),
                rollback: store.close().await.err(),
            });
        }
    };
    match app.run_until(stop).await {
        Ok(report) => Ok(report),
        Err(error) => {
            let rollback = if matches!(&error, RunError::Start(_)) {
                store.close().await.err()
            } else {
                None // The accepted driver owns cleanup, including on service failure.
            };
            Err(BootstrapError {
                source: error.into(),
                rollback,
            })
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("bootstrap failed: {source}; rollback error: {rollback:?}")]
pub(crate) struct BootstrapError {
    pub(crate) source: BoxError,
    pub(crate) rollback: Option<io::Error>,
}
