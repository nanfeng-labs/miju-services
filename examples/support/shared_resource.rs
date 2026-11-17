use miju_services::{App, BoxError, Context, Resource, Result, Service, Spec};
use std::{
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

    async fn close(&self) -> io::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// A service with its own settings, a shared resource and a concrete error.
pub(crate) struct CountingService {
    settings: Settings,
    store: Resource<Arc<CounterStore>>,
}

impl Service for CountingService {
    type Error = BoxError;

    async fn run(self, ctx: Context) -> Result {
        let store = self.store.get(&ctx)?;
        ctx.ready();
        loop {
            tokio::select! {
                _ = ctx.stopping() => return Ok(()),
                _ = tokio::time::sleep(self.settings.interval) => {
                    store.count.fetch_add(self.settings.increment, Ordering::SeqCst);
                }
            }
        }
    }

    fn check(&self) -> Result {
        if self.settings.interval.is_zero() {
            return Err("interval must be positive".into());
        }
        Ok(())
    }
}

/// The store is handed in already built. It becomes the application's
/// responsibility only when the `store` service runs, which is also where its
/// close is registered: one place, and nothing to roll back before start.
pub(crate) fn assemble(settings: Settings, store: Arc<CounterStore>) -> App {
    let mut app = App::new();
    let store = app.resource("store", move |ctx| async move {
        let closing = store.clone();
        ctx.defer("counter-store", async move { closing.close().await })?;
        Ok(store)
    });
    app.service(
        Spec::new("counter").after(&store),
        CountingService { settings, store },
    );
    app
}
