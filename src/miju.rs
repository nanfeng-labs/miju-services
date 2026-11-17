//! Run miju Workers and Monitors as services.
//!
//! miju keeps its backend, middleware, retries and concurrency. The adapter
//! supplies the lifecycle: the [`Seat`] a factory builds its worker with, stop
//! forwarded after the worker's real start, the stream driven to its end so
//! handlers finish and the backend closes, and an executor that makes each
//! parallelized job managed work of the service.
//!
//! What counts as success:
//!
//! - **Worker**: the first backend error fails the service, with that error.
//!   The adapter then still stops and drains the stream, so running handlers
//!   finish and `poll_close` runs. A second error while draining drops the
//!   stream. Job errors are counted in [`MijuStats`] and fail nothing.
//! - **Monitor**: decided by the result of `Monitor::run`. Restarts are
//!   recorded in [`MijuStats`] and do not fail the service by themselves.
//!
//! Jobs run with [`Seat::executor`] hold a lease until they end, including
//! after their stream was dropped by a backend error, a Monitor restart or an
//! abort at grace expiry. Jobs parallelized with `tokio::spawn` are detached
//! and outside the library's scope.
#![forbid(unsafe_code)]

use crate::{
    SharedError,
    contain::{drop_contained, panic_message},
    scope::{Rejected, Scope, Work, WorkError},
    service::{Context, Service},
};
use futures_core::Stream;
use miju::prelude::{Event, Monitor, MonitorContext, MonitorError, WorkerContext, WorkerError};
use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll},
};

/// How many recent job errors a worker service keeps.
const RECENT_JOB_ERRORS: usize = 64;

/// What a worker or monitor factory is given.
///
/// `WorkerBuilder::new(&seat)` builds a worker with the identity the adapter
/// controls. [`Seat::executor`] is an executor for miju's `parallelize`.
#[derive(Clone)]
pub struct Seat {
    name: Arc<str>,
    scope: Scope,
    control: WorkerContext,
}

impl fmt::Debug for Seat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Seat")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Seat {
    fn new(ctx: &Context) -> Self {
        Self {
            name: Arc::from(ctx.name()),
            scope: ctx.scope(),
            control: WorkerContext::new(ctx.name()),
        }
    }

    /// The service name; also the name of the worker built with `&seat`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The service's scope.
    pub fn scope(&self) -> Scope {
        self.scope.clone()
    }

    /// An executor for miju's `parallelize`: each job becomes managed work of
    /// this service and holds a lease until it ends. A job miju has dispatched
    /// still runs when stop arrives first: before its first poll, or before
    /// the executor is called. In the second case admission is closed, so the
    /// job runs inside the future miju polls in the worker's stream, owned by
    /// the root like an in-stream job: the grace period's abort cuts it off,
    /// and so does anything else that ends the stream early, such as a second
    /// backend error. This relies on miju polling the job inside the service's
    /// stream, as `parallelize` in the worker does. A job's panic is contained
    /// on both paths and reported as a lost job.
    ///
    /// Add `parallelize` before `retry` and `timeout`, as miju recommends, so
    /// a retry chain is one job. A job refused because the scope is full is
    /// reported by miju as a failed job; keep miju's `concurrency` below the
    /// service's capacity.
    pub fn executor<Fut>(
        &self,
    ) -> impl Fn(Fut) -> ManagedJob<Fut::Output> + Clone + Send + Sync + 'static + use<Fut>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let scope = self.scope.clone();
        move |job| {
            ManagedJob(
                // Dispatched means in flight for miju: it runs even when stop
                // arrives before its first poll.
                match scope
                    .spawn_dispatched("miju-job", async move { Ok::<_, Infallible>(job.await) })
                {
                    Ok(work) => JobState::Running(work),
                    // Stop closed admission after miju took the job: run it
                    // inside this future, which miju polls in the stream, so
                    // the root owns it as it would an in-stream job.
                    Err((Rejected::Closed, job)) => JobState::Inline(Box::pin(job)),
                    Err((rejected, _job)) => JobState::Rejected(Some(rejected)),
                },
            )
        }
    }
}

/// `WorkerBuilder::new(&seat)` builds the worker with the adapter's identity.
impl From<&Seat> for WorkerContext {
    fn from(seat: &Seat) -> Self {
        seat.control.clone()
    }
}

/// One parallelized job, as miju awaits it. Dropping it does not cancel the
/// job: the job keeps its lease until it ends or is aborted.
pub struct ManagedJob<T>(JobState<T>);

enum JobState<T> {
    Running(Work<T>),
    Inline(Pin<Box<dyn Future<Output = Result<T, Infallible>> + Send>>),
    Rejected(Option<Rejected>),
}

impl<T> fmt::Debug for ManagedJob<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedJob").finish_non_exhaustive()
    }
}

impl<T> Future for ManagedJob<T> {
    type Output = Result<T, JobError>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let state = &mut self.get_mut().0;
        if let JobState::Inline(job) = state {
            // Contained as on the managed path: a panicking job is a lost job,
            // not a failure of the stream that polls it.
            let polled = catch_unwind(AssertUnwindSafe(|| job.as_mut().poll(cx)));
            return match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(Ok(output))) => Poll::Ready(Ok(output)),
                Ok(Poll::Ready(Err(never))) => match never {},
                Err(payload) => {
                    drop_contained(std::mem::replace(state, JobState::Rejected(None)));
                    Poll::Ready(Err(JobError::Lost(WorkError::Panicked(panic_message(
                        payload,
                    )))))
                }
            };
        }
        match state {
            JobState::Running(work) => Pin::new(work).poll(cx).map_err(JobError::Lost),
            JobState::Rejected(rejected) => Poll::Ready(Err(JobError::Rejected(
                rejected.take().expect("polled after completion"),
            ))),
            JobState::Inline(_) => unreachable!("handled above"),
        }
    }
}

/// Why a parallelized job produced no result of its own.
#[derive(Clone, Debug, thiserror::Error)]
pub enum JobError {
    /// The service's scope was full; the job never ran.
    #[error("job rejected: {0}")]
    Rejected(Rejected),
    /// The job was cancelled, aborted or panicked.
    #[error("job lost: {0}")]
    Lost(WorkError),
}

/// Why a worker or monitor service failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdapterError {
    /// The stream started while the seat's context was still pending: the
    /// factory built the worker with another identity, so stop could not
    /// reach it.
    #[error("the worker was not built with its seat; use WorkerBuilder::new(&seat)")]
    ForeignContext,
    /// The first backend error. `drained` tells whether the stream then
    /// reached its end, or was dropped after a second error.
    #[error("backend failed: {error}")]
    Backend {
        /// The first error the stream returned.
        #[source]
        error: WorkerError,
        /// Whether running handlers finished and the backend was closed.
        drained: bool,
    },
    /// miju refused the stop request.
    #[error("the worker refused to stop: {0}")]
    Stop(#[source] WorkerError),
    /// `Monitor::run` returned an error.
    #[error("monitor failed: {0}")]
    Monitor(#[source] MonitorError),
}

/// What a worker or monitor service observed. Published with the service's
/// snapshots; read it with `ServiceSnapshot::detail::<MijuStats>()`.
#[derive(Default)]
pub struct MijuStats {
    job_errors: AtomicU64,
    recent: Mutex<VecDeque<SharedError>>,
    monitor: OnceLock<MonitorContext>,
}

impl fmt::Debug for MijuStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MijuStats")
            .field("job_errors", &self.job_errors())
            .field("restarts", &self.restarts())
            .finish_non_exhaustive()
    }
}

impl MijuStats {
    /// Job errors the worker reported. They do not fail the service.
    pub fn job_errors(&self) -> u64 {
        self.job_errors.load(Ordering::Relaxed)
    }

    /// The most recent job errors, oldest first.
    pub fn recent_job_errors(&self) -> Vec<SharedError> {
        self.recent
            .lock()
            .expect("stats run no user code")
            .iter()
            .cloned()
            .collect()
    }

    /// For a monitor: each registered worker's name and restart count.
    pub fn restarts(&self) -> Vec<(String, usize)> {
        self.monitor.get().map_or_else(Vec::new, |control| {
            control
                .workers()
                .iter()
                .map(|worker| (worker.name(), worker.restarts()))
                .collect()
        })
    }

    fn record_job_error(&self, error: Arc<miju::error::BoxDynError>) {
        self.job_errors.fetch_add(1, Ordering::Relaxed);
        let evicted = {
            let mut recent = self.recent.lock().expect("stats run no user code");
            recent.push_back(Arc::new(JobFailure(error)));
            if recent.len() > RECENT_JOB_ERRORS {
                recent.pop_front()
            } else {
                None
            }
        };
        // An evicted error is user data: dropped outside the lock, contained.
        drop_contained(evicted);
    }
}

/// A job error, keeping miju's error as its source.
#[derive(Debug)]
struct JobFailure(Arc<miju::error::BoxDynError>);

impl fmt::Display for JobFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for JobFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

/// A service that builds a worker with `factory` and drives its stream.
///
/// The factory runs once, when the service starts, and must build the worker
/// with `WorkerBuilder::new(&seat)`. The service is ready at the worker's real
/// `Start`, which does not mean a remote backend is ready. Register a finite
/// source, such as a finite schedule, with `Spec::finite()`: it is then
/// initialized, and its dependents start, only once the source is exhausted
/// and the service has drained. If it is required, the application is not
/// ready before that.
pub fn worker<F, S>(factory: F) -> Worker<F>
where
    F: FnOnce(Seat) -> S + Send + 'static,
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    Worker(factory)
}

/// A service that runs the `Monitor` that `factory` builds.
///
/// Stop shuts the monitor down and waits for the same `run()` to return. The
/// service is ready once `run()` has been polled, when miju has started every
/// registered worker; a finite monitor is initialized when `run()` has
/// returned successfully and the service has drained. A worker's failure can be delayed by its siblings; that
/// is the Monitor's grouping. Register workers as separate services when each
/// failure must stop the application at once.
pub fn monitor<F>(factory: F) -> MonitorService<F>
where
    F: FnOnce(Seat) -> Monitor + Send + 'static,
{
    MonitorService(factory)
}

/// See [`worker`].
pub struct Worker<F>(F);

impl<F> fmt::Debug for Worker<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Worker(..)")
    }
}

/// See [`monitor`].
pub struct MonitorService<F>(F);

impl<F> fmt::Debug for MonitorService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MonitorService(..)")
    }
}

impl<F, S> Service for Worker<F>
where
    F: FnOnce(Seat) -> S + Send + 'static,
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    type Error = AdapterError;

    async fn run(self, ctx: Context) -> Result<(), AdapterError> {
        let stats = Arc::new(MijuStats::default());
        ctx.publish(stats.clone());
        let seat = Seat::new(&ctx);
        let control = seat.control.clone();
        let events = (self.0)(seat);
        drive_worker(&ctx, &control, events, &stats).await
    }
}

/// Drive a worker stream to its end.
///
/// Stop is forwarded after the real `Start`. The first `Err` item requests
/// stop and keeps polling, so handlers finish and `poll_close` runs; a second
/// `Err` drops the stream.
async fn drive_worker<S>(
    ctx: &Context,
    control: &WorkerContext,
    events: S,
    stats: &MijuStats,
) -> Result<(), AdapterError>
where
    S: Stream<Item = Result<Event, WorkerError>>,
{
    let mut events = pin!(events);
    let mut started = false;
    let mut stop_wanted = false;
    let mut stop_sent = false;
    let mut first_error = None;
    loop {
        tokio::select! {
            biased;
            _ = ctx.stopping(), if !stop_wanted => stop_wanted = true,
            event = poll_fn(|cx| events.as_mut().poll_next(cx)) => match event {
                Some(Ok(Event::Start)) => {
                    if control.is_pending() {
                        return Err(AdapterError::ForeignContext);
                    }
                    started = true;
                    ctx.ready();
                }
                Some(Ok(Event::Error(error))) => stats.record_job_error(error),
                Some(Ok(_)) => {}
                Some(Err(error)) => match first_error.take() {
                    // A second error while draining: stop trying, drop the stream.
                    Some(first) => {
                        return Err(AdapterError::Backend { error: first, drained: false });
                    }
                    None => {
                        first_error = Some(error);
                        stop_wanted = true;
                    }
                },
                None => {
                    return match first_error {
                        Some(error) => Err(AdapterError::Backend { error, drained: true }),
                        None => Ok(()),
                    };
                }
            },
        }
        if stop_wanted && started && !stop_sent {
            stop_sent = true;
            if !control.is_terminated() {
                control.stop().map_err(AdapterError::Stop)?;
            }
        }
    }
}

impl<F> Service for MonitorService<F>
where
    F: FnOnce(Seat) -> Monitor + Send + 'static,
{
    type Error = AdapterError;

    async fn run(self, ctx: Context) -> Result<(), AdapterError> {
        let stats = Arc::new(MijuStats::default());
        ctx.publish(stats.clone());
        let monitor = (self.0)(Seat::new(&ctx));
        let control = monitor.context();
        let _ = stats.monitor.set(control.clone());
        let mut run = pin!(monitor.run());
        // The first poll starts every registered worker.
        let first = poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx))).await;
        let result = match first {
            // Finished on its first poll: never report a finished monitor ready.
            Poll::Ready(result) => result,
            Poll::Pending => {
                ctx.ready();
                tokio::select! {
                    biased;
                    result = &mut run => result,
                    _ = ctx.stopping() => match control.shutdown() {
                        Ok(()) => run.await,
                        Err(error) => Err(error),
                    },
                }
            }
        };
        result.map_err(AdapterError::Monitor)
    }
}
