//! miju Workers and Monitors as services, against miju 0.3.0.

use miju::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    prelude::{
        Event, MemoryStorage, Monitor, ParallelizeExt, RetryPolicy, Task, TaskSink, WorkerBuilder,
        WorkerBuilderExt, WorkerContext, WorkerError,
    },
    task::task_id::RandomId,
};
use miju_services::{
    App, AppHandle, BoxError, RunError, Spec,
    miju::{AdapterError, MijuStats, monitor, worker},
    report::Failure,
};
use std::{
    collections::VecDeque,
    error::Error as _,
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{Notify, oneshot},
    time::timeout,
};

const SOON: Duration = Duration::from_millis(150);

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("the miju path stalled")
}

async fn until(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

fn finite(name: &str) -> Spec {
    Spec::new(name).finite()
}

fn restarts(app: &AppHandle, service: &str) -> Vec<(String, usize)> {
    let snapshot = app.snapshot();
    snapshot
        .service(service)
        .unwrap()
        .detail::<MijuStats>()
        .unwrap()
        .restarts()
}

enum Step {
    Job(u32),
    Error(&'static str),
}

/// What the backend does once its script is exhausted.
#[derive(Clone, Copy)]
enum Tail {
    /// Stay pending, like a queue with nothing in it.
    Pending,
    /// Report the end of input, like a finite schedule.
    End,
    /// Return the same error on every poll, like miju-cron's cached timestamp error.
    ErrorForever(&'static str),
}

enum Close {
    Ok,
    Error(&'static str),
    /// Pending until the sender is used or dropped.
    Gated(oneshot::Receiver<()>),
}

#[derive(Default)]
struct Probe {
    next_polls: AtomicUsize,
    close_polls: AtomicUsize,
    closed: AtomicBool,
}

struct Scripted {
    steps: VecDeque<Step>,
    tail: Tail,
    close: Close,
    probe: Arc<Probe>,
}

impl Scripted {
    fn new(steps: impl IntoIterator<Item = Step>, tail: Tail) -> (Self, Arc<Probe>) {
        let probe = Arc::new(Probe::default());
        (
            Self {
                steps: steps.into_iter().collect(),
                tail,
                close: Close::Ok,
                probe: probe.clone(),
            },
            probe,
        )
    }
}

impl Backend for Scripted {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Task<u32>>>> {
        self.probe.next_polls.fetch_add(1, SeqCst);
        match self.steps.pop_front() {
            Some(Step::Job(job)) => Poll::Ready(Some(Ok(Task::new(job)))),
            Some(Step::Error(message)) => Poll::Ready(Some(Err(io::Error::other(message)))),
            None => match self.tail {
                // miju wakes the worker itself when it is asked to stop.
                Tail::Pending => Poll::Pending,
                Tail::End => Poll::Ready(None),
                Tail::ErrorForever(message) => Poll::Ready(Some(Err(io::Error::other(message)))),
            },
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        self.probe.close_polls.fetch_add(1, SeqCst);
        match &mut self.close {
            Close::Ok => {}
            Close::Error(message) => return Poll::Ready(Err(io::Error::other(*message))),
            Close::Gated(gate) => {
                if Pin::new(gate).poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }
        self.probe.closed.store(true, SeqCst);
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for Scripted {
    type Args = u32;
    type Config = ();
    type Kind = Ephemeral;
    type Layer = tower::layer::util::Identity;
    type Id = RandomId;

    fn config(&self) -> &() {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}

#[tokio::test]
async fn the_factory_runs_once_with_the_service_name_and_end_of_input_waits_for_close() {
    let (mut backend, probe) = Scripted::new([Step::Job(2), Step::Job(3)], Tail::End);
    let (release, gate) = oneshot::channel();
    backend.close = Close::Gated(gate);
    let calls = Arc::new(AtomicUsize::new(0));
    let sum = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let (calls, sum, probe) = (calls.clone(), sum.clone(), probe.clone());
        app.service(
            finite("jobs"),
            worker(move |seat| {
                assert_eq!(seat.name(), "jobs");
                calls.fetch_add(1, SeqCst);
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .build(move |value: u32| {
                        let (sum, probe) = (sum.clone(), probe.clone());
                        async move {
                            assert!(!probe.closed.load(SeqCst), "a handler ran after close");
                            sum.fetch_add(value as usize, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
    }
    {
        let probe = probe.clone();
        app.service_fn(Spec::new("store").finite(), move |ctx| async move {
            ctx.defer("after-backend", async move {
                assert!(
                    probe.closed.load(SeqCst),
                    "cleanup overtook the backend close"
                );
                Ok::<_, BoxError>(())
            })?;
            Ok(())
        });
    }
    let app = app.start().unwrap();
    until(|| probe.close_polls.load(SeqCst) > 0).await;
    assert_eq!(calls.load(SeqCst), 1);
    assert_eq!(sum.load(SeqCst), 5);
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while the backend was closing"
    );

    release.send(()).unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    assert!(report.service("jobs").unwrap().initialized);
}

#[tokio::test]
async fn job_errors_are_counted_and_do_not_fail_the_lifecycle() {
    let (backend, _) = Scripted::new([Step::Job(1), Step::Job(2), Step::Job(3)], Tail::End);
    let mut app = App::new();
    app.service(
        finite("jobs"),
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                .build(|_: u32| async { Err::<(), _>(io::Error::other("job failure")) })
                .stream()
        }),
    );
    let report = bounded(app.run()).await.unwrap();
    let stats = report
        .service("jobs")
        .unwrap()
        .detail::<MijuStats>()
        .unwrap();
    assert_eq!(stats.job_errors(), 3);
    let recent = stats.recent_job_errors();
    assert_eq!(recent.len(), 3);
    assert!(
        recent[0].source().is_some(),
        "miju's error must stay reachable as the source"
    );
    assert!(recent[0].to_string().contains("job failure"));
}

// P1: a backend error fails the service, but does not leave it unconfirmed.
// The running handler finishes, the backend is closed, and cleanup runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_error_fails_the_service_after_draining_and_cleanup_still_runs() {
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let (backend, probe) = Scripted::new(
        [Step::Job(1), Step::Error("backend failure")],
        Tail::Pending,
    );
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    {
        let (entered, release, finished, cleaned) = (
            entered.clone(),
            release.clone(),
            finished.clone(),
            cleaned.clone(),
        );
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .build(move |_: u32| {
                        let (entered, release, finished) =
                            (entered.clone(), release.clone(), finished.clone());
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            finished.store(true, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
        app.service_fn("store", move |ctx| async move {
            ctx.defer("store", async move {
                cleaned.store(true, SeqCst);
                Ok::<_, BoxError>(())
            })?;
            ctx.ready();
            ctx.stopping().await;
            Ok(())
        });
    }
    let app = app.start().unwrap();
    bounded(entered.notified()).await;
    until(|| probe.next_polls.load(SeqCst) >= 2).await;
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while a handler was running"
    );
    assert!(
        !probe.closed.load(SeqCst),
        "the backend was closed under a running handler"
    );

    release.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(finished.load(SeqCst), "the running handler was cut off");
    assert!(probe.closed.load(SeqCst), "poll_close did not run");
    assert!(
        cleaned.load(SeqCst),
        "cleanup did not run after the backend failure"
    );
    assert!(!report.is_success());
    let failures = &report.service("jobs").unwrap().failures;
    assert!(
        matches!(&failures[..], [Failure::Error(error)]
            if error.to_string().starts_with("backend failed:")
                && error.to_string().contains("backend failure")),
        "{failures:?}"
    );
    let source = failures_source(&report);
    assert!(matches!(
        source,
        Some(AdapterError::Backend { drained: true, .. })
    ));
}

fn failures_source(report: &miju_services::Report) -> Option<&AdapterError> {
    report
        .service("jobs")?
        .failures
        .iter()
        .find_map(|failure| match failure {
            Failure::Error(error) => error.downcast_ref::<AdapterError>(),
            _ => None,
        })
}

// P11: a backend close that never finishes is cut off at grace expiry. The
// report says so; nothing waits for that close forever.
#[tokio::test(start_paused = true)]
async fn a_backend_close_that_never_finishes_is_aborted_after_grace() {
    let (mut backend, probe) = Scripted::new([], Tail::End);
    let (_never, gate) = oneshot::channel();
    backend.close = Close::Gated(gate);
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    app.service(
        finite("jobs"),
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                .build(|_: u32| async {})
                .stream()
        }),
    );
    let app = app.start().unwrap();
    until(|| probe.close_polls.load(SeqCst) > 0).await;
    app.stop();
    assert!(timeout(Duration::from_secs(2), app.wait()).await.is_err());
    let report = app.wait().await.unwrap();
    let service = report.service("jobs").unwrap();
    assert!(service.forced);
    assert!(!report.is_success());
    assert!(!probe.closed.load(SeqCst));
}

#[tokio::test]
async fn stop_drains_the_running_handler_and_leaves_queued_jobs_unstarted() {
    let mut backend = MemoryStorage::new();
    backend.push(1_u32).await.unwrap();
    backend.push(2_u32).await.unwrap();
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let handled = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let (entered, release, handled) = (entered.clone(), release.clone(), handled.clone());
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .concurrency(1)
                    .build(move |_: u32| {
                        let (entered, release, handled) =
                            (entered.clone(), release.clone(), handled.clone());
                        async move {
                            handled.fetch_add(1, SeqCst);
                            entered.notify_one();
                            release.notified().await;
                        }
                    })
                    .stream()
            }),
        );
    }
    let app = app.start().unwrap();
    bounded(entered.notified()).await;
    app.stop();
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while a handler was running"
    );
    release.notify_one();
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert_eq!(handled.load(SeqCst), 1, "a queued job started after stop");
}

#[tokio::test]
async fn a_stop_before_start_is_forwarded_after_the_real_start() {
    use futures_util::{StreamExt, stream};
    let (entered, entry) = oneshot::channel();
    let (release, gate) = oneshot::channel::<()>();
    let mut app = App::new();
    app.service(
        finite("jobs"),
        worker(move |seat| {
            entered.send(()).unwrap();
            let mut control = WorkerContext::from(&seat);
            let stopping = control.clone();
            stream::once(async move {
                gate.await.unwrap();
                control.start()?;
                Ok::<_, WorkerError>(Event::Start)
            })
            .chain(stream::once(async move {
                assert!(
                    !stopping.is_running(),
                    "the pending stop was not forwarded after Start"
                );
                Ok(Event::Exit)
            }))
        }),
    );
    let app = app.start().unwrap();
    bounded(entry).await.unwrap();
    app.stop();
    release.send(()).unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn a_worker_built_with_another_identity_fails_at_start() {
    let (backend, _) = Scripted::new([], Tail::Pending);
    let mut app = App::new();
    app.service(
        "jobs",
        worker(move |_seat| {
            WorkerBuilder::new("not-the-seat")
                .backend(backend)
                .build(|_: u32| async {})
                .stream()
        }),
    );
    let Err(RunError::Failed { report }) = bounded(app.run()).await else {
        panic!("a worker the adapter cannot stop must fail its service");
    };
    assert!(matches!(
        failures_source(&report),
        Some(AdapterError::ForeignContext)
    ));
}

// miju-cron's contract: an unrepresentable timestamp stays cached and is
// returned on every poll. After the stop request miju does not poll again.
#[tokio::test]
async fn a_cached_backend_error_is_seen_once_and_the_worker_still_drains() {
    let (backend, probe) = Scripted::new([], Tail::ErrorForever("cached error"));
    let mut app = App::new();
    app.service(
        "jobs",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                .build(|_: u32| async {})
                .stream()
        }),
    );
    let Err(RunError::Failed { report }) = bounded(app.run()).await else {
        panic!("a backend error fails the service");
    };
    assert!(matches!(
        failures_source(&report),
        Some(AdapterError::Backend { drained: true, .. })
    ));
    assert_eq!(
        probe.next_polls.load(SeqCst),
        1,
        "the cached error was polled again"
    );
    assert!(probe.closed.load(SeqCst));
}

#[tokio::test]
async fn a_second_error_while_draining_drops_the_stream_and_reports_the_first() {
    let (mut backend, probe) = Scripted::new([Step::Error("first")], Tail::Pending);
    backend.close = Close::Error("close fails");
    let mut app = App::new();
    app.service(
        "jobs",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                .build(|_: u32| async {})
                .stream()
        }),
    );
    let Err(RunError::Failed { report }) = bounded(app.run()).await else {
        panic!("a backend error fails the service");
    };
    let Some(AdapterError::Backend { error, drained }) = failures_source(&report) else {
        panic!("expected the backend error");
    };
    assert!(!drained);
    assert!(error.to_string().contains("first"), "{error}");
    assert_eq!(
        probe.close_polls.load(SeqCst),
        1,
        "close was retried after it failed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_jobs_run_as_managed_work_with_retry_and_timeout_layered_as_miju_recommends() {
    let (backend, probe) = Scripted::new([Step::Job(1), Step::Job(2), Step::Job(3)], Tail::End);
    let seen_as_managed = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let seen_as_managed = seen_as_managed.clone();
        app.service(
            finite("jobs"),
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .concurrency(4)
                    .parallelize(seat.executor())
                    .retry(RetryPolicy::retries(1))
                    .timeout(Duration::from_secs(5))
                    .build(move |_job: u32| {
                        let seen_as_managed = seen_as_managed.clone();
                        async move {
                            seen_as_managed.fetch_add(1, SeqCst);
                            Ok::<(), io::Error>(())
                        }
                    })
                    .stream()
            }),
        );
    }
    let report = bounded(app.run()).await.unwrap();
    assert_eq!(seen_as_managed.load(SeqCst), 3);
    let service = report.service("jobs").unwrap();
    assert_eq!(
        service.work.completed, 3,
        "each job ran as managed work of the service"
    );
    assert_eq!(service.detail::<MijuStats>().unwrap().job_errors(), 0);
    assert!(probe.closed.load(SeqCst));
}

// P11 with the managed executor: a job running on the executor is part of
// the service's scope, so the abort pass at grace expiry reaches it too.
#[tokio::test(start_paused = true)]
async fn jobs_on_the_executor_are_aborted_with_the_root_at_grace_expiry() {
    struct Tracked(Arc<AtomicBool>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.store(true, SeqCst);
        }
    }
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let (backend, _) = Scripted::new([Step::Job(1)], Tail::Pending);
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    {
        let (entered, dropped) = (entered.clone(), dropped.clone());
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .parallelize(seat.executor())
                    .build(move |_job: u32| {
                        let (entered, capture) = (entered.clone(), Tracked(dropped.clone()));
                        async move {
                            let _capture = capture;
                            entered.notify_one();
                            std::future::pending::<()>().await;
                        }
                    })
                    .stream()
            }),
        );
    }
    let app = app.start().unwrap();
    entered.notified().await;
    assert!(
        app.snapshot()
            .outstanding()
            .iter()
            .any(|item| item.name.name() == "miju-job")
    );
    app.stop();
    assert!(
        timeout(Duration::from_secs(2), app.wait()).await.is_err(),
        "the job held the service before grace"
    );
    let report = app.wait().await.unwrap();
    assert!(dropped.load(SeqCst), "the job's captures were not released");
    let service = report.service("jobs").unwrap();
    assert!(service.forced);
    assert_eq!(
        service.work.aborted, 1,
        "the job was not aborted as managed work"
    );
    assert!(!report.is_success());
}

#[tokio::test]
async fn a_full_scope_fails_the_job_instead_of_running_it() {
    let (backend, _) = Scripted::new([Step::Job(1)], Tail::End);
    let handled = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let handled = handled.clone();
        app.service(
            finite("jobs").capacity(NonZeroUsize::new(1).unwrap()),
            worker(move |seat| {
                // The only slot is taken for as long as the worker lives.
                let held = seat.scope().lease("held").unwrap();
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .parallelize(seat.executor())
                    .build(move |_job: u32| {
                        let _held = &held;
                        // miju calls this to build the job's future even when the
                        // executor then rejects it; what must not run is the future.
                        let handled = handled.clone();
                        async move {
                            handled.fetch_add(1, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
    }
    let report = bounded(app.run()).await.unwrap();
    assert_eq!(handled.load(SeqCst), 0);
    let stats = report
        .service("jobs")
        .unwrap()
        .detail::<MijuStats>()
        .unwrap();
    assert_eq!(stats.job_errors(), 1);
    assert!(
        stats.recent_job_errors()[0]
            .to_string()
            .contains("capacity is exhausted")
    );
}

/// Fails its first attempt after the test lets it go, then succeeds.
fn fails_once(
    attempts: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
) -> impl Fn(u32) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> + Clone + Send + Sync + 'static
{
    move |_job: u32| {
        let (attempts, entered, release) = (attempts.clone(), entered.clone(), release.clone());
        Box::pin(async move {
            if attempts.fetch_add(1, SeqCst) == 0 {
                entered.notify_one();
                release.notified().await;
                return Err(io::Error::other("first attempt fails"));
            }
            Ok(())
        })
    }
}

// miju 0.3.0 declines a retry once the worker is stopping. The job ends with
// the attempt's own error; the closed gate is never what stops the chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_stop_is_declined_by_miju_with_the_attempts_own_error() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (backend, probe) = Scripted::new([Step::Job(1)], Tail::Pending);
    let mut app = App::new();
    {
        let handler = fails_once(attempts.clone(), entered.clone(), release.clone());
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .parallelize(seat.executor())
                    .retry(RetryPolicy::retries(2))
                    .build(handler)
                    .stream()
            }),
        );
    }
    let app = app.start().unwrap();
    bounded(entered.notified()).await;
    app.stop();
    tokio::time::sleep(SOON).await;
    release.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success(), "a job error does not fail the service");
    assert_eq!(attempts.load(SeqCst), 1);
    let stats = report
        .service("jobs")
        .unwrap()
        .detail::<MijuStats>()
        .unwrap();
    assert_eq!(stats.job_errors(), 1);
    assert!(
        stats.recent_job_errors()[0]
            .to_string()
            .contains("first attempt fails")
    );
    assert!(probe.closed.load(SeqCst));
}

#[tokio::test]
async fn a_monitor_that_finishes_by_itself_ends_without_a_signal() {
    let mut app = App::new();
    app.service(finite("group"), monitor(|_seat| Monitor::new()));
    assert!(bounded(app.run()).await.unwrap().is_success());
}

#[tokio::test]
async fn an_idle_monitor_stops_through_its_own_run_future() {
    let mut app = App::new();
    app.service(
        "group",
        monitor(|_seat| {
            Monitor::new().register(|_| {
                WorkerBuilder::new("worker")
                    .backend(MemoryStorage::new())
                    .build(|_: u32| async {})
            })
        }),
    );
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
}

// P1 for Monitors: a restart is recorded and does not fail the service; the
// first generation's job stays leased until it ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_monitor_restart_is_recorded_and_the_old_generations_job_stays_leased() {
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let generations = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let (entered, release, finished, generations) = (
            entered.clone(),
            release.clone(),
            finished.clone(),
            generations.clone(),
        );
        app.service(
            "fleet",
            monitor(move |seat| {
                Monitor::new()
                    .should_restart(|worker, _error| worker.restarts() == 0)
                    .register(move |generation| {
                        generations.fetch_add(1, SeqCst);
                        let (backend, _) = if generation == 0 {
                            Scripted::new(
                                [Step::Job(1), Step::Error("generation 0 fails")],
                                Tail::Pending,
                            )
                        } else {
                            Scripted::new([], Tail::Pending)
                        };
                        let (entered, release, finished) =
                            (entered.clone(), release.clone(), finished.clone());
                        WorkerBuilder::new("fleet-worker")
                            .backend(backend)
                            .parallelize(seat.executor())
                            .build(move |_job: u32| {
                                let (entered, release, finished) =
                                    (entered.clone(), release.clone(), finished.clone());
                                async move {
                                    entered.notify_one();
                                    release.notified().await;
                                    finished.store(true, SeqCst);
                                }
                            })
                    })
            }),
        );
    }
    let app = app.start().unwrap();
    bounded(entered.notified()).await;
    until(|| generations.load(SeqCst) == 2).await;
    until(|| restarts(&app, "fleet") == [("fleet-worker".to_owned(), 1)]).await;
    assert!(
        app.snapshot()
            .outstanding()
            .iter()
            .any(|item| item.name.name() == "miju-job"),
        "the first generation's job was lost with its stream"
    );

    app.stop();
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while an old job was running"
    );
    release.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(finished.load(SeqCst));
    assert!(
        report.is_success(),
        "a restart alone does not fail the service"
    );
    let stats = report
        .service("fleet")
        .unwrap()
        .detail::<MijuStats>()
        .unwrap();
    assert_eq!(stats.restarts(), [("fleet-worker".to_owned(), 1)]);
}

#[tokio::test]
async fn a_monitor_without_restart_fails_with_the_worker_error() {
    let mut app = App::new();
    app.service(
        "group",
        monitor(|_seat| {
            Monitor::new().register(|_| {
                let (backend, _) = Scripted::new([Step::Error("fatal")], Tail::Pending);
                WorkerBuilder::new("failing")
                    .backend(backend)
                    .build(|_: u32| async {})
            })
        }),
    );
    let Err(RunError::Failed { report }) = bounded(app.run()).await else {
        panic!("the monitor's own error must fail the service");
    };
    let failures = &report.service("group").unwrap().failures;
    assert!(matches!(&failures[..], [Failure::Error(error)]
        if matches!(error.downcast_ref::<AdapterError>(), Some(AdapterError::Monitor(_)))));
    let stats = report
        .service("group")
        .unwrap()
        .detail::<MijuStats>()
        .unwrap();
    assert_eq!(stats.restarts(), [("failing".to_owned(), 0)]);
}

// Real time: miju-cron uses the wall clock and ignores Tokio's paused clock.
#[tokio::test]
async fn miju_cron_is_a_backend_and_stops_promptly_between_ticks() {
    use miju_cron::{CronScheduler, Tick};
    let schedule: cron::Schedule = "* * * * * *".parse().unwrap();
    let (send, receive) = oneshot::channel();
    let send = Arc::new(Mutex::new(Some(send)));
    let mut app = App::new();
    app.service(
        "calendar",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(CronScheduler::new(schedule).with_name(seat.name()))
                .build(move |_: Tick| {
                    let send = send.lock().unwrap().take();
                    async move {
                        if let Some(send) = send {
                            let _ = send.send(());
                        }
                    }
                })
                .stream()
        }),
    );
    let app = app.start().unwrap();
    timeout(Duration::from_secs(5), receive)
        .await
        .expect("no tick within 5 s")
        .unwrap();
    let report = timeout(Duration::from_millis(500), app.shutdown())
        .await
        .expect("stop waited for the next tick")
        .unwrap();
    assert!(report.is_success());
}

// Stop shuts the monitor down and keeps awaiting the same `run()`, so a
// handler running inside a monitored worker finishes instead of being dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_monitor_stop_lets_running_handlers_finish() {
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    {
        let (entered, release, finished) = (entered.clone(), release.clone(), finished.clone());
        app.service(
            "fleet",
            monitor(move |_seat| {
                Monitor::new().register(move |_| {
                    let (backend, _) = Scripted::new([Step::Job(1)], Tail::Pending);
                    let (entered, release, finished) =
                        (entered.clone(), release.clone(), finished.clone());
                    WorkerBuilder::new("fleet-worker")
                        .backend(backend)
                        .build(move |_job: u32| {
                            let (entered, release, finished) =
                                (entered.clone(), release.clone(), finished.clone());
                            async move {
                                entered.notify_one();
                                release.notified().await;
                                finished.store(true, SeqCst);
                            }
                        })
                })
            }),
        );
    }
    let app = app.start().unwrap();
    bounded(entered.notified()).await;
    app.stop();
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while a handler was running"
    );
    release.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(finished.load(SeqCst), "the running handler was dropped");
    assert!(report.is_success());
}

// A backend that requests application stop at the moment it yields its job.
struct StopOnDispatch {
    jobs: VecDeque<u32>,
    handle: Arc<Mutex<Option<AppHandle>>>,
}

impl Backend for StopOnDispatch {
    type Task = Task<u32>;
    type Error = io::Error;
    fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Task<u32>>>> {
        match self.jobs.pop_front() {
            Some(job) => {
                // A stop request arrives at the moment the job is dispatched.
                if let Some(handle) = self.handle.lock().unwrap().as_ref() {
                    handle.stop();
                }
                Poll::Ready(Some(Ok(Task::new(job))))
            }
            None => Poll::Pending,
        }
    }
    fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for StopOnDispatch {
    type Args = u32;
    type Config = ();
    type Kind = Ephemeral;
    type Layer = tower::layer::util::Identity;
    type Id = RandomId;
    fn config(&self) -> &() {
        &()
    }
    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}

#[tokio::test]
async fn a_dispatched_executor_job_runs_even_when_stop_lands_before_its_first_poll() {
    let handle_slot = Arc::new(Mutex::new(None));
    let handled = Arc::new(AtomicUsize::new(0));
    let backend = StopOnDispatch {
        jobs: VecDeque::from([1]),
        handle: handle_slot.clone(),
    };
    let mut app = App::new();
    {
        let handled = handled.clone();
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .parallelize(seat.executor())
                    .build(move |_job: u32| {
                        let handled = handled.clone();
                        async move {
                            handled.fetch_add(1, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
    }
    let app = app.start().unwrap();
    *handle_slot.lock().unwrap() = Some(app.clone());
    let report = bounded(app.wait()).await.unwrap();
    let service = report.service("jobs").unwrap();
    let stats = service.detail::<MijuStats>().unwrap();
    assert_eq!(
        handled.load(SeqCst),
        1,
        "a dequeued job was dropped without running"
    );
    assert_eq!(service.work.cancelled, 0);
    assert_eq!(stats.job_errors(), 0);
    assert!(report.is_success());
}

// a finite worker's dependents must start only after the source is
// exhausted and the worker has drained (design §5.9 / §7.1).
#[tokio::test]
async fn a_finite_worker_dependent_waits_for_exhaustion_and_drain() {
    let (backend, _) = Scripted::new([Step::Job(1)], Tail::End);
    let release = Arc::new(Notify::new());
    let consumer_started_while_job_running = Arc::new(AtomicBool::new(false));
    let job_running = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    {
        let (release, job_running) = (release.clone(), job_running.clone());
        app.service(
            finite("calendar"),
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .build(move |_job: u32| {
                        let (release, job_running) = (release.clone(), job_running.clone());
                        async move {
                            job_running.store(true, SeqCst);
                            release.notified().await;
                            job_running.store(false, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
    }
    {
        let (flag, job_running) = (
            consumer_started_while_job_running.clone(),
            job_running.clone(),
        );
        app.service_fn(
            Spec::new("dependent").finite().after("calendar"),
            move |_ctx| async move {
                flag.store(job_running.load(SeqCst), SeqCst);
                Ok(())
            },
        );
    }
    let app = app.start().unwrap();
    until(|| job_running.load(SeqCst)).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let dependent_phase = app.snapshot().service("dependent").unwrap().phase;
    let calendar_initialized = app.snapshot().service("calendar").unwrap().initialized;
    assert!(
        !calendar_initialized,
        "a running finite worker counted as initialized"
    );
    assert_eq!(
        dependent_phase,
        miju_services::report::ServicePhase::Waiting
    );
    release.notify_one();
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(
        !consumer_started_while_job_running.load(SeqCst),
        "a dependent of a finite worker started before the worker was exhausted and drained"
    );
}

// A backend that, after one empty poll, takes its job off the queue, has the
// application stopped from another thread, waits until the driver has asked
// the service to stop (closing admission), and only then hands the job over.
struct StopDuringDequeue {
    yielded: bool,
    jobs: VecDeque<u32>,
    handle: Arc<Mutex<Option<AppHandle>>>,
    reached_stopping: Arc<AtomicBool>,
}

impl Backend for StopDuringDequeue {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Task<u32>>>> {
        if !self.yielded {
            // Yield once so the driver is not parked in this worker's local slot.
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let Some(job) = self.jobs.pop_front() else {
            return Poll::Pending;
        };
        let handle = self.handle.lock().unwrap().clone();
        if let Some(handle) = handle {
            let observer = handle.observer();
            std::thread::spawn(move || handle.stop()).join().unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                if observer.snapshot().service("jobs").unwrap().phase
                    == miju_services::report::ServicePhase::Stopping
                {
                    self.reached_stopping.store(true, SeqCst);
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        Poll::Ready(Some(Ok(Task::new(job))))
    }

    fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for StopDuringDequeue {
    type Args = u32;
    type Config = ();
    type Kind = Ephemeral;
    type Layer = tower::layer::util::Identity;
    type Id = RandomId;

    fn config(&self) -> &() {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dequeued_job_runs_when_stop_closes_admission_before_the_executor_call() {
    let handle_slot = Arc::new(Mutex::new(None));
    let handled = Arc::new(AtomicUsize::new(0));
    let reached_stopping = Arc::new(AtomicBool::new(false));
    let backend = StopDuringDequeue {
        yielded: false,
        jobs: VecDeque::from([1]),
        handle: handle_slot.clone(),
        reached_stopping: reached_stopping.clone(),
    };
    let mut app = App::new();
    {
        let handled = handled.clone();
        app.service(
            "jobs",
            worker(move |seat| {
                WorkerBuilder::new(&seat)
                    .backend(backend)
                    .parallelize(seat.executor())
                    .build(move |_job: u32| {
                        let handled = handled.clone();
                        async move {
                            handled.fetch_add(1, SeqCst);
                        }
                    })
                    .stream()
            }),
        );
    }
    let app = app.start().unwrap();
    *handle_slot.lock().unwrap() = Some(app.clone());
    let report = bounded(app.wait()).await.unwrap();
    assert!(
        reached_stopping.load(SeqCst),
        "the service never reached Stopping before the job was handed over"
    );
    let service = report.service("jobs").unwrap();
    let stats = service.detail::<MijuStats>().unwrap();
    assert_eq!(
        handled.load(SeqCst),
        1,
        "a dequeued job was dropped without running"
    );
    assert_eq!(stats.job_errors(), 0);
    assert!(report.is_success());
}

async fn panicking_job(stop_window: bool) -> (Arc<miju_services::Report>, bool) {
    let handle_slot = Arc::new(Mutex::new(None));
    let reached_stopping = Arc::new(AtomicBool::new(false));
    let backend = StopDuringDequeue {
        yielded: false,
        jobs: VecDeque::from([1]),
        handle: handle_slot.clone(),
        reached_stopping: reached_stopping.clone(),
    };
    let mut app = App::new();
    app.service(
        "jobs",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                .parallelize(seat.executor())
                .build(move |job: u32| async move {
                    if job == 1 {
                        panic!("job panics");
                    }
                })
                .stream()
        }),
    );
    let app = app.start().unwrap();
    if stop_window {
        *handle_slot.lock().unwrap() = Some(app.clone());
    } else {
        tokio::time::sleep(Duration::from_millis(200)).await;
        app.stop();
    }
    let report = bounded(app.wait()).await.unwrap();
    (report, reached_stopping.load(SeqCst))
}

// A panicking job is a lost job, not a failure of the worker, whether it ran
// as managed work or inside the stream because stop had closed admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_job_is_contained_whether_or_not_stop_closed_admission() {
    for stop_window in [false, true] {
        let (report, reached_stopping) = panicking_job(stop_window).await;
        assert_eq!(
            reached_stopping, stop_window,
            "the stop window was not exercised"
        );
        assert!(report.is_success(), "stop window {stop_window}: {report}");
        let stats = report
            .service("jobs")
            .unwrap()
            .detail::<MijuStats>()
            .unwrap();
        assert_eq!(stats.job_errors(), 1, "stop window {stop_window}");
    }
}
