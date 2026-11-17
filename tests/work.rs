//! Managed work: bounded admission, typed results, and release that follows
//! the actual end of the work rather than what its submitter does.

use miju_services::{
    App, AppHandle, BoxError, Context, Rejected, Spec, WorkError, critical,
    report::{Failure, Phase},
    scope::Kind,
};
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};

const SOON: Duration = Duration::from_millis(150);

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("the lifecycle stalled")
}

/// Start an application with one resident service and hand out its context.
async fn owner(spec: Spec) -> (AppHandle, Context) {
    let (send, receive) = oneshot::channel();
    let mut app = App::new();
    app.service_fn(spec, move |ctx| async move {
        let _ = send.send(ctx.clone());
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let handle = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    bounded(handle.ready()).await.unwrap();
    (handle, ctx)
}

struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

/// Blocks in `Drop` until the test lets it go.
struct BlocksOnDrop {
    gate: mpsc::Receiver<()>,
    dropped: Arc<AtomicBool>,
}

impl BlocksOnDrop {
    fn new() -> (Self, mpsc::Sender<()>, Arc<AtomicBool>) {
        let (release, gate) = mpsc::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Self {
                gate,
                dropped: dropped.clone(),
            },
            release,
            dropped,
        )
    }
}

impl Drop for BlocksOnDrop {
    fn drop(&mut self) {
        let _ = self.gate.recv();
        self.dropped.store(true, SeqCst);
    }
}

struct PanicsOnDrop;

impl Drop for PanicsOnDrop {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            panic!("destructor panic under test");
        }
    }
}

/// Ready at once, but keeps its capture until the future itself is dropped.
/// The flag is set when the future is first polled, that is, when the work starts.
struct Keeps<C, T>(Option<C>, Option<T>, Arc<AtomicBool>);

impl<C: Unpin, T: Unpin> Future for Keeps<C, T> {
    type Output = Result<T, BoxError>;

    fn poll(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.2.store(true, SeqCst);
        Poll::Ready(Ok(self.1.take().expect("polled once")))
    }
}

#[tokio::test]
async fn work_returns_a_typed_value_and_errors_are_recorded() {
    let (app, ctx) = owner("owner".into()).await;
    let value = ctx
        .spawn("answer", async { Ok::<_, BoxError>(42_u64) })
        .unwrap();
    assert_eq!(bounded(value).await.unwrap(), 42);

    let failed = ctx
        .spawn("fails", async {
            Err::<(), _>(io::Error::other("no such device"))
        })
        .unwrap();
    let Err(WorkError::Failed(error)) = bounded(failed).await else {
        panic!("expected the work's own error");
    };
    assert_eq!(error.to_string(), "no such device");

    // Nobody awaits this one; the service still records what happened.
    drop(
        ctx.spawn("unobserved", async {
            Err::<(), _>(io::Error::other("lost reading"))
        })
        .unwrap(),
    );
    let report = bounded(app.shutdown()).await.unwrap();
    let service = report.service("owner").unwrap();
    assert_eq!((service.work.completed, service.work.failed), (1, 2));
    assert_eq!(service.recent_faults.len(), 2);
    assert_eq!(service.recent_faults[1].label.name(), "unobserved");
    assert!(
        report.is_success(),
        "ordinary work errors do not fail the lifecycle"
    );
}

#[tokio::test]
async fn admission_is_bounded_and_a_rejected_future_is_never_polled() {
    let (app, ctx) = owner(Spec::new("owner").capacity(NonZeroUsize::new(2).unwrap())).await;
    let polled = Arc::new(AtomicBool::new(false));
    let (release, held) = oneshot::channel::<()>();
    let first = ctx
        .spawn("held", async move {
            let _ = held.await;
            Ok::<_, BoxError>(())
        })
        .unwrap();
    let lease = ctx.lease("external").unwrap();

    let rejected = {
        let polled = polled.clone();
        ctx.spawn("over-capacity", async move {
            polled.store(true, SeqCst);
            Ok::<_, BoxError>(())
        })
    };
    assert_eq!(rejected.err(), Some(Rejected::Full));
    assert!(matches!(ctx.lease("also-over"), Err(Rejected::Full)));
    assert!(matches!(
        ctx.spawn_blocking("blocking-over", || Ok::<_, BoxError>(())),
        Err(Rejected::Full)
    ));

    let outstanding = app.snapshot().outstanding();
    let kinds: Vec<_> = outstanding
        .iter()
        .map(|item| (item.kind, item.name.name()))
        .collect();
    assert!(kinds.contains(&(Kind::Root, "root")));
    assert!(kinds.contains(&(Kind::Task, "held")));
    assert!(kinds.contains(&(Kind::Lease, "external")));
    assert_eq!(app.snapshot().outstanding_total(), 3);

    drop(lease);
    release.send(()).unwrap();
    bounded(first).await.unwrap();
    assert!(
        ctx.spawn("fits-again", async { Ok::<_, BoxError>(()) })
            .is_ok()
    );
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(!polled.load(SeqCst));
    assert!(matches!(
        ctx.spawn("after-drain", async { Ok::<_, BoxError>(()) }),
        Err(Rejected::Closed)
    ));
}

#[tokio::test]
async fn work_admitted_but_not_started_at_stop_is_cancelled_without_running() {
    let (app, ctx) = owner("owner".into()).await;
    let polled = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    // The request is recorded, but the driver has not delivered it to the
    // service yet: admission is still open. On this current-thread runtime
    // the driver runs before the work submitted after it.
    app.stop();
    let work = {
        let (polled, capture) = (polled.clone(), Tracked(drops.clone()));
        ctx.spawn("unstarted", async move {
            let _capture = capture;
            polled.store(true, SeqCst);
            Ok::<_, BoxError>(())
        })
        .unwrap()
    };
    let report = bounded(app.wait()).await.unwrap();
    assert!(matches!(bounded(work).await, Err(WorkError::Cancelled)));
    assert!(!polled.load(SeqCst));
    assert_eq!(drops.load(SeqCst), 1);
    assert_eq!(report.service("owner").unwrap().work.cancelled, 1);
    assert!(
        report.is_success(),
        "a cancellation before start is not a failure"
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_handle_or_cancelled_wait_leaves_the_work_owned() {
    let (app, ctx) = owner("owner".into()).await;
    let (release, held) = oneshot::channel::<()>();
    let finished = Arc::new(AtomicBool::new(false));
    let mut work = {
        let finished = finished.clone();
        ctx.spawn("owned", async move {
            let _ = held.await;
            finished.store(true, SeqCst);
            Ok::<_, BoxError>(7_u8)
        })
        .unwrap()
    };
    assert!(
        timeout(SOON, &mut work).await.is_err(),
        "the wait was cancelled"
    );
    drop(work);
    tokio::task::yield_now().await;

    app.stop();
    assert!(
        timeout(Duration::from_secs(1), app.wait()).await.is_err(),
        "stop is not completion"
    );
    assert_eq!(app.status().phase, Phase::Stopping);
    assert!(
        app.snapshot()
            .outstanding()
            .iter()
            .any(|item| item.name.name() == "owned")
    );

    release.send(()).unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(finished.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_and_result_wait_for_capture_destruction() {
    let (app, ctx) = owner("owner".into()).await;
    let (capture, release, dropped) = BlocksOnDrop::new();
    let started = Arc::new(AtomicBool::new(false));
    let mut work = ctx
        .spawn("capture", Keeps(Some(capture), Some(7_u8), started.clone()))
        .unwrap();
    // Stop only once the work has started; before that it would be cancelled.
    while !started.load(SeqCst) {
        tokio::task::yield_now().await;
    }
    app.stop();

    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while a capture was being dropped"
    );
    assert!(
        timeout(SOON, &mut work).await.is_err(),
        "result published before the capture was dropped"
    );
    assert!(!dropped.load(SeqCst));

    release.send(()).unwrap();
    assert_eq!(bounded(&mut work).await.unwrap(), 7);
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(dropped.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unclaimed_result_is_dropped_before_the_service_drains() {
    let (app, ctx) = owner("owner".into()).await;
    let (result, release, dropped) = BlocksOnDrop::new();
    let (entered, has_started) = oneshot::channel::<()>();
    let (go, wait) = oneshot::channel::<()>();
    drop(
        ctx.spawn("result", async move {
            let _ = entered.send(());
            let _ = wait.await;
            Ok::<_, BoxError>(result)
        })
        .unwrap(),
    );
    // The work must have started, or stop would cancel it and no result would exist.
    has_started.await.unwrap();
    go.send(()).unwrap();
    while app.snapshot().service("owner").unwrap().work.completed == 0 {
        tokio::task::yield_now().await;
    }
    app.stop();
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while an unclaimed result was alive"
    );
    assert!(!dropped.load(SeqCst));
    release.send(()).unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(dropped.load(SeqCst));
}

#[tokio::test]
async fn an_unread_handle_does_not_hold_its_works_count() {
    let (app, ctx) = owner("owner".into()).await;
    let drops = Arc::new(AtomicUsize::new(0));
    let unread = {
        let result = Tracked(drops.clone());
        ctx.spawn("unread", async move { Ok::<_, BoxError>(result) })
            .unwrap()
    };
    while app.snapshot().outstanding_total() != 1 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        drops.load(SeqCst),
        0,
        "the result belongs to the live handle"
    );
    // The handle is held outside any lease: it does not delay the report.
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert_eq!(drops.load(SeqCst), 0);
    drop(unread);
    assert_eq!(drops.load(SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panics_are_contained_attributed_and_never_block_release() {
    let (app, ctx) = owner("owner".into()).await;

    let panicked = ctx
        .spawn("panics", async {
            if std::hint::black_box(true) {
                panic!("boom");
            }
            Ok::<_, BoxError>(())
        })
        .unwrap();
    assert!(
        matches!(bounded(panicked).await, Err(WorkError::Panicked(message)) if &*message == "boom")
    );

    let kept = ctx
        .spawn(
            "capture-drop-panics",
            Keeps(Some(PanicsOnDrop), Some(3_u8), Arc::default()),
        )
        .unwrap();
    assert_eq!(
        bounded(kept).await.unwrap(),
        3,
        "a destructor panic must not replace the result"
    );

    let (go, wait) = oneshot::channel::<()>();
    drop(
        ctx.spawn("result-drop-panics", async move {
            let _ = wait.await;
            Ok::<_, BoxError>(PanicsOnDrop)
        })
        .unwrap(),
    );
    go.send(()).unwrap();

    let blocking = ctx
        .spawn_blocking("blocking-panics", || -> Result<(), BoxError> {
            panic!("blocking boom")
        })
        .unwrap();
    assert!(
        matches!(bounded(blocking).await, Err(WorkError::Panicked(message)) if &*message == "blocking boom")
    );

    let report = bounded(app.shutdown()).await.unwrap();
    let work = report.service("owner").unwrap().work;
    assert_eq!(work.panicked, 2);
    assert_eq!(work.release_panics, 2);
    assert!(
        report.is_success(),
        "contained panics in ordinary work do not fail the lifecycle"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_blocking_work_keeps_shutdown_pending_until_it_returns() {
    let (app, ctx) = owner("owner".into()).await;
    let (entered, started) = mpsc::channel();
    let (release, gate) = mpsc::channel::<()>();
    let work = ctx
        .spawn_blocking("flush", move || {
            entered.send(()).unwrap();
            let _ = gate.recv();
            Ok::<_, io::Error>("flushed")
        })
        .unwrap();
    started.recv().unwrap();

    app.stop();
    assert!(
        timeout(SOON, app.wait()).await.is_err(),
        "reported while a blocking call was running"
    );
    let outstanding = app.snapshot().outstanding();
    let flush = outstanding
        .iter()
        .find(|item| item.name.name() == "flush")
        .unwrap();
    assert_eq!((flush.kind, flush.started), (Kind::Blocking, true));
    assert_eq!(flush.to_string(), "owner: Blocking `flush` (started)");

    release.send(()).unwrap();
    assert_eq!(bounded(work).await.unwrap(), "flushed");
    assert!(bounded(app.wait()).await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn abort_after_grace_drops_async_work_and_marks_the_report_forced() {
    let (send, receive) = oneshot::channel();
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    app.service_fn("owner", move |ctx| async move {
        let _ = send.send(ctx.clone());
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let (started, has_started) = oneshot::channel();
    let stuck = {
        let capture = Tracked(drops.clone());
        ctx.spawn("stuck", async move {
            let _capture = capture;
            let _ = started.send(());
            std::future::pending::<Result<(), BoxError>>().await
        })
        .unwrap()
    };
    has_started.await.unwrap();

    app.stop();
    assert!(
        timeout(Duration::from_secs(2), app.wait()).await.is_err(),
        "grace has not expired yet"
    );
    let report = bounded(app.wait()).await.unwrap();
    assert!(matches!(bounded(stuck).await, Err(WorkError::Aborted)));
    assert_eq!(drops.load(SeqCst), 1);
    let service = report.service("owner").unwrap();
    assert!(service.forced);
    assert_eq!(service.work.aborted, 1);
    assert!(
        !report.is_success(),
        "a forced abort is never a successful completion"
    );
}

#[tokio::test(start_paused = true)]
async fn a_lease_cannot_be_forced_and_an_unconfirmed_one_never_releases() {
    let (app, ctx) = owner("owner".into()).await;
    let lease = ctx.lease("upgraded-socket").unwrap();
    let doubtful = ctx.lease("external-executor").unwrap();
    app.stop();
    // Far beyond the grace period: nothing can abort a lease.
    assert!(timeout(Duration::from_secs(120), app.wait()).await.is_err());
    assert_eq!(app.status().phase, Phase::Stopping);

    doubtful.mark_unconfirmed("serve returned an error; its tasks may still run");
    drop(lease);
    assert!(
        timeout(Duration::from_secs(120), app.wait()).await.is_err(),
        "an unconfirmed lease was treated as released"
    );
    let outstanding = app.snapshot().outstanding();
    assert_eq!(outstanding.len(), 1);
    assert_eq!(
        outstanding[0].to_string(),
        "owner: Lease `external-executor` (unconfirmed: serve returned an error; its tasks may still run)"
    );
}

#[tokio::test]
async fn one_handle_can_abort_its_own_work() {
    let (app, ctx) = owner("owner".into()).await;
    let (started, has_started) = oneshot::channel();
    let work = ctx
        .spawn("abortable", async move {
            let _ = started.send(());
            std::future::pending::<Result<(), BoxError>>().await
        })
        .unwrap();
    has_started.await.unwrap();
    work.abort();
    assert!(matches!(bounded(work).await, Err(WorkError::Aborted)));
    let report = bounded(app.shutdown()).await.unwrap();
    assert!(!report.service("owner").unwrap().forced);
    assert!(
        report.is_success(),
        "an abort the owner asked for is not a forced shutdown"
    );
}

#[tokio::test]
async fn critical_work_fails_its_service_and_stops_the_application() {
    let (send, receive) = oneshot::channel();
    let mut app = App::new();
    app.service_fn("owner", move |ctx| async move {
        let _ = send.send(ctx.clone());
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    drop(
        ctx.spawn(critical("calibration"), async {
            Err::<(), _>(io::Error::other("sensor drift"))
        })
        .unwrap(),
    );
    // No stop request: the failure of critical work ends the application.
    let report = bounded(app.wait()).await.unwrap();
    assert!(!report.is_success());
    let failures = &report.service("owner").unwrap().failures;
    assert!(
        matches!(failures.as_slice(), [Failure::Work(fault)] if fault.label.name() == "calibration"),
        "{failures:?}"
    );
}

// I1 against threads, through the whole path. The exact interleavings are
// covered by the loom model of the gate in the design's prototype A.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_accepted_item_runs_and_no_rejected_future_is_polled() {
    for _ in 0..50 {
        let (app, ctx) =
            owner(Spec::new("owner").capacity(NonZeroUsize::new(1 << 16).unwrap())).await;
        let polled = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(5));
        let submitters: Vec<_> = (0..4)
            .map(|_| {
                let (ctx, polled, start) = (ctx.clone(), polled.clone(), start.clone());
                std::thread::spawn(move || {
                    start.wait();
                    let mut accepted = 0_usize;
                    for _ in 0..50 {
                        let polled = polled.clone();
                        match ctx.spawn("item", async move {
                            polled.fetch_add(1, SeqCst);
                            Ok::<_, BoxError>(())
                        }) {
                            Ok(_) => accepted += 1,
                            Err(Rejected::Closed) => break,
                            Err(Rejected::Full) => unreachable!("capacity is not reached"),
                        }
                    }
                    accepted
                })
            })
            .collect();
        let stopper = {
            let (app, start) = (app.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                app.stop();
            })
        };
        let accepted: usize = submitters
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum();
        stopper.join().unwrap();

        let report = bounded(app.wait()).await.unwrap();
        let work = report.service("owner").unwrap().work;
        assert_eq!(
            work.completed + work.cancelled,
            accepted as u64,
            "accepted work was lost"
        );
        assert_eq!(
            polled.load(SeqCst) as u64,
            work.completed,
            "a rejected or cancelled future ran"
        );
        assert_eq!(report.service("owner").unwrap().outstanding_total, 0);
    }
}

#[test]
fn work_can_be_submitted_from_a_thread_outside_the_runtime() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    let (app, ctx) = runtime.block_on(owner("owner".into()));
    let value = std::thread::spawn(move || ctx.spawn("outside", async { Ok::<_, BoxError>(5_u8) }))
        .join()
        .unwrap()
        .unwrap();
    assert_eq!(runtime.block_on(bounded(value)).unwrap(), 5);
    assert!(
        runtime
            .block_on(bounded(app.shutdown()))
            .unwrap()
            .is_success()
    );
}

#[test]
fn runtime_loss_releases_work_and_is_never_a_report() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    let (app, ctx) = runtime.block_on(owner("owner".into()));
    let drops = Arc::new(AtomicUsize::new(0));
    let (started, has_started) = mpsc::channel();
    let work = {
        let capture = Tracked(drops.clone());
        ctx.spawn("orphaned", async move {
            let _capture = capture;
            started.send(()).unwrap();
            std::future::pending::<Result<(), BoxError>>().await
        })
        .unwrap()
    };
    has_started.recv().unwrap();
    drop(runtime);

    assert_eq!(
        drops.load(SeqCst),
        1,
        "the capture was not released with the runtime"
    );
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(matches!(observer.block_on(work), Err(WorkError::Aborted)));
    let lost = observer
        .block_on(app.wait())
        .expect_err("a lost driver must not produce a report");
    assert_eq!(lost.snapshot.status.phase, Phase::Lost);
    assert_eq!(app.status().phase, Phase::Lost);
}
