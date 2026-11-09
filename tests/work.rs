//! Caller-facing work admission, release and cancellation contracts.

use miju_services::{
    App, AppHandle, BoxError,
    app::AppOptions,
    observe::ShutdownProgress,
    work::{SpawnError, WorkCompletion, WorkOutcome, WorkScope, WorkSpec},
};
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::oneshot;

async fn running(capacity: usize) -> (AppHandle, WorkScope) {
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_millis(30),
            abort_wait: Duration::from_millis(10),
            ..AppOptions::default()
        })
        .service_fn(
            miju_services::service::ServiceSpec::new("owner")
                .max_work(NonZeroUsize::new(capacity).unwrap()),
            move |context| async move {
                send.send(context.work().clone()).unwrap();
                context.ready();
                context.stopping().await;
                Ok(())
            },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    (app, receive.await.unwrap())
}

#[tokio::test]
async fn rejects_before_factory_and_counts_queued_work() {
    let (app, scope) = running(1).await;
    let first = scope
        .spawn(WorkSpec::new("first"), |context| async move {
            context.stopping().await;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    let invoked = Arc::new(AtomicBool::new(false));
    let called = invoked.clone();
    assert!(matches!(
        scope.spawn(WorkSpec::new("full"), move |_| async move {
            called.store(true, Ordering::SeqCst);
            Ok::<(), BoxError>(())
        }),
        Err(SpawnError::AtCapacity)
    ));
    assert!(!invoked.load(Ordering::SeqCst));
    first.stop();
    first.wait().await;
    scope.close();
    assert!(matches!(
        scope.spawn(WorkSpec::new("closed"), |_| async {
            Ok::<(), BoxError>(())
        }),
        Err(SpawnError::Closed)
    ));
    scope.wait().await;
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test]
async fn stopped_before_first_poll_does_not_invoke_factory() {
    let (app, scope) = running(1).await;
    let invoked = Arc::new(AtomicBool::new(false));
    let called = invoked.clone();
    let work = scope
        .spawn(WorkSpec::aggregate("not-started"), move |_| {
            called.store(true, Ordering::SeqCst);
            async { Ok::<(), BoxError>(()) }
        })
        .unwrap();
    work.stop();
    let report = work.wait().await;
    assert!(matches!(
        report.outcome(),
        WorkOutcome::CancelledBeforeStart
    ));
    assert_eq!(report.completion(), WorkCompletion::Confirmed);
    assert!(!invoked.load(Ordering::SeqCst));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_wait_keeps_accepted_work_owned() {
    let (app, scope) = running(2).await;
    let (finish, done) = oneshot::channel();
    let work = scope
        .spawn(WorkSpec::new("owned"), |_| async move {
            done.await?;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), work.wait())
            .await
            .is_err()
    );
    drop(work);
    assert_eq!(scope.snapshot().active.len(), 1);
    finish.send(()).unwrap();
    scope.close();
    scope.wait().await;
    assert!(scope.snapshot().active.is_empty());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn scope_uses_captured_runtime_from_an_external_thread() {
    let (app, scope) = running(1).await;
    let work = std::thread::spawn(move || {
        scope
            .spawn(WorkSpec::new("external-thread"), |_| async {
                Ok::<(), BoxError>(())
            })
            .unwrap()
    })
    .join()
    .unwrap();
    assert!(matches!(
        work.wait().await.outcome(),
        WorkOutcome::Succeeded
    ));
    app.shutdown().await.unwrap();
}

struct Released(Arc<AtomicBool>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn runtime_loss_releases_started_and_unpolled_work_without_a_final_report() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (app, scope) = runtime.block_on(running(3));
    let released: Vec<_> = (0..3).map(|_| Arc::new(AtomicBool::new(false))).collect();
    let capture = Released(released[0].clone());
    let started = runtime.block_on(async {
        let (send, receive) = oneshot::channel();
        let work = scope
            .spawn(WorkSpec::new("started"), move |_| async move {
                let _capture = capture;
                send.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok::<(), BoxError>(())
            })
            .unwrap();
        receive.await.unwrap();
        work
    });
    let called = Arc::new(AtomicBool::new(false));
    let mut unpolled = Vec::new();
    for (index, spec) in [
        WorkSpec::new("unpolled"),
        WorkSpec::aggregate("unpolled-aggregate"),
    ]
    .into_iter()
    .enumerate()
    {
        let called = called.clone();
        let capture = Released(released[index + 1].clone());
        unpolled.push(
            scope
                .spawn(spec, move |_| {
                    called.store(true, Ordering::SeqCst);
                    let _capture = capture;
                    async { Ok::<(), BoxError>(()) }
                })
                .unwrap(),
        );
    }
    drop(runtime);
    assert!(!called.load(Ordering::SeqCst));
    assert!(released.iter().all(|flag| flag.load(Ordering::SeqCst)));
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    observer.block_on(async {
        let report = started.wait().await;
        assert!(matches!(report.outcome(), WorkOutcome::Aborted));
        assert_eq!(report.completion(), WorkCompletion::Confirmed);
        for handle in unpolled {
            let report = handle.wait().await;
            assert!(matches!(
                report.outcome(),
                WorkOutcome::CancelledBeforeStart
            ));
            assert_eq!(report.completion(), WorkCompletion::Confirmed);
        }
        tokio::time::timeout(Duration::from_secs(1), scope.wait())
            .await
            .expect("runtime loss left a tracker token or entry behind");
        assert!(app.wait().await.unwrap_err().snapshot.driver_lost);
    });
}

#[tokio::test]
async fn publication_follows_capture_release() {
    let (app, scope) = running(1).await;
    let released = Arc::new(AtomicBool::new(false));
    let capture = Released(released.clone());
    let work = scope
        .spawn(WorkSpec::new("release"), |_| async move {
            let _capture = capture;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    work.wait().await;
    assert!(released.load(Ordering::SeqCst));
    assert!(scope.snapshot().active.is_empty());
    app.shutdown().await.unwrap();
}

struct FailedFuture;
impl Future for FailedFuture {
    type Output = Result<(), std::io::Error>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(std::io::Error::other("original execution error")))
    }
}
impl Drop for FailedFuture {
    fn drop(&mut self) {
        panic!("release failed");
    }
}

#[tokio::test]
async fn retains_execution_error_and_destructor_panic() {
    let (app, scope) = running(1).await;
    let work = scope
        .spawn(WorkSpec::new("two-errors"), |_| FailedFuture)
        .unwrap();
    let report = work.wait().await;
    match report.outcome() {
        WorkOutcome::Failed(error) => assert_eq!(error.to_string(), "original execution error"),
        other => panic!("lost original error: {other:?}"),
    }
    assert_eq!(
        report.release_panic().map(AsRef::as_ref),
        Some("release failed")
    );
    assert_eq!(report.completion(), WorkCompletion::Confirmed);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn records_factory_and_poll_panics() {
    let (app, scope) = running(2).await;
    let constructor = scope
        .spawn(
            WorkSpec::new("constructor"),
            |_| -> std::future::Ready<Result<(), BoxError>> { panic!("constructor panic") },
        )
        .unwrap();
    let poll = scope
        .spawn(WorkSpec::new("poll"), |_| async {
            panic!("poll panic");
            #[allow(unreachable_code)]
            Ok::<(), BoxError>(())
        })
        .unwrap();
    assert!(
        matches!(constructor.wait().await.outcome(), WorkOutcome::Panicked(message) if message.as_ref() == "constructor panic")
    );
    assert!(
        matches!(poll.wait().await.outcome(), WorkOutcome::Panicked(message) if message.as_ref() == "poll panic")
    );
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_blocking_keeps_shutdown_pending_until_actual_release() {
    let (app, scope) = running(1).await;
    let (started, began) = oneshot::channel();
    let (finish, done) = std::sync::mpsc::channel();
    let released = Arc::new(AtomicBool::new(false));
    let capture = Released(released.clone());
    let work = scope
        .spawn_blocking(WorkSpec::new("blocking"), move |_| {
            let _capture = capture;
            started.send(()).unwrap();
            done.recv().unwrap();
            Ok::<(), BoxError>(())
        })
        .unwrap();
    began.await.unwrap();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(80))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(!released.load(Ordering::SeqCst));
    finish.send(()).unwrap();
    assert!(matches!(
        work.wait().await.outcome(),
        WorkOutcome::Succeeded
    ));
    assert!(released.load(Ordering::SeqCst));
    assert!(app.wait().await.unwrap().is_success());
}

#[tokio::test]
async fn aggregate_error_retains_capacity_and_completion_barrier() {
    let (app, scope) = running(1).await;
    let work = scope
        .spawn(WorkSpec::aggregate("external"), |_| async {
            Err::<(), BoxError>(std::io::Error::other("external completion unknown").into())
        })
        .unwrap();
    let report = work.wait().await;
    assert_eq!(report.completion(), WorkCompletion::Unconfirmed);
    assert_eq!(scope.snapshot().unconfirmed, 1);
    assert_eq!(scope.snapshot().active.len(), 1);
    assert!(matches!(
        scope.spawn(WorkSpec::new("overlap"), |_| async {
            Ok::<(), BoxError>(())
        }),
        Err(SpawnError::AtCapacity | SpawnError::Closed)
    ));
    let progress = app
        .shutdown_within(Duration::from_millis(80))
        .await
        .unwrap();
    assert!(matches!(progress, ShutdownProgress::Pending(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(5), scope.wait())
            .await
            .is_err()
    );
    // This intentionally unresolved instance ends only when the test host drops its runtime.
}

#[tokio::test]
async fn aggregate_cannot_be_blocking_or_forced_after_start() {
    let (app, scope) = running(1).await;
    assert!(matches!(
        scope.spawn_blocking(WorkSpec::aggregate("invalid"), |_| Ok::<(), BoxError>(())),
        Err(SpawnError::IncompatibleSpec)
    ));
    let (started, began) = oneshot::channel();
    let (finish, done) = oneshot::channel();
    let work = scope
        .spawn(WorkSpec::aggregate("external"), move |_| async move {
            started.send(()).unwrap();
            done.await?;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    began.await.unwrap();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(80))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(
        finish.send(()).is_ok(),
        "aggregate was forcibly dropped before external completion"
    );
    let report = work.wait().await;
    assert!(matches!(report.outcome(), WorkOutcome::Succeeded));
    assert!(!report.was_aborted());
    assert_eq!(report.completion(), WorkCompletion::Confirmed);
    app.wait().await.unwrap();
}

#[tokio::test]
async fn ordinary_abort_releases_before_completion() {
    let (app, scope) = running(1).await;
    let (started, began) = oneshot::channel();
    let released = Arc::new(AtomicBool::new(false));
    let capture = Released(released.clone());
    let work = scope
        .spawn(WorkSpec::new("ordinary"), move |_| async move {
            let _capture = capture;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    began.await.unwrap();
    let report = app.shutdown().await.unwrap_err().report().unwrap().clone();
    assert!(!report.is_success());
    assert!(released.load(Ordering::SeqCst));
    let work = work.wait().await;
    assert!(matches!(work.outcome(), WorkOutcome::Aborted));
    assert!(work.was_aborted());
}

struct Reenter(WorkScope, Arc<AtomicBool>);
impl Drop for Reenter {
    fn drop(&mut self) {
        self.0.close();
        let _ = self.0.snapshot();
        self.1.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn rejected_and_cancelled_captures_may_reenter_scope() {
    let (app, scope) = running(1).await;
    let rejected = Arc::new(AtomicBool::new(false));
    scope.close();
    let capture = Reenter(scope.clone(), rejected.clone());
    let result = scope.spawn(WorkSpec::new("rejected"), move |_| async move {
        let _capture = capture;
        Ok::<(), BoxError>(())
    });
    assert!(matches!(result, Err(SpawnError::Closed)));
    assert!(rejected.load(Ordering::SeqCst));
    app.shutdown().await.unwrap();

    let (app, scope) = running(1).await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let capture = Reenter(scope.clone(), cancelled.clone());
    let work = scope
        .spawn(WorkSpec::new("cancelled"), move |_| async move {
            let _capture = capture;
            Ok::<(), BoxError>(())
        })
        .unwrap();
    work.stop();
    work.wait().await;
    assert!(cancelled.load(Ordering::SeqCst));
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_race_keeps_every_accepted_item_owned() {
    for _ in 0..16 {
        let (app, scope) = running(32).await;
        let start = Arc::new(std::sync::Barrier::new(3));
        let spawn_scope = scope.clone();
        let spawn_start = start.clone();
        let invoked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called = invoked.clone();
        let submitter = std::thread::spawn(move || {
            spawn_start.wait();
            (0..32)
                .filter_map(|index| {
                    let called = called.clone();
                    match spawn_scope.spawn(WorkSpec::new(index.to_string()), move |_| async move {
                        called.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok::<(), BoxError>(())
                    }) {
                        Ok(handle) => Some(handle),
                        Err(SpawnError::Closed) => None,
                        Err(other) => panic!("unexpected admission error: {other}"),
                    }
                })
                .collect::<Vec<_>>()
        });
        let close_scope = scope.clone();
        let close_start = start.clone();
        let closer = std::thread::spawn(move || {
            close_start.wait();
            close_scope.close();
        });
        // Only native threads wait on the barrier; do not block a Tokio worker.
        let start_main = start.clone();
        tokio::task::spawn_blocking(move || start_main.wait())
            .await
            .unwrap();
        closer.join().unwrap();
        let accepted = submitter.join().unwrap();
        for handle in &accepted {
            assert!(matches!(
                handle.wait().await.outcome(),
                WorkOutcome::Succeeded
            ));
        }
        scope.wait().await;
        assert_eq!(invoked.load(Ordering::SeqCst), accepted.len());
        assert!(scope.snapshot().active.is_empty());
        app.shutdown().await.unwrap();
    }
}

struct ReleaseBarrier {
    started: Option<oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}
impl Future for ReleaseBarrier {
    type Output = Result<(), BoxError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}
impl Drop for ReleaseBarrier {
    fn drop(&mut self) {
        self.started.take().unwrap().send(()).unwrap();
        self.release.recv().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_waits_for_in_progress_capture_destruction() {
    let (app, scope) = running(1).await;
    let (started, began) = oneshot::channel();
    let (release, finish) = std::sync::mpsc::channel();
    let work = scope
        .spawn(WorkSpec::new("release-barrier"), move |_| ReleaseBarrier {
            started: Some(started),
            release: finish,
        })
        .unwrap();
    began.await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(20), work.wait()).await;
    release.send(()).unwrap();
    assert!(
        early.is_err(),
        "work completion preceded actual capture destruction"
    );
    work.wait().await;
    app.shutdown().await.unwrap();
}
