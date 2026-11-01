#![forbid(unsafe_code)]
use super::*;
use std::{
    future::pending,
    sync::atomic::{AtomicBool, AtomicUsize},
    time::Duration,
};

fn scope(max: usize) -> WorkScope {
    WorkScope::new(
        ServiceId(Arc::from("test")),
        max,
        2,
        Handle::current(),
        Arc::new(Notify::new()),
        Arc::new(AtomicU64::new(1)),
    )
}

struct Released(Arc<AtomicBool>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn success_error_panic_and_abort_release_before_report() {
    for kind in 0..4 {
        let scope = scope(1);
        let released = Arc::new(AtomicBool::new(false));
        let guard = Released(released.clone());
        let (started, start) = tokio::sync::oneshot::channel();
        let work = scope
            .spawn(WorkSpec::new("drop-order"), move |_| async move {
                let _guard = guard;
                let _ = started.send(());
                match kind {
                    0 => Ok(()),
                    1 => Err(std::io::Error::other("business")),
                    2 => panic!("business panic"),
                    _ => pending().await,
                }
            })
            .unwrap();
        start.await.unwrap();
        if kind == 3 {
            scope.abort();
        }
        let report = work.wait().await;
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(report.completion(), WorkCompletion::Confirmed);
        match (kind, report.outcome()) {
            (0, WorkOutcome::Succeeded)
            | (1, WorkOutcome::Failed(_))
            | (2, WorkOutcome::Panicked(_))
            | (3, WorkOutcome::Aborted) => {}
            other => panic!("wrong report: {other:?}"),
        }
        scope.close();
        scope.wait().await;
        assert!(scope.is_drained());
        assert!(!scope.has_service_failure());
    }
}

#[tokio::test]
async fn unpolled_cancellation_drops_factory_without_invoking_it() {
    let scope = scope(1);
    let released = Arc::new(AtomicBool::new(false));
    let invoked = Arc::new(AtomicBool::new(false));
    let guard = Released(released.clone());
    let called = invoked.clone();
    let work = scope
        .spawn(WorkSpec::aggregate("not-started"), move |_| {
            called.store(true, Ordering::SeqCst);
            async move {
                drop(guard);
                Ok::<(), BoxError>(())
            }
        })
        .unwrap();
    scope.stop();
    scope.abort();
    let report = work.wait().await;
    assert!(released.load(Ordering::SeqCst));
    assert!(!invoked.load(Ordering::SeqCst));
    assert!(matches!(
        report.outcome(),
        WorkOutcome::CancelledBeforeStart
    ));
    assert_eq!(report.completion(), WorkCompletion::Confirmed);
    scope.wait().await;
}

#[tokio::test]
async fn aggregate_failure_keeps_identity_capacity_and_completion_debt() {
    let scope = scope(1);
    let work = scope
        .spawn(WorkSpec::aggregate("framework"), |_| async {
            Err::<(), _>(std::io::Error::other("external completion unknown"))
        })
        .unwrap();
    let report = work.wait().await;
    assert_eq!(report.completion(), WorkCompletion::Unconfirmed);
    assert!(scope.has_service_failure());
    assert_eq!(scope.snapshot().unconfirmed, vec![work.id()]);
    assert_eq!(scope.snapshot().failed, 1);
    assert_eq!(scope.snapshot().completed, 0);
    assert!(matches!(
        scope.spawn(WorkSpec::new("late"), |_| async { Ok::<(), BoxError>(()) }),
        Err(SpawnError::AtCapacity)
    ));
    scope.stop();
    scope.abort();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), scope.wait())
            .await
            .is_err()
    );
    assert_eq!(scope.snapshot().failed, 1);
}

#[tokio::test]
async fn started_aggregate_survives_abort_until_completion() {
    let scope = scope(1);
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, barrier) = tokio::sync::oneshot::channel();
    let work = scope
        .spawn(
            WorkSpec::aggregate("framework"),
            move |context| async move {
                let _ = started.send(());
                context.stopping().await;
                barrier.await.unwrap();
                Ok::<(), BoxError>(())
            },
        )
        .unwrap();
    start.await.unwrap();
    scope.stop();
    scope.abort();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), work.wait())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert!(matches!(
        work.wait().await.outcome(),
        WorkOutcome::Succeeded
    ));
    scope.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_blocking_retains_slot_after_abort() {
    let scope = scope(1);
    let released = Arc::new(AtomicBool::new(false));
    let guard = Released(released.clone());
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, barrier) = std::sync::mpsc::channel();
    let work = scope
        .spawn_blocking(WorkSpec::new("blocking"), move |_| {
            let _guard = guard;
            let _ = started.send(());
            barrier.recv().unwrap();
            Ok::<(), BoxError>(())
        })
        .unwrap();
    start.await.unwrap();
    scope.stop();
    scope.abort();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), scope.wait())
            .await
            .is_err()
    );
    assert_eq!(scope.snapshot().active, vec![work.id()]);
    assert!(!released.load(Ordering::SeqCst));
    release.send(()).unwrap();
    let report = work.wait().await;
    assert!(released.load(Ordering::SeqCst));
    assert!(matches!(report.outcome(), WorkOutcome::Succeeded));
    scope.wait().await;
}

#[tokio::test]
async fn close_only_closes_admission_and_handle_drop_does_not_cancel() {
    let scope = scope(1);
    let (release, barrier) = tokio::sync::oneshot::channel();
    let work = scope
        .spawn(WorkSpec::new("held"), move |context| async move {
            barrier.await.unwrap();
            assert!(!context.is_stopping());
            Ok::<(), BoxError>(())
        })
        .unwrap();
    drop(work);
    scope.close();
    assert!(!scope.is_stopping());
    assert!(matches!(
        scope.spawn(WorkSpec::new("late"), |_| async { Ok::<(), BoxError>(()) }),
        Err(SpawnError::Closed)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(5), scope.wait())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    scope.wait().await;
    assert_eq!(scope.snapshot().completed, 1);
}

#[tokio::test]
async fn rejection_and_user_drop_can_reenter_scope() {
    struct Reenter(WorkScope, Arc<AtomicUsize>);

    impl Drop for Reenter {
        fn drop(&mut self) {
            let _ = self.0.snapshot();
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }

    let scope = scope(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = Reenter(scope.clone(), dropped.clone());
    let work = scope
        .spawn(WorkSpec::new("reentrant"), move |_| async move {
            drop(guard);
            Ok::<(), BoxError>(())
        })
        .unwrap();
    work.wait().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    scope.close();
    let guard = Reenter(scope.clone(), dropped.clone());
    assert!(matches!(
        scope.spawn(WorkSpec::new("rejected"), move |_| async move {
            drop(guard);
            Ok::<(), BoxError>(())
        }),
        Err(SpawnError::Closed)
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_runtime_thread_uses_saved_runtime_and_ids_span_scopes() {
    let ids = Arc::new(AtomicU64::new(1));
    let make = || {
        WorkScope::new(
            ServiceId(Arc::from("test")),
            1,
            0,
            Handle::current(),
            Arc::new(Notify::new()),
            ids.clone(),
        )
    };
    let first = make();
    let second = make();
    let outside = first.clone();
    let a = std::thread::spawn(move || {
        outside
            .spawn(WorkSpec::new("outside"), |_| async {
                Ok::<(), BoxError>(())
            })
            .unwrap()
    })
    .join()
    .unwrap();
    let b = second
        .spawn(WorkSpec::new("second"), |_| async {
            Ok::<(), BoxError>(())
        })
        .unwrap();
    assert_ne!(a.id(), b.id());
    a.wait().await;
    b.wait().await;
    first.close();
    second.close();
    first.wait().await;
    second.wait().await;
}

#[tokio::test]
async fn evicted_error_drop_reenters_without_a_gate_or_watch_lock() {
    #[derive(Debug)]
    struct ReentrantError(WorkScope, Arc<AtomicBool>);

    impl fmt::Display for ReentrantError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("reentrant")
        }
    }

    impl std::error::Error for ReentrantError {}

    impl Drop for ReentrantError {
        fn drop(&mut self) {
            let _ = self.0.snapshot();
            self.1.store(true, Ordering::SeqCst);
        }
    }

    let scope = scope(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let error = ReentrantError(scope.clone(), dropped.clone());
    let handle = scope
        .spawn(WorkSpec::new("error"), move |_| async {
            Err::<(), _>(error)
        })
        .unwrap();
    handle.wait().await;
    drop(handle);
    for _ in 0..2 {
        scope
            .spawn(WorkSpec::new("evict"), |_| async { Ok::<(), BoxError>(()) })
            .unwrap()
            .wait()
            .await;
    }
    assert!(dropped.load(Ordering::SeqCst));
    scope.close();
    scope.wait().await;
}

#[tokio::test]
async fn abort_with_panicking_destructor_retains_both_failure_and_forced_abort() {
    struct PanicDrop;

    impl Drop for PanicDrop {
        fn drop(&mut self) {
            panic!("destructor panic");
        }
    }

    let scope = scope(1);
    let (send, receive) = tokio::sync::oneshot::channel();
    let work = scope
        .spawn(WorkSpec::new("abort-drop"), move |_| async move {
            let _guard = PanicDrop;
            send.send(()).unwrap();
            pending::<Result<(), BoxError>>().await
        })
        .unwrap();
    receive.await.unwrap();
    scope.abort();
    let report = work.wait().await;
    assert!(matches!(report.outcome(), WorkOutcome::Panicked(_)));
    assert!(report.was_aborted());
    assert_eq!(scope.snapshot().aborted, 1);
    assert_eq!(scope.snapshot().failed, 1);
    scope.close();
    scope.wait().await;
}

#[test]
fn shutdown_runtime_drops_unstarted_entry_and_reports_without_reentrant_deadlock() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let scope = WorkScope::new(
        ServiceId(Arc::from("dead-runtime")),
        1,
        1,
        runtime.handle().clone(),
        Arc::new(Notify::new()),
        Arc::new(AtomicU64::new(1)),
    );
    drop(runtime);

    struct Reenter(WorkScope, Arc<AtomicBool>);

    impl Drop for Reenter {
        fn drop(&mut self) {
            let _ = self.0.snapshot();
            self.1.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Reenter(scope.clone(), dropped.clone());
    let work = scope
        .spawn(WorkSpec::new("never-run"), move |_| async move {
            drop(guard);
            panic!("closed runtime must not invoke user entry");
            #[allow(unreachable_code)]
            Ok::<(), BoxError>(())
        })
        .unwrap();
    scope.close();
    let observer_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    observer_runtime.block_on(async {
        assert!(matches!(
            work.wait().await.outcome(),
            WorkOutcome::CancelledBeforeStart
        ));
        scope.wait().await;
    });
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn factory_panic_is_observed_after_factory_captures_are_released() {
    let scope = scope(1);
    let released = Arc::new(AtomicBool::new(false));
    let guard = Released(released.clone());
    let work = scope
        .spawn(WorkSpec::new("factory").fail_service(), move |_| {
            let _guard = guard;
            panic!("factory failed before producing a future");
            #[allow(unreachable_code)]
            async {
                Ok::<(), BoxError>(())
            }
        })
        .unwrap();
    let report = work.wait().await;
    assert!(matches!(report.outcome(), WorkOutcome::Panicked(_)));
    assert!(released.load(Ordering::SeqCst));
    assert!(scope.has_service_failure());
    scope.close();
    scope.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_admission_and_close_never_lose_accepted_work() {
    for _ in 0..64 {
        let scope = scope(1);
        let released = Arc::new(AtomicBool::new(false));
        let guard = Released(released.clone());
        let race = Arc::new(std::sync::Barrier::new(2));
        let other = scope.clone();
        let other_race = race.clone();
        let thread = std::thread::spawn(move || {
            other_race.wait();
            other.spawn(WorkSpec::new("race"), move |_| async move {
                drop(guard);
                Ok::<(), BoxError>(())
            })
        });
        race.wait();
        scope.close();
        match thread.join().unwrap() {
            Ok(handle) => {
                handle.wait().await;
            }
            Err(SpawnError::Closed) => {}
            other => panic!("unexpected admission: {other:?}"),
        }
        scope.wait().await;
        assert!(released.load(Ordering::SeqCst));
        assert!(scope.snapshot().active.is_empty());
    }
}
