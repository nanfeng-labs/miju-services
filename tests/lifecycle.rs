//! Caller-visible initialization, failure, ownership, and cleanup contracts.

use miju_services::{
    App, BoxError, RunError, ServiceContext,
    app::AppOptions,
    observe::{ReadyError, ShutdownProgress},
    report::{Health, Phase, ServiceFailure, ServicePhase},
    service::{Criticality, Lifetime, ServiceSpec},
    work::WorkSpec,
};
use std::{
    future::pending,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

fn finite(name: &str) -> ServiceSpec {
    ServiceSpec::new(name).lifetime(Lifetime::Finite)
}
fn short_options() -> AppOptions {
    AppOptions {
        startup_timeout: Duration::from_secs(3),
        shutdown_grace: Duration::from_millis(40),
        abort_wait: Duration::from_millis(10),
        ..AppOptions::default()
    }
}
async fn resident(context: ServiceContext) -> Result<(), BoxError> {
    context.ready();
    context.stopping().await;
    Ok(())
}

#[tokio::test]
async fn stop_wakes_driver_even_when_scope_waiter_is_older() {
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            context.ready();
            send.send(context.clone()).unwrap();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    let context = receive.await.unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let scope = context.work().clone();
    let waiter = tokio::spawn(async move {
        scope.wait().await;
    });
    tokio::task::yield_now().await;
    let mut updates = app.observer().subscribe();
    context.set_health(Health::Healthy);
    updates.changed().await.unwrap();
    let result = tokio::time::timeout(Duration::from_millis(150), app.shutdown()).await;
    assert!(
        result.is_ok(),
        "stop notification was consumed by scope.wait instead of lifecycle driver"
    );
    assert!(result.unwrap().unwrap().is_success());
    waiter.await.unwrap();
}

#[tokio::test]
async fn readiness_wakes_driver_even_when_scope_waiter_is_older() {
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            send.send(context.clone()).unwrap();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    let context = receive.await.unwrap();
    let scope = context.work().clone();
    let waiter = tokio::spawn(async move {
        scope.wait().await;
    });
    tokio::task::yield_now().await;
    let mut updates = app.observer().subscribe();
    context.set_health(Health::Healthy);
    updates.changed().await.unwrap();
    context.ready();
    let result = app.ready(Duration::from_millis(150)).await;
    assert!(
        result.is_ok(),
        "readiness notification was consumed by scope.wait instead of lifecycle driver"
    );
    app.shutdown().await.unwrap();
    waiter.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn live_global_and_optional_budget_flags_keep_pending_responsibility() {
    let release = Arc::new(Notify::new());
    let complete = release.clone();
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .options(short_options())
        .service_fn("required", resident)
        .service_fn(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            move |context| async move {
                let (started, running) = oneshot::channel();
                context
                    .work()
                    .spawn(WorkSpec::aggregate("external"), move |_| async move {
                        started.send(()).unwrap();
                        complete.notified().await;
                        Ok::<(), BoxError>(())
                    })?;
                running.await.unwrap();
                send.send(()).unwrap();
                Err(io::Error::other("optional-fault").into())
            },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.await.unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_millis(60)).await;
    let mut updates = app.observer().subscribe();
    while !updates.current().services[1].stop_budget_exceeded {
        updates.changed().await.unwrap();
    }
    assert!(app.snapshot().ready);
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(100))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(
        app.snapshot().budget_exceeded,
        "live global budget flag was not published"
    );
    assert!(app.snapshot().services[1].stop_budget_exceeded);
    release.notify_one();
    let report = app.shutdown().await.unwrap();
    assert!(report.is_success());
    assert!(report.snapshot.budget_exceeded);
}

#[tokio::test]
async fn readiness_is_health_sensitive_and_default_health_is_healthy() {
    for explicit in [
        None,
        Some(Health::Unknown),
        Some(Health::Degraded),
        Some(Health::Unhealthy),
    ] {
        let (send, receive) = oneshot::channel();
        let app = App::builder()
            .service_fn("service", move |context| async move {
                if let Some(health) = explicit {
                    context.set_health(health);
                }
                context.ready();
                context.ready();
                send.send(context.clone()).unwrap();
                context.stopping().await;
                // Late notifications cannot revive readiness or change saved health.
                context.set_health(Health::Unhealthy);
                context.ready();
                Ok(())
            })
            .build()
            .unwrap()
            .start()
            .unwrap();
        let context = receive.await.unwrap();
        if let Some(explicit) = explicit {
            assert!(matches!(
                app.ready(Duration::from_millis(5)).await,
                Err(ReadyError::Timeout(_))
            ));
            assert_eq!(app.snapshot().phase, Phase::Running);
            assert_eq!(app.snapshot().services[0].health, explicit);
        } else {
            assert!(app.ready(Duration::from_secs(1)).await.unwrap().ready);
        }
        context.set_health(Health::Healthy);
        assert!(app.ready(Duration::from_secs(1)).await.unwrap().ready);
        app.stop();
        assert!(!app.snapshot().ready);
        let report = app.shutdown().await.unwrap();
        assert!(report.is_success());
        assert_eq!(report.snapshot.services[0].health, Health::Healthy);
    }
}

#[tokio::test]
async fn failure_and_natural_completion_do_not_wait_for_host_signal() {
    let report = App::builder()
        .service_fn(finite("step"), |_| async { Ok(()) })
        .build()
        .unwrap()
        .run_until(pending::<Result<(), io::Error>>())
        .await
        .unwrap();
    assert!(report.is_success());
    for after_ready in [false, true] {
        let error = App::builder()
            .service_fn("fault", move |context| async move {
                if after_ready {
                    context.ready();
                }
                Err(io::Error::other("original-service-error").into())
            })
            .build()
            .unwrap()
            .run_until(pending::<Result<(), io::Error>>())
            .await
            .unwrap_err();
        let report = error.report().unwrap();
        assert!(!report.is_success());
        assert!(
            matches!(&report.snapshot.services[0].failures[0], ServiceFailure::Error(e) if e.to_string()=="original-service-error")
        );
    }
}

#[tokio::test(start_paused = true)]
async fn finite_work_may_exceed_resident_startup_timeout() {
    let handle = App::builder()
        .options(AppOptions {
            startup_timeout: Duration::from_secs(30),
            ..AppOptions::default()
        })
        .service_fn(finite("migration"), |context| async move {
            context.ready();
            tokio::time::sleep(Duration::from_secs(45)).await;
            Ok(())
        })
        .service_fn("resident", resident)
        .build()
        .unwrap()
        .start()
        .unwrap();

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    let snapshot = handle.snapshot();
    assert!(
        !snapshot.ready,
        "finite completion has not opened its dependency gate"
    );
    assert!(
        !snapshot.services[0]
            .failures
            .iter()
            .any(|failure| matches!(failure, ServiceFailure::StartupTimeout)),
        "Finite work was subjected to the Resident startup_timeout"
    );
    assert_eq!(snapshot.services[0].phase, ServicePhase::Starting);

    tokio::time::advance(Duration::from_secs(14)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let ready = handle.snapshot();
    assert!(ready.ready);
    assert_eq!(ready.services[0].phase, ServicePhase::Completed);
    assert_eq!(ready.services[0].health, Health::Healthy);
    assert!(handle.shutdown().await.unwrap().is_success());
}

#[tokio::test]
async fn stop_source_error_retains_report_and_business_failure() {
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            context.ready();
            let _ = send.send(());
            context.stopping().await;
            Err(io::Error::other("close-service-error").into())
        })
        .build()
        .unwrap();
    let error = app
        .run_until(async {
            receive.await.unwrap();
            Err::<(), _>(io::Error::other("signal-error"))
        })
        .await
        .unwrap_err();
    let RunError::StopSource { source, report } = error else {
        panic!("lost host error")
    };
    assert_eq!(source.to_string(), "signal-error");
    assert!(!report.is_success());
    assert!(
        matches!(&report.snapshot.services[0].failures[0], ServiceFailure::Error(e) if e.to_string()=="close-service-error")
    );
}

#[tokio::test]
async fn resident_early_ok_and_panic_are_faults_but_optional_fault_is_nonfatal() {
    for panic in [false, true] {
        let result = App::builder()
            .service_fn("early", move |_| async move {
                assert!(!panic, "root-panic");
                Ok(())
            })
            .build()
            .unwrap()
            .run_until(pending::<Result<(), io::Error>>())
            .await;
        assert!(matches!(result, Err(RunError::Failed { .. })));
    }
    let app = App::builder()
        .service_fn("required", resident)
        .service_fn(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            |_| async { Err(io::Error::other("optional-error").into()) },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let mut changes = app.observer().subscribe();
    loop {
        let current = changes.current();
        if current.ready && current.health == Health::Degraded {
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn startup_timeout_stops_and_freezes_dependents() {
    let called = Arc::new(AtomicBool::new(false));
    let seen = called.clone();
    let app = App::builder()
        .options(short_options())
        .service_fn("provider", |context| async move {
            context.stopping().await;
            context.ready();
            Ok(())
        })
        .service_fn(
            ServiceSpec::new("consumer").depends_on("provider"),
            move |_| async move {
                seen.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let report = app.wait().await.unwrap();
    assert!(!called.load(Ordering::SeqCst));
    assert!(matches!(
        report.snapshot.services[0].failures[0],
        ServiceFailure::StartupTimeout
    ));
    assert_eq!(report.snapshot.services[1].phase, ServicePhase::NotStarted);
    assert!(!report.snapshot.services[0].initialized);
}

#[tokio::test]
async fn finite_record_error_and_old_health_do_not_block_dependency() {
    for old_health in [Health::Unknown, Health::Degraded, Health::Unhealthy] {
        let app = App::builder()
            .service_fn(finite("initialize"), move |context| async move {
                context.set_health(old_health);
                context
                    .work()
                    .spawn(WorkSpec::new("record-only"), |_| async {
                        Err::<(), _>(io::Error::other("record-error"))
                    })?;
                Ok(())
            })
            .service_fn(
                ServiceSpec::new("consumer").depends_on("initialize"),
                resident,
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        let ready = app.ready(Duration::from_secs(1)).await.unwrap();
        assert_eq!(ready.health, Health::Healthy);
        assert!(ready.services[0].initialized);
        assert!(app.shutdown().await.unwrap().is_success());
    }
}

#[tokio::test]
async fn finite_fail_service_error_never_unlocks_consumer_or_final_success() {
    for consumer in [false, true] {
        let entered = Arc::new(AtomicBool::new(false));
        let marker = entered.clone();
        let mut builder = App::builder().service_fn(finite("initialize"), |context| async move {
            context
                .work()
                .spawn(WorkSpec::new("fault").fail_service(), |_| async {
                    tokio::task::yield_now().await;
                    Err::<(), _>(io::Error::other("owner-fault"))
                })?;
            Ok(())
        });
        if consumer {
            builder = builder.service_fn(
                ServiceSpec::new("consumer").depends_on("initialize"),
                move |context| async move {
                    marker.store(true, Ordering::SeqCst);
                    resident(context).await
                },
            );
        }
        let report = builder
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(!entered.load(Ordering::SeqCst));
        assert!(!report.is_success());
        assert!(!report.snapshot.services[0].initialized);
        assert!(
            report.snapshot.services[0]
                .failures
                .iter()
                .any(|f| matches!(f, ServiceFailure::Work(_)))
        );
    }
}

#[tokio::test]
async fn provider_waits_for_consumer_root_and_child_then_cleanup_lifo() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let provider_events = events.clone();
    let child_events = events.clone();
    let cleanup_a = events.clone();
    let cleanup_b = events.clone();
    let (send, receive) = oneshot::channel();
    let child_release = Arc::new(Notify::new());
    let release = child_release.clone();
    let app = App::builder()
        .cleanup("first", move || async move {
            cleanup_a.lock().unwrap().push("cleanup-first");
            Ok(())
        })
        .cleanup("second", move || async move {
            cleanup_b.lock().unwrap().push("cleanup-second");
            Ok(())
        })
        .service_fn("provider", move |context| async move {
            context.ready();
            context.stopping().await;
            provider_events.lock().unwrap().push("provider-stop");
            Ok(())
        })
        .service_fn(
            ServiceSpec::new("consumer").depends_on("provider"),
            move |context| async move {
                context
                    .work()
                    .spawn(WorkSpec::new("child"), move |work| async move {
                        work.stopping().await;
                        release.notified().await;
                        child_events.lock().unwrap().push("child-release");
                        Ok::<(), BoxError>(())
                    })?;
                context.ready();
                let _ = send.send(());
                context.stopping().await;
                Ok(())
            },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.await.unwrap();
    app.stop();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(5)).await.unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(
        events.lock().unwrap().is_empty(),
        "provider stopped before consumer child released"
    );
    child_release.notify_one();
    app.shutdown().await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            "child-release",
            "provider-stop",
            "cleanup-second",
            "cleanup-first"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn hanging_cleanup_retains_lifo_and_historical_budget() {
    let done = Arc::new(AtomicBool::new(false));
    let done_close = done.clone();
    let release = Arc::new(Notify::new());
    let release_close = release.clone();
    let app = App::builder()
        .options(short_options())
        .cleanup("later", move || async move {
            done_close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .cleanup("hanging", move || async move {
            release_close.notified().await;
            Ok(())
        })
        .service_fn("service", resident)
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let observed = app
        .shutdown_within(Duration::from_millis(100))
        .await
        .unwrap();
    let ShutdownProgress::Pending(snapshot) = observed else {
        panic!("cleanup falsely completed")
    };
    assert!(
        snapshot.budget_exceeded,
        "stop budget overrun was not published"
    );
    assert!(!done.load(Ordering::SeqCst));
    assert_eq!(snapshot.current_cleanup.as_deref(), Some("hanging"));
    assert_eq!(snapshot.pending_cleanup, ["later"]);
    // A late stop must not renew or erase the historical deadline.
    app.stop();
    release.notify_one();
    let report = app.shutdown().await.unwrap();
    assert!(report.snapshot.budget_exceeded);
    assert!(done.load(Ordering::SeqCst));
}

#[tokio::test]
async fn single_root_keeps_child_and_cleanup_after_root_return() {
    let release = Arc::new(Notify::new());
    let child = release.clone();
    let closed = Arc::new(AtomicBool::new(false));
    let close = closed.clone();
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .cleanup("resource", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service_fn(finite("initialize"), move |context| async move {
            context
                .work()
                .spawn(WorkSpec::new("child"), move |_| async move {
                    send.send(()).unwrap();
                    child.notified().await;
                    Ok::<(), BoxError>(())
                })?;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.await.unwrap();
    let result = app
        .observer()
        .drain(Duration::from_millis(5))
        .await
        .unwrap();
    assert!(
        matches!(result, ShutdownProgress::Pending(_)),
        "root return bypassed actual child release"
    );
    assert!(!closed.load(Ordering::SeqCst));
    release.notify_one();
    assert!(app.wait().await.unwrap().is_success());
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn initialization_cleanup_accepts_interrupted_init_and_rejects_after_ready() {
    let closed = Arc::new(AtomicBool::new(false));
    let close = closed.clone();
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            send.send(()).unwrap();
            context.stopping().await;
            context
                .defer_cleanup("acquired", move || async move {
                    close.store(true, Ordering::SeqCst);
                    Ok::<(), BoxError>(())
                })
                .unwrap();
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.await.unwrap();
    app.shutdown().await.unwrap();
    assert!(closed.load(Ordering::SeqCst));
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            context.ready();
            let rejected = context
                .defer_cleanup("late", || async { Ok::<(), BoxError>(()) })
                .unwrap_err();
            (rejected.into_inner())().await?;
            send.send(()).unwrap();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.await.unwrap();
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test]
async fn cleanup_failure_and_panic_keep_later_actions_and_original_errors() {
    let last = Arc::new(AtomicBool::new(false));
    let close = last.clone();
    let result = App::builder()
        .cleanup("last", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .cleanup("panic", || async { panic!("cleanup-panic") })
        .cleanup("error", || async {
            Err(io::Error::other("cleanup-error").into())
        })
        .service_fn(finite("step"), |_| async { Ok(()) })
        .build()
        .unwrap()
        .run_until(pending::<Result<(), io::Error>>())
        .await
        .unwrap_err();
    let reports = &result.report().unwrap().snapshot.cleanup;
    assert_eq!(
        reports[0].error.as_ref().unwrap().to_string(),
        "cleanup-error"
    );
    assert!(reports[1].panic.as_ref().unwrap().contains("cleanup-panic"));
    assert!(reports[2].is_success());
    assert!(last.load(Ordering::SeqCst));
}

#[tokio::test]
async fn last_control_drop_and_cancelled_run_until_still_drain() {
    let app = App::builder()
        .service_fn("service", resident)
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let observer = app.observer();
    let retained = app.clone();
    drop(app);
    assert!(!retained.snapshot().services[0].stopping);
    drop(retained);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observer.wait())
            .await
            .expect("control lease drop did not stop owned driver")
            .unwrap()
            .is_success()
    );
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("service", move |context| async move {
            context.ready();
            send.send(context.observer()).unwrap();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap();
    let run = tokio::spawn(app.run_until(pending::<Result<(), io::Error>>()));
    let observer = receive.await.unwrap();
    run.abort();
    let _ = run.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observer.wait())
            .await
            .expect("cancelled run_until lost lifecycle ownership")
            .unwrap()
            .is_success()
    );
}

#[test]
fn start_error_is_send_sync_and_recovers_unstarted_app() {
    fn assert_error<T: std::error::Error + Send + Sync>() {}
    assert_error::<miju_services::StartError>();
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let app = App::builder()
        .service_fn(finite("step"), move |_| async move {
            mark.store(true, Ordering::SeqCst);
            Ok(())
        })
        .build()
        .unwrap();
    let error: BoxError = app.start().unwrap_err().into();
    assert!(!called.load(Ordering::SeqCst));
    let app = error
        .downcast::<miju_services::StartError>()
        .unwrap()
        .into_app();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let report = runtime.block_on(async { app.start().unwrap().wait().await.unwrap() });
    assert!(report.is_success());
    assert!(called.load(Ordering::SeqCst));
}

#[test]
fn runtime_loss_before_driver_poll_drops_captures_without_invoking_them() {
    struct Capture(Arc<AtomicBool>);
    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let root_dropped = Arc::new(AtomicBool::new(false));
    let cleanup_dropped = Arc::new(AtomicBool::new(false));
    let root_capture = Capture(root_dropped.clone());
    let cleanup_capture = Capture(cleanup_dropped.clone());
    let called = Arc::new(AtomicBool::new(false));
    let invocation = called.clone();
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let close = cleanup_called.clone();
    let app = App::builder()
        .service_fn("unpolled", move |_| async move {
            let _capture = root_capture;
            invocation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .cleanup("unpolled", move || async move {
            let _capture = cleanup_capture;
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .build()
        .unwrap();
    let handle = {
        let _entered = runtime.enter();
        app.start().unwrap()
    };
    drop(runtime);
    assert!(root_dropped.load(Ordering::SeqCst));
    assert!(cleanup_dropped.load(Ordering::SeqCst));
    assert!(!called.load(Ordering::SeqCst));
    assert!(!cleanup_called.load(Ordering::SeqCst));
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(
        observer
            .block_on(handle.wait())
            .unwrap_err()
            .snapshot
            .driver_lost
    );
}

#[test]
fn runtime_loss_wakes_current_and_future_observers_without_completion_certificate() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let handle = runtime.block_on(async {
        let handle = App::builder()
            .service_fn("service", resident)
            .build()
            .unwrap()
            .start()
            .unwrap();
        handle.ready(Duration::from_secs(1)).await.unwrap();
        handle
    });
    let observer = handle.observer();
    drop(runtime);
    let other = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    other.block_on(async {
        assert!(observer.wait().await.unwrap_err().snapshot.driver_lost);
        assert!(handle.wait().await.is_err());
        assert!(matches!(
            handle.shutdown().await,
            Err(RunError::DriverLost { .. })
        ));
    });
}

#[test]
fn driver_loss_keeps_an_already_observed_stop_source_error() {
    let app = App::builder()
        .cleanup("pending-close", pending::<Result<(), BoxError>>)
        .service_fn("service", resident)
        .build()
        .unwrap();
    let mut running = Box::pin(
        app.run_until(async { Err::<(), _>(io::Error::other("signal-before-driver-loss")) }),
    );
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    owner.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(5), running.as_mut())
                .await
                .is_err()
        );
    });
    drop(owner);
    let observer_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let error = observer_runtime.block_on(running).unwrap_err();
    let RunError::DriverLost {
        stop_source,
        source,
    } = error
    else {
        panic!("falsely reported actual completion")
    };
    assert!(source.snapshot.driver_lost);
    assert_eq!(
        stop_source.unwrap().to_string(),
        "signal-before-driver-loss"
    );
}

struct ErrorThenDropPanic(Arc<AtomicBool>);

impl std::future::Future for ErrorThenDropPanic {
    type Output = Result<(), BoxError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(Err(io::Error::other("result-before-release").into()))
    }
}

impl Drop for ErrorThenDropPanic {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
        panic!("release-panic");
    }
}

#[tokio::test]
async fn root_and_cleanup_keep_error_and_release_panic_separately() {
    use miju_services::{Service, report::ServiceOutcome};
    struct FaultService(Arc<AtomicBool>);
    impl Service for FaultService {
        type Error = BoxError;
        fn run(
            self,
            _: ServiceContext,
        ) -> impl std::future::Future<Output = Result<(), BoxError>> + Send {
            ErrorThenDropPanic(self.0)
        }
    }
    let root_dropped = Arc::new(AtomicBool::new(false));
    let close_dropped = Arc::new(AtomicBool::new(false));
    let close = close_dropped.clone();
    let error = App::builder()
        .cleanup("release", move || ErrorThenDropPanic(close))
        .service("fault", FaultService(root_dropped.clone()))
        .build()
        .unwrap()
        .run_until(pending::<io::Result<()>>())
        .await
        .unwrap_err();
    let report = error.report().unwrap();
    assert!(root_dropped.load(Ordering::SeqCst));
    assert!(close_dropped.load(Ordering::SeqCst));
    assert!(
        matches!(&report.snapshot.services[0].outcome, Some(ServiceOutcome::Failed(e)) if e.to_string()=="result-before-release")
    );
    assert!(
        report.snapshot.services[0]
            .failures
            .iter()
            .any(|f| matches!(f, ServiceFailure::ReleasePanic(_)))
    );
    assert_eq!(
        report.snapshot.cleanup[0]
            .error
            .as_ref()
            .unwrap()
            .to_string(),
        "result-before-release"
    );
    assert!(report.snapshot.cleanup[0].release_panic.is_some());
}

#[test]
fn unstarted_cleanup_is_not_an_async_drop_guarantee() {
    let closed = Arc::new(AtomicBool::new(false));
    let close = closed.clone();
    assert!(
        App::builder()
            .cleanup("store", move || async move {
                close.store(true, Ordering::SeqCst);
                Ok(())
            })
            .build()
            .is_err()
    );
    assert!(!closed.load(Ordering::SeqCst));
    let close = closed.clone();
    let app = App::builder()
        .cleanup("store", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service_fn("service", resident)
        .build()
        .unwrap();
    drop(app);
    assert!(!closed.load(Ordering::SeqCst));
}

#[test]
fn build_rejects_invalid_dag_and_budgets_without_running_entries() {
    assert!(App::builder().build().is_err());
    assert!(App::builder().service_fn(" ", resident).build().is_err());
    assert!(
        App::builder()
            .service_fn("a", resident)
            .service_fn("a", resident)
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .service_fn(ServiceSpec::new("a").depends_on("missing"), resident)
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .service_fn(ServiceSpec::new("a").depends_on("a"), resident)
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .service_fn(
                ServiceSpec::new("a").depends_on("b").depends_on("b"),
                resident
            )
            .service_fn("b", resident)
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .service_fn(ServiceSpec::new("a").depends_on("b"), resident)
            .service_fn(
                ServiceSpec::new("b").criticality(Criticality::Optional),
                resident
            )
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .options(AppOptions {
                shutdown_grace: Duration::ZERO,
                ..AppOptions::default()
            })
            .service_fn("a", resident)
            .build()
            .is_err()
    );
    assert!(
        App::builder()
            .options(AppOptions {
                abort_wait: Duration::MAX,
                ..AppOptions::default()
            })
            .service_fn("a", resident)
            .build()
            .is_err()
    );
}
