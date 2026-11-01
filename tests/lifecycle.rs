//! Public lifecycle contracts, including deterministic shutdown interleavings.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use miju_services::{
    AppObserver, AppOptions, Application, BoxError, Criticality, DrainObservation, Health,
    Lifetime, ReadyError, RootOutcome, ServiceContext, ServiceSpec, ShutdownReport, WorkSpec,
    service_fn,
};
use tokio::sync::oneshot;

const LIMIT: Duration = Duration::from_secs(3);

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("public lifecycle operation did not finish")
}

fn announce(context: &ServiceContext) -> Result<(), BoxError> {
    match context
        .set_health(Health::Healthy)
        .and_then(|()| context.ready())
    {
        Ok(()) => Ok(()),
        Err(_) if context.is_stopping() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn wait_health(observer: &AppObserver, health: Health, ready: bool) {
    bounded(async {
        let mut changes = observer.subscribe();
        loop {
            let snapshot = changes.current();
            if snapshot.health == health && snapshot.ready == ready {
                break;
            }
            changes.changed().await.expect("observer closed too early");
        }
    })
    .await;
}

#[tokio::test]
async fn ordinary_service_has_explicit_readiness_and_shared_final_result() {
    let (entered, entry) = oneshot::channel();
    let (release, initialize) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("ordinary"),
            service_fn(move |context: ServiceContext| async move {
                entered.send(()).unwrap();
                initialize.await?;
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(entry).await.unwrap();
    assert!(!observer.snapshot().ready);
    release.send(()).unwrap();
    assert!(bounded(observer.ready(LIMIT)).await.unwrap().ready);
    control.stop();
    assert!(!observer.snapshot().ready);
    let first = bounded(observer.wait()).await.unwrap();
    let second = bounded(observer.wait()).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(first.is_drained());
    assert!(first.is_success());
    assert!(matches!(
        bounded(control.shutdown()).await.unwrap().as_ref(),
        ShutdownReport::Completed(_)
    ));
}

#[tokio::test]
async fn stopping_before_between_and_after_initialization_notifications_is_not_failure() {
    for stage in 0..=2 {
        let (entered, entry) = oneshot::channel();
        let (release, initialize) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("initializing"),
                service_fn(move |context: ServiceContext| async move {
                    if stage >= 1 {
                        context.set_health(Health::Healthy)?;
                    }
                    if stage == 2 {
                        context.ready()?;
                    }
                    entered.send(()).unwrap();
                    initialize.await?;
                    context.stopping().await;
                    assert!(context.set_health(Health::Healthy).is_err());
                    assert!(context.ready().is_err());
                    announce(&context)?;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap();
        let control = app.start().unwrap();
        let observer = control.observer();
        bounded(entry).await.unwrap();
        if stage == 2 {
            bounded(observer.ready(LIMIT)).await.unwrap();
        }
        control.stop();
        release.send(()).unwrap();
        assert!(matches!(
            bounded(observer.ready(LIMIT)).await,
            Err(ReadyError::StoppedBeforeReady)
        ));
        let report = bounded(observer.wait()).await.unwrap();
        assert!(
            report.is_success(),
            "normal stop at notification stage {stage}: {report:?}"
        );
        assert!(!observer.snapshot().ready);
        assert!(
            report
                .services
                .iter()
                .all(|service| service.failure.is_none())
        );
        assert!(matches!(
            report.services[0].outcome,
            Some(RootOutcome::Succeeded)
        ));
    }
}

#[tokio::test]
async fn real_initialization_error_after_stop_is_preserved() {
    let (entered, entry) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("failing"),
            service_fn(move |context: ServiceContext| async move {
                entered.send(()).unwrap();
                context.stopping().await;
                announce(&context)?;
                Err::<(), BoxError>(std::io::Error::other("real initialization failure").into())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(entry).await.unwrap();
    control.stop();
    let report = bounded(observer.wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(
        matches!(&report.services[0].outcome, Some(RootOutcome::Failed(error))
        if error.to_string() == "real initialization failure")
    );
    assert!(report.services[0].failure.is_some());
}

#[tokio::test]
async fn health_changes_do_not_restart_or_reinitialize_service() {
    let (send_context, receive_context) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("health"),
            service_fn(move |context: ServiceContext| async move {
                context.ready()?;
                send_context.send(context.clone()).unwrap();
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    let context = bounded(receive_context).await.unwrap();
    wait_health(&observer, Health::Unknown, false).await;
    context.set_health(Health::Healthy).unwrap();
    wait_health(&observer, Health::Healthy, true).await;
    context.set_health(Health::Degraded).unwrap();
    wait_health(&observer, Health::Degraded, false).await;
    context.ready().unwrap();
    assert!(!observer.snapshot().ready);
    context.set_health(Health::Healthy).unwrap();
    wait_health(&observer, Health::Healthy, true).await;
    control.stop();
    assert!(bounded(observer.wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn finite_provider_waits_for_its_child_before_unlocking_consumer() {
    let (provider_entered, provider_entry) = oneshot::channel();
    let (release, child_release) = oneshot::channel();
    let (consumer_entered, mut consumer_entry) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("init").lifetime(Lifetime::Finite),
            service_fn(move |context: ServiceContext| async move {
                context.work().spawn(
                    WorkSpec::new("initialization-child"),
                    move |_| async move {
                        child_release.await?;
                        Ok::<(), BoxError>(())
                    },
                )?;
                context.ready()?;
                provider_entered.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("consumer").depends_on("init"),
            service_fn(move |context: ServiceContext| async move {
                consumer_entered.send(()).unwrap();
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(provider_entry).await.unwrap();
    assert!(matches!(
        consumer_entry.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    bounded(consumer_entry).await.unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    assert!(matches!(
        bounded(control.shutdown()).await.unwrap().as_ref(),
        ShutdownReport::Completed(report) if report.is_success()
    ));
}

#[tokio::test]
async fn provider_stop_waits_for_consumer_root_and_child() {
    let (provider_stopped, mut provider_stop) = oneshot::channel();
    let (child_started, child_start) = oneshot::channel();
    let (child_stopped, child_stop) = oneshot::channel();
    let (release, child_release) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                provider_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("consumer").depends_on("provider"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .work()
                    .spawn(WorkSpec::new("child"), move |work| async move {
                        child_started.send(()).unwrap();
                        work.stopping().await;
                        child_stopped.send(()).unwrap();
                        child_release.await?;
                        Ok::<(), BoxError>(())
                    })?;
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(child_start).await.unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    control.stop();
    bounded(child_stop).await.unwrap();
    assert!(matches!(
        provider_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    bounded(provider_stop).await.unwrap();
    assert!(
        bounded(control.observer().wait())
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
async fn cleanup_is_lifo_and_errors_do_not_skip_later_actions() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let owned_order = order.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("resource"),
            service_fn(move |context: ServiceContext| async move {
                for index in 0..3 {
                    let order = owned_order.clone();
                    context
                        .defer_cleanup(format!("cleanup-{index}"), move || async move {
                            order.lock().unwrap().push(index);
                            if index == 1 {
                                Err::<(), BoxError>(std::io::Error::other("flush failed").into())
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_or_else(|_| panic!("initialization cleanup registration rejected"));
                }
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    control.stop();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert_eq!(*order.lock().unwrap(), [2, 1, 0]);
    assert!(report.is_drained());
    assert!(!report.is_success());
    assert_eq!(
        report
            .cleanup
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        ["cleanup-2", "cleanup-1", "cleanup-0"]
    );
    assert!(report.cleanup[0].error.is_none());
    assert_eq!(
        report.cleanup[1].error.as_ref().unwrap().to_string(),
        "flush failed"
    );
    assert!(report.cleanup[2].error.is_none());
}

#[tokio::test]
async fn partial_initialization_failure_still_runs_owned_cleanup() {
    let (cleanup_started, cleanup_start) = oneshot::channel();
    let (release, cleanup_release) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("partial"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("partial-resource", move || async move {
                        cleanup_started.send(()).unwrap();
                        cleanup_release.await?;
                        Ok::<(), BoxError>(())
                    })
                    .unwrap_or_else(|_| panic!("cleanup registration rejected"));
                Err::<(), BoxError>(std::io::Error::other("second acquisition failed").into())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(cleanup_start).await.unwrap();
    assert!(bounded(observer.ready(LIMIT)).await.is_err());
    assert!(matches!(
        observer.drain(Duration::ZERO).await,
        DrainObservation::Pending(_)
    ));
    release.send(()).unwrap();
    let report = bounded(observer.wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(
        matches!(&report.services[0].outcome, Some(RootOutcome::Failed(error))
        if error.to_string() == "second acquisition failed")
    );
}

#[tokio::test]
async fn rejected_cleanup_returns_the_original_action() {
    let (send_context, receive_context) = oneshot::channel();
    let (executed, execution) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("complete").lifetime(Lifetime::Finite),
            service_fn(move |context: ServiceContext| async move {
                send_context.send(context).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let context = bounded(receive_context).await.unwrap();
    bounded(control.observer().wait()).await.unwrap();
    let result = context.defer_cleanup("too-late", move || async move {
        executed.send(()).unwrap();
        Ok::<(), BoxError>(())
    });
    let rejected = match result {
        Ok(_) => panic!("a finished root accepted cleanup"),
        Err(rejected) => rejected,
    };
    bounded(rejected.into_cleanup()()).await.unwrap();
    bounded(execution).await.unwrap();
    assert!(context.ready().is_err());
    assert!(context.set_health(Health::Healthy).is_err());
}

#[tokio::test]
async fn optional_failure_does_not_stop_required_service() {
    let (optional_failed, optional_failure) = oneshot::channel();
    let (required_stopped, mut required_stop) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("required"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                required_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            service_fn(move |_context: ServiceContext| async move {
                optional_failed.send(()).unwrap();
                Err::<(), BoxError>(std::io::Error::other("optional unavailable").into())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(optional_failure).await.unwrap();
    wait_health(&observer, Health::Degraded, true).await;
    assert!(matches!(
        required_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    control.stop();
    bounded(required_stop).await.unwrap();
    let report = bounded(observer.wait()).await.unwrap();
    assert!(report.is_success());
    let optional = report
        .services
        .iter()
        .find(|service| service.id.as_str() == "optional")
        .unwrap();
    assert!(matches!(&optional.outcome, Some(RootOutcome::Failed(error))
        if error.to_string() == "optional unavailable"));
    assert!(optional.failure.is_some());
}

#[tokio::test]
async fn cancelled_waiter_and_last_control_drop_keep_cleanup_owned() {
    let (cleanup_started, cleanup_start) = oneshot::channel();
    let (release, cleanup_release) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("resident"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("held", move || async move {
                        cleanup_started.send(()).unwrap();
                        cleanup_release.await?;
                        Ok::<(), BoxError>(())
                    })
                    .unwrap_or_else(|_| panic!("cleanup rejected"));
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(observer.ready(LIMIT)).await.unwrap();
    let waiter_observer = observer.clone();
    let waiter = tokio::spawn(async move { waiter_observer.wait().await });
    waiter.abort();
    assert!(bounded(waiter).await.unwrap_err().is_cancelled());
    drop(control);
    bounded(cleanup_start).await.unwrap();
    assert!(matches!(
        observer.drain(Duration::ZERO).await,
        DrainObservation::Pending(_)
    ));
    release.send(()).unwrap();
    assert!(bounded(observer.wait()).await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn budget_history_stays_incomplete_after_cleanup_later_finishes() {
    let (cleanup_started, cleanup_start) = oneshot::channel();
    let (release, cleanup_release) = oneshot::channel();
    let app = Application::builder()
        .options(AppOptions {
            startup_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(2),
            abort_wait: Duration::from_secs(1),
            recent_results: 8,
        })
        .service(
            ServiceSpec::new("cleanup"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("held", move || async move {
                        cleanup_started.send(()).unwrap();
                        cleanup_release.await?;
                        Ok::<(), BoxError>(())
                    })
                    .unwrap_or_else(|_| panic!("cleanup rejected"));
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    observer.ready(LIMIT).await.unwrap();
    control.stop();
    cleanup_start.await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    let first = control.shutdown().await.unwrap();
    assert!(matches!(first.as_ref(), ShutdownReport::Incomplete(_)));
    assert!(matches!(
        observer.drain(Duration::ZERO).await,
        DrainObservation::Pending(_)
    ));
    release.send(()).unwrap();
    let report = bounded(observer.wait()).await.unwrap();
    assert!(report.is_success());
    let second = control.shutdown().await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(matches!(second.as_ref(), ShutdownReport::Incomplete(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applications_sharing_runtime_stop_independently() {
    let mut controls = Vec::new();
    for _ in 0..2 {
        let app = Application::builder()
            .service(
                ServiceSpec::new("same-name"),
                service_fn(|context: ServiceContext| async move {
                    announce(&context)?;
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap();
        let control = app.start().unwrap();
        bounded(control.observer().ready(LIMIT)).await.unwrap();
        controls.push(control);
    }
    controls[0].stop();
    assert!(
        bounded(controls[0].observer().wait())
            .await
            .unwrap()
            .is_success()
    );
    assert!(controls[1].observer().snapshot().ready);
    controls[1].stop();
    assert!(
        bounded(controls[1].observer().wait())
            .await
            .unwrap()
            .is_success()
    );
}

#[test]
fn no_runtime_start_returns_the_application_for_retry() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("finite").lifetime(Lifetime::Finite),
            service_fn(|_: ServiceContext| async { Ok::<(), BoxError>(()) }),
        )
        .build()
        .unwrap();
    let recovered = match app.start() {
        Ok(_) => panic!("start outside a runtime succeeded"),
        Err(error) => error.into_application(),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let control = recovered.start().unwrap();
        assert!(
            bounded(control.observer().wait())
                .await
                .unwrap()
                .is_success()
        );
    });
}

#[tokio::test]
async fn stop_before_first_root_poll_releases_unstarted_captures() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Capture(Arc<AtomicUsize>);

    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let run_entered = entered.clone();
    let capture = Capture(dropped.clone());
    let app = Application::builder()
        .service(
            ServiceSpec::new("not-started"),
            service_fn(move |context: ServiceContext| async move {
                let _capture = capture;
                run_entered.fetch_add(1, Ordering::SeqCst);
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    control.stop();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert_eq!(entered.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.services[0].outcome,
        Some(RootOutcome::NotStarted)
    ));
    assert!(report.is_success());
}

#[test]
fn invalid_assembly_is_rejected_before_any_service_runs() {
    fn never_run() -> impl miju_services::Service<Error = BoxError> {
        async fn entry(_: ServiceContext) -> Result<(), BoxError> {
            panic!("build validation must not run service entries");
        }
        service_fn(entry)
    }

    assert!(Application::builder().build().is_err());
    assert!(
        Application::builder()
            .service(ServiceSpec::new(""), never_run())
            .build()
            .is_err()
    );
    assert!(
        Application::builder()
            .service(ServiceSpec::new("duplicate"), never_run())
            .service(ServiceSpec::new("duplicate"), never_run())
            .build()
            .is_err()
    );
    assert!(
        Application::builder()
            .service(ServiceSpec::new("a").depends_on("missing"), never_run())
            .build()
            .is_err()
    );
    assert!(
        Application::builder()
            .service(
                ServiceSpec::new("a").depends_on("b").depends_on("b"),
                never_run()
            )
            .service(ServiceSpec::new("b"), never_run())
            .build()
            .is_err()
    );
    assert!(
        Application::builder()
            .service(ServiceSpec::new("a").depends_on("b"), never_run())
            .service(ServiceSpec::new("b").depends_on("a"), never_run())
            .build()
            .is_err()
    );
    assert!(
        Application::builder()
            .service(ServiceSpec::new("a").depends_on("b"), never_run())
            .service(
                ServiceSpec::new("b").criticality(Criticality::Optional),
                never_run()
            )
            .build()
            .is_err()
    );
    for options in [
        AppOptions {
            startup_timeout: Duration::ZERO,
            ..AppOptions::default()
        },
        AppOptions {
            shutdown_grace: Duration::ZERO,
            ..AppOptions::default()
        },
        AppOptions {
            shutdown_grace: Duration::MAX,
            abort_wait: Duration::from_secs(1),
            ..AppOptions::default()
        },
    ] {
        assert!(
            Application::builder()
                .options(options)
                .service(ServiceSpec::new("valid"), never_run())
                .build()
                .is_err()
        );
    }
}

#[tokio::test]
async fn interrupted_initialization_can_still_transfer_cleanup() {
    let (entered, entry) = oneshot::channel();
    let (cleaned, cleanup) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("interrupted"),
            service_fn(move |context: ServiceContext| async move {
                entered.send(()).unwrap();
                context.stopping().await;
                context
                    .defer_cleanup("partially-acquired", move || async move {
                        cleaned.send(()).unwrap();
                        Ok::<(), BoxError>(())
                    })
                    .unwrap_or_else(|_| {
                        panic!("stop must not prevent initialization cleanup transfer")
                    });
                announce(&context)?;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(entry).await.unwrap();
    control.stop();
    let report = bounded(control.observer().wait()).await.unwrap();
    bounded(cleanup).await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.cleanup.len(), 1);
    assert_eq!(report.cleanup[0].label, "partially-acquired");
}

#[tokio::test]
async fn resident_return_without_stop_is_a_lifecycle_failure() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("unexpected"),
            service_fn(|context: ServiceContext| async move {
                announce(&context)?;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(matches!(
        report.services[0].outcome,
        Some(RootOutcome::Succeeded)
    ));
    assert!(report.services[0].failure.is_some());
}

#[tokio::test]
async fn initialization_panic_preserves_registered_cleanup() {
    async fn panic_after_registration(context: ServiceContext) -> Result<(), BoxError> {
        context
            .defer_cleanup("owned-before-panic", || async { Ok::<(), BoxError>(()) })
            .unwrap_or_else(|_| panic!("registration failed"));
        panic!("initialization panic");
    }

    let app = Application::builder()
        .service(
            ServiceSpec::new("panic"),
            service_fn(panic_after_registration),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(
        matches!(&report.services[0].outcome, Some(RootOutcome::Panicked(message))
        if message.as_ref() == "initialization panic")
    );
    assert_eq!(report.cleanup.len(), 1);
    assert_eq!(report.cleanup[0].label, "owned-before-panic");
    assert!(report.cleanup[0].error.is_none());
}

#[tokio::test(start_paused = true)]
async fn required_startup_timeout_stops_and_cleans_partial_initialization() {
    let (entered, entry) = oneshot::channel();
    let app = Application::builder()
        .options(AppOptions {
            startup_timeout: Duration::from_secs(2),
            ..AppOptions::default()
        })
        .service(
            ServiceSpec::new("timeout"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("partial", || async { Ok::<(), BoxError>(()) })
                    .unwrap();
                entered.send(()).unwrap();
                context.stopping().await;
                announce(&context)?;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    entry.await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    let report = bounded(control.observer().wait()).await.unwrap();
    assert!(!report.is_success());
    assert_eq!(
        report.services[0].failure.as_ref().unwrap().to_string(),
        "service initialization timed out"
    );
    assert_eq!(report.cleanup.len(), 1);
    assert!(report.cleanup[0].error.is_none());
}

#[tokio::test(start_paused = true)]
async fn optional_startup_timeout_is_local_and_later_global_stop_does_not_extend_it() {
    let (optional_entered, optional_entry) = oneshot::channel();
    let (optional_stopped, optional_stop) = oneshot::channel();
    let (required_stopped, mut required_stop) = oneshot::channel();
    let app = Application::builder()
        .options(AppOptions {
            startup_timeout: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(4),
            abort_wait: Duration::from_secs(1),
            recent_results: 8,
        })
        .service(
            ServiceSpec::new("required"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                required_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            service_fn(move |context: ServiceContext| async move {
                optional_entered.send(()).unwrap();
                context.stopping().await;
                optional_stopped.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    optional_entry.await.unwrap();
    control.observer().ready(LIMIT).await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    optional_stop.await.unwrap();
    assert!(matches!(
        required_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    control.stop();
    required_stop.await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    // The optional deadline is t=6, not t=7 from the later global request.
    let report = tokio::time::timeout(Duration::from_millis(1), control.observer().wait())
        .await
        .expect("global stop extended the existing optional deadline")
        .unwrap();
    let optional = report
        .services
        .iter()
        .find(|s| s.id.as_str() == "optional")
        .unwrap();
    assert!(matches!(optional.outcome, Some(RootOutcome::Aborted)));
    assert!(optional.failure.is_some());
    assert!(!report.is_success());
}

#[tokio::test]
async fn diamond_provider_outlives_both_branches() {
    let (provider_stopped, mut provider_stop) = oneshot::channel();
    let (left_stopped, left_stop) = oneshot::channel();
    let (right_stopped, right_stop) = oneshot::channel();
    let (release_left, left_release) = oneshot::channel();
    let (release_right, right_release) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                provider_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("left").depends_on("provider"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                left_stopped.send(()).unwrap();
                left_release.await?;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("right").depends_on("provider"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                right_stopped.send(()).unwrap();
                right_release.await?;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("outer")
                .depends_on("left")
                .depends_on("right"),
            service_fn(|context: ServiceContext| async move {
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    control.stop();
    bounded(left_stop).await.unwrap();
    bounded(right_stop).await.unwrap();
    release_left.send(()).unwrap();
    assert!(matches!(
        provider_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release_right.send(()).unwrap();
    bounded(provider_stop).await.unwrap();
    assert!(
        bounded(control.observer().wait())
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_blocking_work_retains_provider_and_cleanup_after_budget() {
    let (started, start) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let (provider_stopped, mut provider_stop) = oneshot::channel();
    let (cleaned, mut cleanup) = oneshot::channel();
    let app = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_millis(20),
            abort_wait: Duration::from_millis(20),
            ..AppOptions::default()
        })
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("provider-resource", move || async move {
                        cleaned.send(()).unwrap();
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                announce(&context)?;
                context.stopping().await;
                provider_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("consumer").depends_on("provider"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .work()
                    .spawn_blocking(WorkSpec::new("blocking"), move |_| {
                        started.send(()).unwrap();
                        blocked.recv()?;
                        Ok::<(), BoxError>(())
                    })?;
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(start).await.unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    let budget = bounded(control.shutdown()).await.unwrap();
    let retained = matches!(budget.as_ref(), ShutdownReport::Incomplete(snapshot)
        if snapshot.services.iter().find(|s| s.id.as_str() == "consumer").unwrap().work.active.len() == 1);
    let provider_pending = matches!(
        provider_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    let cleanup_pending = matches!(cleanup.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    // Always release the real blocking thread before asserting observations.
    release.send(()).unwrap();
    bounded(cleanup).await.unwrap();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert!(
        report
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .outcome
            .is_some()
    );
    assert!(retained && provider_pending && cleanup_pending);
    assert!(report.is_drained());
}

#[tokio::test(start_paused = true)]
async fn started_aggregate_survives_root_abort_and_keeps_dependency_until_certificate() {
    let (started, start) = oneshot::channel();
    let (release, aggregate_release) = oneshot::channel();
    let (provider_stopped, mut provider_stop) = oneshot::channel();
    let (cleaned, mut cleanup) = oneshot::channel();
    let app = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_secs(2),
            abort_wait: Duration::from_secs(1),
            ..AppOptions::default()
        })
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("resource", move || async move {
                        cleaned.send(()).unwrap();
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                announce(&context)?;
                context.stopping().await;
                provider_stopped.send(()).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("consumer").depends_on("provider"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .work()
                    .spawn(WorkSpec::aggregate("framework"), move |_| async move {
                        started.send(()).unwrap();
                        aggregate_release.await?;
                        Ok::<(), BoxError>(())
                    })?;
                announce(&context)?;
                std::future::pending::<()>().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    start.await.unwrap();
    control.observer().ready(LIMIT).await.unwrap();
    control.stop();
    tokio::time::advance(Duration::from_secs(3)).await;
    let budget = control.shutdown().await.unwrap();
    assert!(matches!(budget.as_ref(), ShutdownReport::Incomplete(_)));
    assert!(matches!(
        provider_stop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        cleanup.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    cleanup.await.unwrap();
    let report = bounded(control.observer().wait()).await.unwrap();
    assert!(
        report
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .outcome
            .is_some()
    );
    let consumer = report
        .services
        .iter()
        .find(|s| s.id.as_str() == "consumer")
        .unwrap();
    assert!(matches!(consumer.outcome, Some(RootOutcome::Aborted)));
    assert_eq!(consumer.work.completed, 1);
    assert!(consumer.work.unconfirmed.is_empty());
    assert!(!report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saved_scope_accepts_work_from_a_thread_without_runtime_context() {
    let (send_scope, receive_scope) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("thread-submit"),
            service_fn(move |context: ServiceContext| async move {
                announce(&context)?;
                send_scope.send(context.work().clone()).unwrap();
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let scope = bounded(receive_scope).await.unwrap();
    let thread = std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        scope
            .spawn(WorkSpec::new("from-thread"), |_| async {
                Ok::<(), BoxError>(())
            })
            .unwrap()
    });
    let work = thread.join().unwrap();
    let report = bounded(work.wait()).await;
    assert_eq!(
        report.completion(),
        miju_services::WorkCompletion::Confirmed
    );
    assert!(matches!(
        report.outcome(),
        miju_services::WorkOutcome::Succeeded
    ));
    assert!(matches!(
        bounded(control.shutdown()).await.unwrap().as_ref(),
        ShutdownReport::Completed(report) if report.is_success()
    ));
}

#[tokio::test]
async fn ready_observation_timeout_does_not_cancel_initialization_or_owned_cleanup() {
    let (entered, entry) = oneshot::channel();
    let (release, initialization) = oneshot::channel();
    let (cleaned, cleanup) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("slow-init"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("acquired", move || async move {
                        cleaned.send(()).unwrap();
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                entered.send(()).unwrap();
                initialization.await?;
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    let observer = control.observer();
    bounded(entry).await.unwrap();
    assert!(matches!(
        observer.ready(Duration::ZERO).await,
        Err(ReadyError::Timeout)
    ));
    assert!(!observer.snapshot().services[0].stopping);
    release.send(()).unwrap();
    bounded(observer.ready(LIMIT)).await.unwrap();
    control.stop();
    assert!(bounded(observer.wait()).await.unwrap().is_success());
    bounded(cleanup).await.unwrap();
}

#[tokio::test]
async fn cleanup_panic_is_retained_and_remaining_cleanup_runs() {
    let (cleaned, cleanup) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("cleanup-panic"),
            service_fn(move |context: ServiceContext| async move {
                context
                    .defer_cleanup("last", move || async move {
                        cleaned.send(()).unwrap();
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context
                    .defer_cleanup("panic", || async {
                        async fn fail() -> Result<(), BoxError> {
                            panic!("cleanup failure");
                        }
                        fail().await
                    })
                    .unwrap();
                announce(&context)?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap();
    let control = app.start().unwrap();
    bounded(control.observer().ready(LIMIT)).await.unwrap();
    control.stop();
    let report = bounded(control.observer().wait()).await.unwrap();
    bounded(cleanup).await.unwrap();
    assert!(!report.is_success());
    assert_eq!(report.cleanup[0].label, "panic");
    assert!(report.cleanup[0].error.is_some());
    assert_eq!(report.cleanup[1].label, "last");
    assert!(report.cleanup[1].error.is_none());
}
