//! Public periodic time lines, ownership, and failure policies.
#![cfg(feature = "periodic")]
use miju_services::{
    Application, BoxError, DrainObservation, ServiceSpec,
    periodic::{FirstTick, OnRoundError, PeriodicMode, PeriodicRule, periodic},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

async fn timeline(
    mode: PeriodicMode,
    first: FirstTick,
    duration: u64,
    fail: bool,
    expected: &[u64],
) {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let active = Arc::new(AtomicUsize::new(0));
    let owned = active.clone();
    let mut calls = 0;
    let service = periodic(
        PeriodicRule::new(Duration::from_secs(5), mode)
            .unwrap()
            .first_tick(first),
        move |round| {
            calls += 1;
            let number = calls;
            let send = send.clone();
            let active = owned.clone();
            async move {
                assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                send.send((
                    number,
                    round.round_id(),
                    round.actual_started_at().duration_since(origin),
                ))
                .unwrap();
                tokio::time::sleep(Duration::from_secs(duration)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                if fail {
                    Err::<(), BoxError>(std::io::Error::other("sample failed").into())
                } else {
                    Ok(())
                }
            }
        },
    );
    let app = Application::builder()
        .service(ServiceSpec::new("periodic"), service)
        .build()
        .unwrap()
        .start()
        .unwrap();
    for (index, seconds) in expected.iter().enumerate() {
        let (call, id, at) = receive.recv().await.unwrap();
        assert_eq!(call, index + 1);
        assert_eq!(id, (index + 1) as u64);
        assert_eq!(at, Duration::from_secs(*seconds));
    }
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!(stats.started, expected.len() as u64);
    assert_eq!(stats.finished, stats.started);
    assert_eq!(stats.failed, if fail { stats.started } else { 0 });
}

#[tokio::test(start_paused = true)]
async fn fixed_delay_uses_actual_completion() {
    timeline(
        PeriodicMode::FixedDelay,
        FirstTick::Immediate,
        8,
        false,
        &[0, 13],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn fixed_rate_skips_busy_grid() {
    timeline(
        PeriodicMode::FixedRateSkip,
        FirstTick::Immediate,
        8,
        false,
        &[0, 10],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn delayed_first_tick_keeps_both_semantics() {
    timeline(
        PeriodicMode::FixedDelay,
        FirstTick::AfterPeriod,
        8,
        false,
        &[5, 18],
    )
    .await;
    timeline(
        PeriodicMode::FixedRateSkip,
        FirstTick::AfterPeriod,
        8,
        false,
        &[5, 15],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn exact_boundary_and_short_rounds_keep_origin() {
    timeline(
        PeriodicMode::FixedRateSkip,
        FirstTick::Immediate,
        5,
        false,
        &[0, 5, 10],
    )
    .await;
    timeline(
        PeriodicMode::FixedRateSkip,
        FirstTick::Immediate,
        2,
        false,
        &[0, 5, 10],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn business_errors_continue_without_retry() {
    timeline(
        PeriodicMode::FixedDelay,
        FirstTick::Immediate,
        2,
        true,
        &[0, 7, 14],
    )
    .await;
    timeline(
        PeriodicMode::FixedRateSkip,
        FirstTick::Immediate,
        8,
        true,
        &[0, 10, 20],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn late_wakeup_runs_one_latest_grid_and_does_not_catch_up() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("late"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip)
                    .unwrap()
                    .first_tick(FirstTick::AfterPeriod),
                move |round| {
                    let send = send.clone();
                    async move {
                        send.send((round.scheduled_at(), round.actual_started_at()))
                            .unwrap();
                        Ok::<(), BoxError>(())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_secs(12)).await;
    let (scheduled, actual) = receive.recv().await.unwrap();
    assert_eq!(scheduled - origin, Duration::from_secs(10));
    assert_eq!(actual - origin, Duration::from_secs(12));
    let (scheduled, actual) = receive.recv().await.unwrap();
    assert_eq!(scheduled - origin, Duration::from_secs(15));
    assert_eq!(actual - origin, Duration::from_secs(15));
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!(stats.started, 2);
    assert_eq!(stats.skipped, 1);
}

#[tokio::test(start_paused = true)]
async fn stop_while_waiting_prevents_first_factory_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let record = calls.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("waiting"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay)
                    .unwrap()
                    .first_tick(FirstTick::AfterPeriod),
                move |_| {
                    record.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<(), BoxError>(()) }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    app.stop();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(app.observer().wait().await.unwrap().is_success());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn active_round_keeps_async_cleanup_after_stop() {
    let (started, start) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    let app = Application::builder()
        .service(
            ServiceSpec::new("cleanup"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap(),
                move |round| {
                    let started = started.take().unwrap();
                    async move {
                        started.send(()).unwrap();
                        round.stopping().await;
                        assert!(round.set_health(miju_services::Health::Healthy).is_err());
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        Ok::<(), BoxError>(())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    start.await.unwrap();
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_secs(1)).await,
        DrainObservation::Pending(_)
    ));
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success());
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (1, 1, 0));
}

#[tokio::test(start_paused = true)]
async fn stop_service_escalates_one_business_error() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("failed"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay)
                    .unwrap()
                    .on_error(OnRoundError::StopService),
                |_| async { Err::<(), BoxError>(std::io::Error::other("sample failure").into()) },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(!exit.is_success());
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (1, 1, 1));
    assert!(exit.services[0].failure.is_some());
}

#[tokio::test(start_paused = true)]
async fn factory_panic_is_not_continue() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("panic"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap(),
                |_| {
                    fn fail() -> std::future::Ready<Result<(), BoxError>> {
                        panic!("factory failed")
                    }
                    fail()
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(!exit.is_success());
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (1, 0, 0));
    assert_eq!(exit.services[0].work.failed, 1);
}

#[test]
fn zero_and_unrepresentable_period_are_rejected() {
    assert!(PeriodicRule::new(Duration::ZERO, PeriodicMode::FixedDelay).is_err());
    assert!(PeriodicRule::new(Duration::MAX, PeriodicMode::FixedRateSkip).is_err());
}

#[tokio::test(start_paused = true)]
async fn business_error_delay_is_measured_from_return_even_when_observer_is_late() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("delayed-error"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap(),
                move |round| {
                    let send = send.clone();
                    async move {
                        send.send(round.actual_started_at()).unwrap();
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        Err::<(), BoxError>(std::io::Error::other("business failure").into())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(receive.recv().await.unwrap() - origin, Duration::ZERO);
    let mut observer = app.observer().subscribe();
    // This observer stays dormant while the periodic driver advances independently.
    let second = receive.recv().await.unwrap();
    assert_eq!(second - origin, Duration::from_secs(7));
    let latest = observer.changed().await.unwrap();
    assert!(latest.services[0].periodic.as_ref().unwrap().finished >= 1);
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success());
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (2, 2, 2));
}

#[tokio::test(start_paused = true)]
async fn stopping_health_rejection_is_distinct_from_real_business_failure() {
    for business_failure in [false, true] {
        let (send, mut receive) = mpsc::unbounded_channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("stopping-health"),
                periodic(
                    PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay)
                        .unwrap()
                        .on_error(OnRoundError::StopService),
                    move |round| {
                        let send = send.clone();
                        async move {
                            send.send(()).unwrap();
                            round.stopping().await;
                            match round.set_health(miju_services::Health::Healthy) {
                                Err(_) if round.is_stopping() => {}
                                other => panic!("stopping health must be rejected: {other:?}"),
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            if business_failure {
                                Err::<(), BoxError>(
                                    std::io::Error::other("real stopping error").into(),
                                )
                            } else {
                                Ok(())
                            }
                        }
                    },
                ),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        receive.recv().await.unwrap();
        app.stop();
        let exit = app.observer().wait().await.unwrap();
        let stats = exit.services[0].periodic.as_ref().unwrap();
        assert_eq!(
            (stats.started, stats.finished, stats.failed),
            (1, 1, u64::from(business_failure))
        );
        assert_eq!(exit.is_success(), !business_failure);
        assert_eq!(exit.services[0].failure.is_some(), business_failure);
    }
}

#[tokio::test(start_paused = true)]
async fn business_future_poll_panic_is_fatal_under_continue() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("poll-panic"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap(),
                |_| async {
                    tokio::task::yield_now().await;
                    async fn panic_result() -> Result<(), BoxError> {
                        panic!("business poll panic");
                    }
                    panic_result().await
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(!exit.is_success());
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (1, 0, 0));
    assert_eq!(exit.services[0].work.failed, 1);
    assert!(matches!(exit.services[0].work.recent[0].outcome(),
        miju_services::WorkOutcome::Panicked(message) if message.as_ref() == "business poll panic"));
}

#[tokio::test(start_paused = true)]
async fn after_period_late_wakeup_counts_all_replaced_grid_points() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("late-after-period"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip)
                    .unwrap()
                    .first_tick(FirstTick::AfterPeriod),
                move |round| {
                    let send = send.clone();
                    async move {
                        send.send((
                            round.round_id(),
                            round.scheduled_at(),
                            round.actual_started_at(),
                        ))
                        .unwrap();
                        Ok::<(), BoxError>(())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_secs(22)).await;
    let (id, scheduled, actual) = receive.recv().await.unwrap();
    assert_eq!(id, 1);
    assert_eq!(scheduled - origin, Duration::from_secs(20));
    assert_eq!(actual - origin, Duration::from_secs(22));
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.skipped), (1, 1, 3));
}

#[tokio::test(start_paused = true)]
async fn shutdown_abort_releases_the_managed_round_without_claiming_business_return() {
    struct Capture(Arc<AtomicUsize>);

    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let released = Arc::new(AtomicUsize::new(0));
    let record = released.clone();
    let (send, mut receive) = mpsc::unbounded_channel();
    let app = Application::builder()
        .options(miju_services::AppOptions {
            shutdown_grace: Duration::from_secs(1),
            abort_wait: Duration::from_secs(1),
            ..miju_services::AppOptions::default()
        })
        .service(
            ServiceSpec::new("uncooperative"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap(),
                move |_| {
                    let capture = Capture(record.clone());
                    let send = send.clone();
                    async move {
                        let _capture = capture;
                        send.send(()).unwrap();
                        std::future::pending::<()>().await;
                        Ok::<(), BoxError>(())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    receive.recv().await.unwrap();
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    assert!(!exit.is_success());
    assert_eq!(released.load(Ordering::SeqCst), 1);
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!((stats.started, stats.finished, stats.failed), (1, 0, 0));
    assert_eq!(exit.services[0].work.aborted, 1);
    assert!(exit.services[0].work.active.is_empty());
    assert!(stats.last_round.as_ref().unwrap().finished_at.is_none());
}
