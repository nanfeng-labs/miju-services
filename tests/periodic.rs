//! Periodic timing and actual round ownership through public App paths.
#![cfg(feature = "periodic")]

use miju_services::{
    App, BoxError, ShutdownProgress,
    periodic::{self, FirstTick, OnRoundError, PeriodicMode, PeriodicRule},
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

async fn turn() {
    for _ in 0..6 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn delay_waits_after_completion_and_rate_skips_busy_slots() {
    for (mode, next) in [
        (PeriodicMode::FixedDelay, 13),
        (PeriodicMode::FixedRateSkip, 10),
    ] {
        let origin = Instant::now();
        let (send, mut receive) = mpsc::unbounded_channel();
        let (release, finish) = oneshot::channel();
        let mut finish = Some(finish);
        let rule = PeriodicRule::new(Duration::from_secs(5), mode).unwrap();
        let app = App::builder()
            .service(
                "sampler",
                periodic::periodic(rule, move |round| {
                    let finish = finish.take();
                    let send = send.clone();
                    async move {
                        send.send((round.round_id(), Instant::now(), round.scheduled_at()))
                            .unwrap();
                        if let Some(finish) = finish {
                            finish.await.unwrap();
                        }
                        Ok::<(), BoxError>(())
                    }
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        let first = receive.recv().await.unwrap();
        assert_eq!(first.1, origin);
        tokio::time::advance(Duration::from_secs(8)).await;
        release.send(()).unwrap();
        turn().await;
        assert!(receive.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(next - 9)).await;
        turn().await;
        assert!(
            receive.try_recv().is_err(),
            "next round began before its due time"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        turn().await;
        let second = receive
            .try_recv()
            .expect("next round did not start on its due time");
        assert_eq!(second.0, 2);
        assert_eq!(second.1.duration_since(origin).as_secs(), next);
        assert_eq!(second.2, second.1);
        assert_eq!(
            app.snapshot().services[0]
                .periodic
                .as_ref()
                .unwrap()
                .skipped,
            u64::from(mode == PeriodicMode::FixedRateSkip)
        );
        assert!(app.shutdown().await.unwrap().is_success());
    }
}

#[tokio::test(start_paused = true)]
async fn a_round_finishing_on_the_grid_can_start_the_next_slot_without_skipping() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let (release, finish) = oneshot::channel();
    let mut finish = Some(finish);
    let rule = PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip).unwrap();
    let app = App::builder()
        .service(
            "sampler",
            periodic::periodic(rule, move |round| {
                let send = send.clone();
                let finish = finish.take();
                async move {
                    send.send((round.round_id(), round.scheduled_at())).unwrap();
                    if let Some(finish) = finish {
                        finish.await.unwrap();
                    }
                    Ok::<(), BoxError>(())
                }
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(receive.recv().await.unwrap(), (1, origin));
    tokio::time::advance(Duration::from_secs(5)).await;
    release.send(()).unwrap();
    turn().await;
    assert_eq!(
        receive.try_recv().unwrap(),
        (2, origin + Duration::from_secs(5))
    );
    assert!(receive.try_recv().is_err());
    assert_eq!(
        app.snapshot().services[0]
            .periodic
            .as_ref()
            .unwrap()
            .skipped,
        0
    );
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn late_wake_runs_latest_slot_once_without_a_burst() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let rule = PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip)
        .unwrap()
        .first_tick(FirstTick::AfterPeriod);
    let app = App::builder()
        .service(
            "sampler",
            periodic::periodic(rule, move |round| {
                let send = send.clone();
                async move {
                    send.send((round.round_id(), round.scheduled_at())).unwrap();
                    Ok::<(), BoxError>(())
                }
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_secs(31)).await;
    turn().await;
    let (id, scheduled) = receive.try_recv().unwrap();
    assert_eq!(id, 1);
    assert_eq!(scheduled.duration_since(origin).as_secs(), 30);
    assert!(receive.try_recv().is_err());
    let stats = app.snapshot().services[0].periodic.clone().unwrap();
    assert_eq!(stats.skipped, 5);
    app.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn continuing_business_errors_are_recorded_without_retrying() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let service = periodic::fixed_delay(Duration::from_secs(5), move || {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("read-failed").into())
        }
    })
    .unwrap();
    let app = App::builder()
        .service("sampler", service)
        .build()
        .unwrap()
        .start()
        .unwrap();
    turn().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(4)).await;
    turn().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    turn().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let report = app.shutdown().await.unwrap();
    assert!(report.is_success());
    let snapshot = &report.snapshot.services[0];
    assert_eq!(snapshot.periodic.as_ref().unwrap().failed, 2);
    assert_eq!(snapshot.work.recent.len(), 2);
}

#[tokio::test]
async fn explicit_escalation_and_panics_fail_the_lifecycle() {
    let rule = PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay)
        .unwrap()
        .on_error(OnRoundError::StopService);
    let app = App::builder()
        .service(
            "sampler",
            periodic::periodic(rule, |_| async {
                Err::<(), _>(io::Error::other("original-read-error"))
            }),
        )
        .build()
        .unwrap();
    let error = app
        .run_until(std::future::pending::<Result<(), io::Error>>())
        .await
        .unwrap_err();
    assert!(!error.report().unwrap().is_success());
    assert!(format!("{error:?}").contains("original-read-error"));
    let app = App::builder()
        .service(
            "sampler",
            periodic::fixed_delay(Duration::from_secs(5), || async {
                panic!("round-panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap(),
        )
        .build()
        .unwrap();
    assert!(
        app.run_until(std::future::pending::<Result<(), io::Error>>())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn stop_owns_current_round_until_cleanup_completes() {
    let (entered, started) = oneshot::channel();
    let mut entered = Some(entered);
    let (release, finish) = oneshot::channel();
    let mut finish = Some(finish);
    let rule = PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay).unwrap();
    let app = App::builder()
        .service(
            "sampler",
            periodic::periodic(rule, move |round| {
                let send = entered.take().unwrap();
                let finish = finish.take().unwrap();
                async move {
                    send.send(()).unwrap();
                    round.stopping().await;
                    finish.await.unwrap();
                    Ok::<(), BoxError>(())
                }
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    started.await.unwrap();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert_eq!(app.snapshot().services[0].work.active.len(), 1);
    release.send(()).unwrap();
    assert!(app.wait().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn stop_before_first_tick_never_calls_the_operation() {
    let count = Arc::new(AtomicUsize::new(0));
    let called = count.clone();
    let rule = PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedDelay)
        .unwrap()
        .first_tick(FirstTick::AfterPeriod);
    let app = App::builder()
        .service(
            "sampler",
            periodic::periodic(rule, move |_| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok::<(), BoxError>(()) }
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    app.shutdown().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[test]
fn convenience_errors_infer_and_invalid_periods_are_rejected() {
    assert!(periodic::fixed_rate_skip(Duration::from_secs(5), || async { Ok(()) }).is_ok());
    assert!(periodic::fixed_delay(Duration::ZERO, || async { Ok(()) }).is_err());
    assert!(periodic::fixed_delay(Duration::MAX, || async { Ok(()) }).is_err());
}
