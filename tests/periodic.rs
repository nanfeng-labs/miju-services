//! Periodic timing and round ownership through the public `App` path.

use miju_services::{
    App, AssemblyError, BoxError, RunError,
    periodic::{PeriodicStats, Round, every, every_with},
    report::{Failure, Health, Phase},
    scope::Kind,
};
use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout},
};

/// Let the driver and the service run after a clock change.
async fn turn() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(3600), future)
        .await
        .expect("the lifecycle stalled")
}

fn stats(app: &miju_services::AppHandle) -> PeriodicStats {
    app.snapshot()
        .service("sampler")
        .unwrap()
        .detail::<PeriodicStats>()
        .expect("a periodic service publishes its stats")
        .clone()
}

// The README's example: a round starts at 0 s and finishes at 8 s with a 5 s
// period. The next round starts at 13 s with a fixed delay, at 10 s on the grid.
#[tokio::test(start_paused = true)]
async fn delay_waits_after_completion_and_the_grid_skips_busy_slots() {
    for (grid, next) in [(false, 13), (true, 10)] {
        let origin = Instant::now();
        let (send, mut receive) = mpsc::unbounded_channel();
        let (release, finish) = oneshot::channel::<()>();
        let mut finish = Some(finish);
        let periodic = every_with(Duration::from_secs(5), move |round: Round| {
            let (finish, send) = (finish.take(), send.clone());
            async move {
                send.send((round.id(), round.started_at(), round.scheduled_at()))
                    .unwrap();
                if let Some(finish) = finish {
                    finish.await.unwrap();
                }
                Ok::<_, BoxError>(())
            }
        });
        let mut app = App::new();
        app.service("sampler", if grid { periodic.on_grid() } else { periodic });
        let app = app.start().unwrap();

        let first = receive.recv().await.unwrap();
        assert_eq!((first.0, first.1), (1, origin));
        tokio::time::advance(Duration::from_secs(8)).await;
        release.send(()).unwrap();
        turn().await;
        assert!(receive.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(next - 9)).await;
        turn().await;
        assert!(
            receive.try_recv().is_err(),
            "grid={grid}: the next round began early"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        turn().await;
        let second = receive
            .try_recv()
            .expect("the next round did not start when due");
        assert_eq!(second.0, 2);
        assert_eq!(
            second.1.duration_since(origin).as_secs(),
            next,
            "grid={grid}"
        );
        assert_eq!(second.2, second.1);
        assert_eq!(stats(&app).skipped, u64::from(grid));
        assert!(app.shutdown().await.unwrap().is_success());
    }
}

#[tokio::test(start_paused = true)]
async fn a_round_finishing_on_the_grid_starts_the_next_slot_without_skipping() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let (release, finish) = oneshot::channel::<()>();
    let mut finish = Some(finish);
    let mut app = App::new();
    app.service(
        "sampler",
        every_with(Duration::from_secs(5), move |round: Round| {
            let (send, finish) = (send.clone(), finish.take());
            async move {
                send.send((round.id(), round.scheduled_at())).unwrap();
                if let Some(finish) = finish {
                    finish.await.unwrap();
                }
                Ok::<_, BoxError>(())
            }
        })
        .on_grid(),
    );
    let app = app.start().unwrap();
    assert_eq!(receive.recv().await.unwrap(), (1, origin));
    tokio::time::advance(Duration::from_secs(5)).await;
    release.send(()).unwrap();
    turn().await;
    assert_eq!(
        receive.try_recv().unwrap(),
        (2, origin + Duration::from_secs(5))
    );
    assert!(receive.try_recv().is_err());
    assert_eq!(stats(&app).skipped, 0);
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn a_late_wake_runs_the_latest_slot_once_without_a_burst() {
    let origin = Instant::now();
    let (send, mut receive) = mpsc::unbounded_channel();
    let mut app = App::new();
    app.service(
        "sampler",
        every_with(Duration::from_secs(5), move |round: Round| {
            let send = send.clone();
            async move {
                send.send((round.id(), round.scheduled_at())).unwrap();
                Ok::<_, BoxError>(())
            }
        })
        .on_grid()
        .first_after_period(),
    );
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    tokio::time::advance(Duration::from_secs(31)).await;
    turn().await;
    let (id, scheduled) = receive.try_recv().unwrap();
    assert_eq!(id, 1);
    assert_eq!(scheduled.duration_since(origin).as_secs(), 30);
    assert!(
        receive.try_recv().is_err(),
        "missed slots were run as a burst"
    );
    assert_eq!(stats(&app).skipped, 5);
    assert!(app.shutdown().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn business_errors_are_recorded_and_the_next_period_follows_without_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let calls = calls.clone();
        app.service(
            "sampler",
            every(Duration::from_secs(5), move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, SeqCst);
                    Err(io::Error::other("read failed").into())
                }
            }),
        );
    }
    let app = app.start().unwrap();
    turn().await;
    assert_eq!(calls.load(SeqCst), 1);
    tokio::time::advance(Duration::from_secs(4)).await;
    turn().await;
    assert_eq!(calls.load(SeqCst), 1, "an error was retried");
    tokio::time::advance(Duration::from_secs(1)).await;
    turn().await;
    assert_eq!(calls.load(SeqCst), 2);
    assert!(
        app.status().ready,
        "a recorded round error does not withdraw readiness"
    );

    let report = app.shutdown().await.unwrap();
    assert!(report.is_success());
    let recorded = report
        .service("sampler")
        .unwrap()
        .detail::<PeriodicStats>()
        .unwrap();
    assert_eq!(
        (recorded.started, recorded.finished, recorded.failed),
        (2, 2, 2)
    );
    assert_eq!(
        recorded.last_error.as_ref().unwrap().to_string(),
        "read failed"
    );
}

#[tokio::test]
async fn escalation_and_panics_fail_the_lifecycle() {
    let mut app = App::new();
    app.service(
        "sampler",
        every_with(Duration::from_secs(5), |_round: Round| async {
            Err::<(), _>(io::Error::other("original read error"))
        })
        .stop_on_error(),
    );
    let Err(RunError::Failed { report }) = app.run().await else {
        panic!("an escalated round error must fail the run");
    };
    let failures = &report.service("sampler").unwrap().failures;
    assert!(
        matches!(&failures[..], [Failure::Error(error)]
            if error.to_string() == "periodic round failed: original read error"),
        "{failures:?}"
    );

    let mut app = App::new();
    app.service(
        "sampler",
        every(Duration::from_secs(5), || async {
            if std::hint::black_box(true) {
                panic!("round panic");
            }
            Ok(())
        }),
    );
    let Err(RunError::Failed { report }) = app.run().await else {
        panic!("a panicking round must fail the run");
    };
    assert!(matches!(&report.service("sampler").unwrap().failures[..],
        [Failure::Panic(message)] if &**message == "round panic"));
}

#[tokio::test]
async fn a_running_round_owns_the_service_until_it_returns() {
    let (entered, started) = oneshot::channel();
    let mut entered = Some(entered);
    let (release, finish) = oneshot::channel::<()>();
    let mut finish = Some(finish);
    let mut app = App::new();
    app.service(
        "sampler",
        every_with(Duration::from_secs(5), move |round: Round| {
            let (entered, finish) = (entered.take().unwrap(), finish.take().unwrap());
            async move {
                entered.send(()).unwrap();
                round.stopping().await;
                finish.await.unwrap();
                Ok::<_, BoxError>(())
            }
        }),
    );
    let app = app.start().unwrap();
    started.await.unwrap();
    app.stop();
    assert!(
        timeout(Duration::from_millis(50), app.wait())
            .await
            .is_err(),
        "reported while a round was running"
    );
    assert_eq!(app.status().phase, Phase::Stopping);
    let snapshot = app.snapshot();
    assert!(
        snapshot
            .outstanding()
            .iter()
            .any(|item| item.kind == Kind::Root)
    );
    let running = stats(&app);
    assert_eq!((running.started, running.finished), (1, 0));
    assert!(running.finished_at.is_none());

    release.send(()).unwrap();
    let report = app.wait().await.unwrap();
    assert!(report.is_success());
    assert_eq!(
        report
            .service("sampler")
            .unwrap()
            .detail::<PeriodicStats>()
            .unwrap()
            .finished,
        1
    );
}

#[tokio::test(start_paused = true)]
async fn a_round_that_ignores_stop_is_aborted_after_the_grace_period() {
    let (entered, started) = oneshot::channel();
    let mut entered = Some(entered);
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    app.service(
        "sampler",
        every(Duration::from_secs(5), move || {
            let entered = entered.take();
            async move {
                if let Some(entered) = entered {
                    entered.send(()).unwrap();
                }
                std::future::pending::<()>().await;
                Ok(())
            }
        }),
    );
    let app = app.start().unwrap();
    started.await.unwrap();
    app.stop();
    assert!(timeout(Duration::from_secs(2), app.wait()).await.is_err());
    let report = app.wait().await.unwrap();
    let service = report.service("sampler").unwrap();
    assert!(service.forced);
    assert!(!report.is_success());
}

#[tokio::test(start_paused = true)]
async fn stop_before_the_first_tick_never_calls_the_operation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let calls = calls.clone();
        app.service(
            "sampler",
            every(Duration::from_secs(5), move || {
                calls.fetch_add(1, SeqCst);
                async { Ok(()) }
            })
            .first_after_period(),
        );
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    assert!(app.shutdown().await.unwrap().is_success());
    assert_eq!(calls.load(SeqCst), 0);
}

#[tokio::test]
async fn a_round_reaches_its_service_through_the_context() {
    let mut app = App::new();
    app.service(
        "sampler",
        every_with(Duration::from_secs(5), |round: Round| async move {
            round.context().set_health(Health::Degraded);
            Ok::<_, io::Error>(())
        }),
    );
    let app = app.start().unwrap();
    while app.status().health != Health::Degraded {
        bounded(app.changed()).await;
    }
    assert!(!app.status().ready);
    assert!(app.shutdown().await.unwrap().is_success());
}

#[test]
fn convenience_errors_infer_and_invalid_periods_are_rejected_at_assembly() {
    let mut app = App::new();
    app.service(
        "sampler",
        every(Duration::from_secs(5), || async { Ok(()) }).on_grid(),
    );
    assert_eq!(app.validate(), Ok(()));

    for (period, error) in [
        (Duration::ZERO, "period must be positive"),
        (Duration::MAX, "periodic time or counter overflow"),
    ] {
        let mut app = App::new();
        app.service("sampler", every(period, || async { Ok(()) }));
        assert_eq!(
            app.validate(),
            Err(AssemblyError::Service {
                service: "sampler".into(),
                error: error.into()
            })
        );
    }
}
