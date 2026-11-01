//! Regression for missed-grid accounting during shutdown.
//! Review checks for periodic stop statistics.
#![cfg(feature = "periodic")]
use miju_services::periodic::*;
use miju_services::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn busy_grid_before_stop_remains_counted() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    let app = Application::builder()
        .service(
            ServiceSpec::new("periodic"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip).unwrap(),
                move |_| {
                    let tx = tx.take().unwrap();
                    async move {
                        tx.send(()).unwrap();
                        tokio::time::sleep(Duration::from_secs(8)).await;
                        Ok::<(), BoxError>(())
                    }
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    rx.await.unwrap();
    tokio::time::advance(Duration::from_secs(7)).await;
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!(
        stats.skipped, 1,
        "grid at 5 was missed while running before stop at 7"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_last_round_keeps_busy_grid_count() {
    let app = Application::builder()
        .service(
            ServiceSpec::new("periodic"),
            periodic(
                PeriodicRule::new(Duration::from_secs(5), PeriodicMode::FixedRateSkip)
                    .unwrap()
                    .on_error(OnRoundError::StopService),
                move |_| async move {
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    Err::<(), BoxError>(std::io::Error::other("last round failed").into())
                },
            ),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = app.observer().wait().await.unwrap();
    let stats = exit.services[0].periodic.as_ref().unwrap();
    assert_eq!(stats.skipped, 1, "grid at 5 was missed before failure at 8");
}
