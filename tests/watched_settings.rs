//! Application-owned watch updates and the real service's per-round snapshots.

#[path = "../examples/support/watched_settings.rs"]
mod watched_settings;

use miju_services::{App, RunError, observe::ShutdownProgress, report::ServiceOutcome};
use std::{
    future::pending,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc, watch};
use watched_settings::Settings;

fn settings(message: &str) -> Arc<Settings> {
    Arc::new(Settings {
        interval: Duration::from_secs(1),
        message: message.into(),
    })
}

#[tokio::test(start_paused = true)]
async fn no_change_suppression_belongs_to_writer_and_equal_send_still_notifies() {
    let initial = settings("same value");
    let (updates, receiver) = watch::channel(initial.clone());
    let (send, mut rounds) = mpsc::unbounded_channel();
    let handle = App::builder()
        .service_fn("reader", move |context| {
            watched_settings::run(context, receiver, move |snapshot| {
                send.send(snapshot).unwrap();
                async { Ok(()) }
            })
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(rounds.recv().await.unwrap(), initial);

    let published = updates.send_if_modified(|current| {
        if current.as_ref() == initial.as_ref() {
            false
        } else {
            *current = initial.clone();
            true
        }
    });
    assert!(!published);
    tokio::task::yield_now().await;
    assert!(
        matches!(rounds.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "unchanged settings unexpectedly started another round"
    );

    // Tokio's ordinary send notifies even when PartialEq says the values are equal.
    updates.send(initial.clone()).unwrap();
    let next = tokio::time::timeout(Duration::from_millis(1), rounds.recv())
        .await
        .expect("equal send must notify before the one-second timer")
        .unwrap();
    assert_eq!(next, initial);
    assert!(handle.shutdown().await.unwrap().is_success());
}

#[tokio::test(start_paused = true)]
async fn in_progress_round_keeps_snapshot_and_next_round_reads_latest_update() {
    let initial = settings("initial");
    let latest = settings("latest");
    let (updates, receiver) = watch::channel(initial.clone());
    let release = Arc::new(Notify::new());
    let release_round = release.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let (send, mut snapshots) = mpsc::unbounded_channel();
    let handle = App::builder()
        .service_fn("reader", move |context| {
            watched_settings::run(context, receiver, move |snapshot| {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let release = release_round.clone();
                let send = send.clone();
                async move {
                    send.send((index, false, snapshot.clone())).unwrap();
                    if index == 0 {
                        release.notified().await;
                    }
                    send.send((index, true, snapshot)).unwrap();
                    Ok(())
                }
            })
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(snapshots.recv().await.unwrap(), (0, false, initial.clone()));

    updates.send(settings("intermediate")).unwrap();
    updates.send(latest.clone()).unwrap();
    assert_eq!(updates.borrow().as_ref(), latest.as_ref());
    assert_eq!(initial.message, "initial");
    assert!(
        matches!(snapshots.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "an update changed the in-progress round"
    );

    release.notify_one();
    assert_eq!(snapshots.recv().await.unwrap(), (0, true, initial));
    let next = tokio::time::timeout(Duration::from_millis(1), snapshots.recv())
        .await
        .expect("pending changes should start the next round without waiting the old interval")
        .unwrap();
    assert_eq!(next, (1, false, latest));
    handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn closed_sender_keeps_last_settings_without_busy_loop_or_false_completion() {
    let initial = settings("last valid");
    let (updates, receiver) = watch::channel(initial.clone());
    let (send, mut rounds) = mpsc::unbounded_channel();
    let handle = App::builder()
        .service_fn("reader", move |context| {
            watched_settings::run(context, receiver, move |snapshot| {
                send.send(snapshot).unwrap();
                async { Ok(()) }
            })
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(rounds.recv().await.unwrap(), initial);
    handle.ready(Duration::from_secs(1)).await.unwrap();

    drop(updates);
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert!(
        matches!(rounds.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "closed sender changed the consumer lifetime or started an immediate round"
    );
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(
        matches!(rounds.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "closed sender reset or skipped the configured interval"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(rounds.recv().await.unwrap(), initial);
    assert!(handle.snapshot().ready);
    assert!(handle.shutdown().await.unwrap().is_success());
}

#[tokio::test]
async fn stop_during_round_waits_for_that_round_and_does_not_start_another() {
    let (_updates, receiver) = watch::channel(settings("running"));
    let release = Arc::new(Notify::new());
    let release_round = release.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let rounds = counter.clone();
    let (send, mut started) = mpsc::unbounded_channel();
    let handle = App::builder()
        .service_fn("reader", move |context| {
            watched_settings::run(context, receiver, move |_| {
                rounds.fetch_add(1, Ordering::SeqCst);
                send.send(()).unwrap();
                let release = release_round.clone();
                async move {
                    release.notified().await;
                    Ok(())
                }
            })
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    started.recv().await.unwrap();
    assert!(
        matches!(
            handle
                .shutdown_within(Duration::from_millis(5))
                .await
                .unwrap(),
            ShutdownProgress::Pending(_)
        ),
        "stop cancelled an in-progress configured round"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert!(handle.shutdown().await.unwrap().is_success());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn round_error_remains_an_application_service_fault() {
    let (_updates, receiver) = watch::channel(settings("fault"));
    let error = App::builder()
        .service_fn("reader", move |context| {
            watched_settings::run(context, receiver, |_| async {
                Err(io::Error::other("round failed").into())
            })
        })
        .build()
        .unwrap()
        .run_until(pending::<io::Result<()>>())
        .await
        .unwrap_err();
    assert!(matches!(error, RunError::Failed { .. }));
    let report = error.report().unwrap();
    assert!(!report.is_success());
    assert!(matches!(
        &report.snapshot.services[0].outcome,
        Some(ServiceOutcome::Failed(error)) if error.to_string() == "round failed"
    ));
}
