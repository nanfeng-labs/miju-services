//! Host ownership across budget expiry, observer cancellation, and control release.

use miju_services::{
    AppOptions, Application, BoxError, DrainObservation, Health, ServiceSpec, ShutdownReport,
    service_fn,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn exercise(runtime: tokio::runtime::Runtime) {
    let (release, wait) = tokio::sync::oneshot::channel();
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = cleaned.clone();
    let (first, observer, second, other) = runtime.block_on(async move {
        let first = Application::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_millis(20),
                abort_wait: Duration::ZERO,
                ..AppOptions::default()
            })
            .service(
                ServiceSpec::new("first"),
                service_fn(move |context| async move {
                    context
                        .defer_cleanup("blocked-save", move || async move {
                            wait.await?;
                            flag.store(true, Ordering::SeqCst);
                            Ok::<(), BoxError>(())
                        })
                        .unwrap();
                    context.set_health(Health::Healthy).unwrap();
                    context.ready().unwrap();
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        let second = Application::builder()
            .service(
                ServiceSpec::new("second"),
                service_fn(|context| async move {
                    context.set_health(Health::Healthy).unwrap();
                    context.ready().unwrap();
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        let observer = first.observer();
        let other = second.observer();
        observer.ready(Duration::from_secs(3)).await.unwrap();
        other.ready(Duration::from_secs(3)).await.unwrap();
        let budget = first.shutdown().await.unwrap();
        assert!(matches!(budget.as_ref(), ShutdownReport::Incomplete(_)));
        assert!(matches!(
            observer.drain(Duration::from_millis(5)).await,
            DrainObservation::Pending(_)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), observer.wait())
                .await
                .is_err()
        );
        assert!(other.snapshot().ready);
        (first, observer, second, other)
    });
    assert!(!cleaned.load(Ordering::SeqCst));
    release.send(()).unwrap();
    runtime.block_on(async move {
        assert!(observer.wait().await.unwrap().is_success());
        assert!(matches!(
            first.shutdown().await.unwrap().as_ref(),
            ShutdownReport::Incomplete(_)
        ));
        assert!(other.snapshot().ready);
        drop(first);
        drop(second);
        assert!(other.wait().await.unwrap().is_success());
    });
    assert!(cleaned.load(Ordering::SeqCst));
}

#[test]
fn host_keeps_current_thread_runtime_through_incomplete() {
    exercise(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
}

#[test]
fn host_keeps_multi_thread_runtime_through_incomplete() {
    exercise(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );
}
