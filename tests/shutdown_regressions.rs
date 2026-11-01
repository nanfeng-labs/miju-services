//! Regression paths discovered by independent lifecycle review.

use miju_services::*;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

struct PanicDrop;

impl Drop for PanicDrop {
    fn drop(&mut self) {
        panic!("drop failed");
    }
}

#[tokio::test(start_paused = true)]
async fn optional_abort_drop_panic_must_not_report_success() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let control = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_secs(1),
            abort_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .service(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            service_fn(move |c| async move {
                let _guard = PanicDrop;
                c.set_health(Health::Healthy)?;
                c.ready()?;
                tx.send(()).unwrap();
                std::future::pending::<Result<(), BoxError>>().await
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    rx.await.unwrap();
    control.stop();
    let exit = control.observer().wait().await.unwrap();
    eprintln!("exit={exit:?}");
    assert!(
        !exit.is_success(),
        "forced root abort was lost behind panic"
    );
}

struct ReadyDropPanic;

impl Future for ReadyDropPanic {
    type Output = Result<(), BoxError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for ReadyDropPanic {
    fn drop(&mut self) {
        panic!("cleanup destructor panic");
    }
}

#[tokio::test]
async fn cleanup_destructor_panic_must_continue_remaining_actions() {
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d = done.clone();
    let control = Application::builder()
        .service(
            ServiceSpec::new("finite").lifetime(Lifetime::Finite),
            service_fn(move |c| async move {
                c.defer_cleanup("later", move || async move {
                    d.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok::<(), BoxError>(())
                })
                .unwrap();
                c.defer_cleanup("panic", || ReadyDropPanic).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = control.observer().wait().await;
    eprintln!("exit={exit:?}");
    assert!(exit.is_ok());
    assert!(done.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn delayed_cleanup_poll_must_not_erase_expired_budget() {
    let mut completed_late = 0;
    for _ in 0..32 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let control = Application::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_secs(1),
                abort_wait: Duration::ZERO,
                ..Default::default()
            })
            .service(
                ServiceSpec::new("finite").lifetime(Lifetime::Finite),
                service_fn(move |c| async move {
                    c.defer_cleanup("slow", move || async move {
                        tx.send(()).unwrap();
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        rx.await.unwrap();
        control.stop();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        let exit = control.observer().wait().await.unwrap();
        if matches!(
            &*control.shutdown().await.unwrap(),
            ShutdownReport::Completed(_)
        ) {
            completed_late += 1;
            assert!(!exit.budget_exceeded);
        }
    }
    assert_eq!(
        completed_late, 0,
        "late cleanup completion forgot expired budget"
    );
}

#[tokio::test(start_paused = true)]
async fn optional_unfinished_scope_must_publish_local_budget_expiry() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let control = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_secs(1),
            abort_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .service(
            ServiceSpec::new("required"),
            service_fn(|c| async move {
                c.set_health(Health::Healthy)?;
                c.ready()?;
                c.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            service_fn(move |c| async move {
                c.work()
                    .spawn(WorkSpec::aggregate("hold"), move |_| async move {
                        tx.send(()).unwrap();
                        wait.await.unwrap();
                        Ok::<(), BoxError>(())
                    })?;
                tokio::task::yield_now().await;
                Err::<(), BoxError>(std::io::Error::other("optional failure").into())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    rx.await.unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let s = control.observer().snapshot();
    let exceeded = s
        .services
        .iter()
        .find(|s| s.id.as_str() == "optional")
        .unwrap()
        .stop_budget_exceeded;
    release.send(()).unwrap();
    control.stop();
    control.observer().wait().await.unwrap();
    assert!(
        exceeded,
        "optional unresolved responsibility did not publish expired local budget"
    );
}

#[tokio::test(start_paused = true)]
async fn optional_natural_panic_before_join_observation_is_not_forced_abort() {
    let (started, entered) = tokio::sync::oneshot::channel();
    let control = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_secs(1),
            abort_wait: Duration::ZERO,
            ..Default::default()
        })
        .service(
            ServiceSpec::new("optional").criticality(Criticality::Optional),
            service_fn(move |context| async move {
                context.set_health(Health::Healthy)?;
                context.ready()?;
                started.send(()).unwrap();
                context.stopping().await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                panic!("natural panic");
                #[allow(unreachable_code)]
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    entered.await.unwrap();
    control.stop();
    let exit = control.observer().wait().await.unwrap();
    assert!(matches!(
        &exit.services[0].outcome,
        Some(RootOutcome::Panicked(_))
    ));
    assert!(!exit.services[0].forced_abort);
    assert!(exit.is_success());
}

#[tokio::test(start_paused = true)]
async fn cleanup_within_budget_keeps_completed_conclusion() {
    let (started, entered) = tokio::sync::oneshot::channel();
    let control = Application::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_secs(5),
            abort_wait: Duration::ZERO,
            ..Default::default()
        })
        .service(
            ServiceSpec::new("finite").lifetime(Lifetime::Finite),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("short", move || async move {
                        started.send(()).unwrap();
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    entered.await.unwrap();
    assert!(matches!(
        &*control.shutdown().await.unwrap(),
        ShutdownReport::Completed(_)
    ));
    assert!(!control.observer().wait().await.unwrap().budget_exceeded);
}
