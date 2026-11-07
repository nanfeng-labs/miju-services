//! Exercise the same assembly functions used by the executable examples.

#[path = "../examples/support/basic.rs"]
mod basic;
#[path = "../examples/support/managed_work.rs"]
mod managed_work;
#[path = "../examples/support/operational_host.rs"]
mod operational_host;
#[path = "../examples/support/shared_resource.rs"]
mod shared_resource;

use miju_services::{App, BoxError, RunError};
use operational_host::{HostError, HostOutcome};
use shared_resource::{CounterStore, Settings};
use std::{future::pending, io, sync::Arc, time::Duration};

#[tokio::test]
async fn basic_runs_until_host_stop() -> Result<(), BoxError> {
    let handle = basic::assemble()?.start()?;
    assert!(handle.ready(Duration::from_secs(1)).await?.ready);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), handle.wait())
            .await
            .is_err()
    );
    assert!(handle.shutdown().await?.is_success());
    Ok(())
}

#[tokio::test]
async fn dependency_and_managed_work_use_the_same_assembly() -> Result<(), BoxError> {
    let handle = managed_work::assemble()?.start()?;
    handle.ready(Duration::from_secs(1)).await?;
    assert!(handle.shutdown().await?.is_success());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn settings_reach_service_and_cleanup_closes_shared_store() -> Result<(), BoxError> {
    let store = Arc::new(CounterStore::default());
    let settings = Settings {
        interval: Duration::from_millis(10),
        increment: 7,
    };
    let handle = shared_resource::assemble(settings, store.clone())?.start()?;
    handle.ready(Duration::from_secs(1)).await?;
    tokio::time::advance(settings.interval).await;
    tokio::task::yield_now().await;
    assert_eq!(store.count(), 7);
    assert!(!store.is_closed());
    assert!(handle.shutdown().await?.is_success());
    assert!(store.is_closed());
    Ok(())
}

#[tokio::test]
async fn bootstrap_leaves_accepted_failure_cleanup_to_the_driver() -> Result<(), BoxError> {
    let store = Arc::new(CounterStore::default());
    let settings = Settings {
        interval: Duration::ZERO,
        ..Settings::default()
    };
    let error = shared_resource::run(settings, store.clone(), pending::<io::Result<()>>())
        .await
        .unwrap_err();
    assert!(error.source.downcast_ref::<RunError>().is_some());
    assert!(error.rollback.is_none());
    assert!(store.is_closed());
    Ok(())
}

fn failing_app() -> App {
    App::builder()
        .service_fn("broken", |_| async {
            Err(io::Error::other("startup failed").into())
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn operational_host_observes_failure_before_any_signal() {
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        operational_host::run(failing_app(), pending(), pending()),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(HostError::Run(RunError::Failed { .. }))
    ));
}

#[tokio::test]
async fn operational_host_retains_first_signal_error_with_report() {
    let app = basic::assemble().unwrap();
    let error = operational_host::run(
        app,
        async { Err(io::Error::other("first signal")) },
        pending(),
    )
    .await
    .unwrap_err();
    match error {
        HostError::Run(RunError::StopSource { source, report }) => {
            assert_eq!(source.to_string(), "first signal");
            assert!(report.is_success());
        }
        other => panic!("wrong error: {other:?}"),
    }
}

fn slow_cleanup_app() -> App {
    App::builder()
        .cleanup("flush", || async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .service_fn("service", |context| async move {
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn operational_host_retains_both_signal_errors_and_final_report() {
    let error = operational_host::run(
        slow_cleanup_app(),
        async { Err(io::Error::other("first signal")) },
        async { Err(io::Error::other("second signal")) },
    )
    .await
    .unwrap_err();
    match error {
        HostError::SecondSignal { source, result } => {
            assert_eq!(source.to_string(), "second signal");
            let RunError::StopSource { source, report } = result.unwrap_err() else {
                panic!("first signal error was lost");
            };
            assert_eq!(source.to_string(), "first signal");
            assert!(report.is_success());
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[tokio::test]
async fn operational_force_exit_is_a_host_decision_without_a_report() {
    let (observer_send, observer_receive) = tokio::sync::oneshot::channel();
    let (stop_send, stop_receive) = tokio::sync::oneshot::channel();
    let app = App::builder()
        .cleanup("flush", || async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .service_fn("service", |context| async move {
            let _ = observer_send.send(context.observer());
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap();
    let host = tokio::spawn(operational_host::run(
        app,
        async {
            stop_receive.await.unwrap();
            Ok(())
        },
        async { Ok(()) },
    ));
    let observer = observer_receive.await.unwrap();
    observer.ready(Duration::from_secs(1)).await.unwrap();
    stop_send.send(()).unwrap();
    let result = host.await.unwrap().unwrap();
    assert!(matches!(result, HostOutcome::ForceExit));
    // The test host keeps its runtime alive and independently confirms real completion.
    let report = tokio::time::timeout(Duration::from_secs(1), observer.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(report.is_success());
}

#[tokio::test]
async fn operational_normal_stop_returns_the_actual_report() {
    let result = operational_host::run(basic::assemble().unwrap(), async { Ok(()) }, pending())
        .await
        .unwrap();
    let HostOutcome::Exited(report) = result else {
        panic!("unexpected force exit")
    };
    assert!(report.is_success());
}
