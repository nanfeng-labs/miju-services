//! Unconfirmed aggregate responsibility is inspected in an isolated test process.

use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[test]
fn unconfirmed_responsibility_is_retained_in_isolated_host() {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "unconfirmed_child", "--nocapture"])
        .env("MIJU_SERVICES_UNCONFIRMED_CHILD", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut timed_out = false;
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            child.kill().unwrap();
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("isolated aggregate host stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        !timed_out,
        "isolated aggregate host exceeded its test budget"
    );
    assert!(output.status.success(), "isolated host assertions failed");
    assert!(stdout.contains("UNCONFIRMED_RETAINED_NOT_DRAINED"));
}

#[test]
fn unconfirmed_child() {
    if std::env::var_os("MIJU_SERVICES_UNCONFIRMED_CHILD").is_none() {
        return;
    }
    use miju_services::{
        AppOptions, Application, BoxError, DrainObservation, Health, ServiceContext, ServiceSpec,
        ShutdownReport, WorkCompletion, WorkOutcome, WorkSpec, service_fn,
    };
    use tokio::sync::oneshot;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (send_work, receive_work) = oneshot::channel();
        let (provider_stopped, mut provider_stop) = oneshot::channel();
        let (cleaned, mut cleanup) = oneshot::channel();
        let app = Application::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_millis(10),
                abort_wait: Duration::from_millis(10),
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
                    context.set_health(Health::Healthy)?;
                    context.ready()?;
                    context.stopping().await;
                    provider_stopped.send(()).unwrap();
                    Ok::<(), BoxError>(())
                }),
            )
            .service(
                ServiceSpec::new("consumer")
                    .depends_on("provider")
                    .max_work(std::num::NonZeroUsize::new(1).unwrap()),
                service_fn(move |context: ServiceContext| async move {
                    let work = context.work().spawn(
                        WorkSpec::aggregate("uncertified-framework"),
                        |_| async {
                            Err::<(), BoxError>(
                                std::io::Error::other("framework lost completion certificate")
                                    .into(),
                            )
                        },
                    )?;
                    send_work.send(work).unwrap();
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap();
        let control = app.start().unwrap();
        let observer = control.observer();
        let work = receive_work.await.unwrap();
        let result = work.wait().await;
        assert_eq!(result.completion(), WorkCompletion::Unconfirmed);
        assert!(matches!(result.outcome(), WorkOutcome::Failed(_)));
        let budget = control.shutdown().await.unwrap();
        let ShutdownReport::Incomplete(snapshot) = budget.as_ref() else {
            panic!("unconfirmed responsibility incorrectly drained");
        };
        let consumer = snapshot
            .services
            .iter()
            .find(|s| s.id.as_str() == "consumer")
            .unwrap();
        assert_eq!(consumer.work.active.len(), 1);
        assert_eq!(consumer.work.unconfirmed, [work.id()]);
        assert_eq!(consumer.work.completed, 0);
        assert_eq!(consumer.work.failed, 1);
        assert!(matches!(
            provider_stop.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            cleanup.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            observer.drain(Duration::from_millis(10)).await,
            DrainObservation::Pending(_)
        ));
        println!("UNCONFIRMED_RETAINED_NOT_DRAINED: {snapshot:?}");
        // This isolated host deliberately ends with outstanding responsibility.
        // Runtime destruction is not asserted to be drain or successful cleanup.
    });
}
