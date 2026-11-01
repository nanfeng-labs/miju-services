//! Fixed-count lifecycle acceptance with fresh resource ownership on every iteration.

use miju_services::{
    Application, BoxError, Health, RootOutcome, ServiceContext, ServiceSpec, ShutdownReport,
    WorkSpec, service_fn,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

struct Resource(Arc<AtomicUsize>);

impl Resource {
    fn new(live: Arc<AtomicUsize>) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self(live)
    }
}

impl Drop for Resource {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_thousand_independent_lifecycles_release_all_owned_resources() {
    const ITERATIONS: usize = 1000;
    let mut completed = 0;
    let mut normal = 0;
    let mut initialization_failed = 0;
    for iteration in 0..ITERATIONS {
        let live = Arc::new(AtomicUsize::new(0));
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let service_live = live.clone();
        let service_cleanup_calls = cleanup_calls.clone();
        let fail = iteration % 2 == 1;
        let (child_started, child_start) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("same-instance-local-name"),
                service_fn(move |context: ServiceContext| async move {
                    let resource = Resource::new(service_live.clone());
                    context
                        .defer_cleanup("release", move || async move {
                            tokio::task::yield_now().await;
                            drop(resource);
                            service_cleanup_calls.fetch_add(1, Ordering::SeqCst);
                            Ok::<(), BoxError>(())
                        })
                        .unwrap();
                    let work_resource = Resource::new(service_live);
                    context
                        .work()
                        .spawn(WorkSpec::new("child"), move |work| async move {
                            let _resource = work_resource;
                            let _ = child_started.send(());
                            work.stopping().await;
                            Ok::<(), BoxError>(())
                        })?;
                    if fail {
                        return Err(
                            std::io::Error::other(format!("initialization-{iteration}")).into()
                        );
                    }
                    context.set_health(Health::Healthy)?;
                    context.ready()?;
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap();
        let control = app.start().unwrap();
        let observer = control.observer();
        let report = tokio::time::timeout(Duration::from_secs(5), async {
            if !fail {
                observer.ready(Duration::from_secs(2)).await.unwrap();
                child_start.await.unwrap();
            }
            if fail {
                let exit = observer.wait().await.unwrap();
                let budget = control.shutdown().await.unwrap();
                assert!(matches!(budget.as_ref(), ShutdownReport::Completed(_)));
                return exit;
            }
            let budget = control.shutdown().await.unwrap();
            assert!(
                matches!(budget.as_ref(), ShutdownReport::Completed(_)),
                "iteration {iteration}"
            );
            observer.wait().await.unwrap()
        })
        .await
        .unwrap_or_else(|_| panic!("iteration {iteration} exceeded five seconds"));
        assert!(report.is_drained(), "iteration {iteration}");
        assert_eq!(report.is_success(), !fail, "iteration {iteration}");
        assert_eq!(report.services.len(), 1);
        assert!(report.services[0].work.active.is_empty());
        assert!(report.services[0].work.unconfirmed.is_empty());
        assert_eq!(report.services[0].work.completed, 1);
        assert_eq!(report.cleanup.len(), 1);
        assert!(report.cleanup[0].error.is_none());
        assert_eq!(
            cleanup_calls.load(Ordering::SeqCst),
            1,
            "iteration {iteration}"
        );
        assert_eq!(live.load(Ordering::SeqCst), 0, "iteration {iteration}");
        if fail {
            assert!(
                matches!(&report.services[0].outcome, Some(RootOutcome::Failed(error))
                if error.to_string() == format!("initialization-{iteration}"))
            );
            initialization_failed += 1;
        } else {
            normal += 1;
        }
        drop(report);
        drop(observer);
        drop(control);
        assert_eq!(live.load(Ordering::SeqCst), 0);
        completed += 1;
    }
    assert_eq!((completed, normal, initialization_failed), (1000, 500, 500));
    println!(
        "LIFECYCLE_ACCEPTANCE completed={completed} normal={normal} initialization_failed={initialization_failed} resources_remaining=0"
    );
}
