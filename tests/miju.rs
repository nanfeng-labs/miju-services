//! Real miju execution through the public lifecycle adapters.
#![cfg(feature = "miju")]
use futures_util::StreamExt;
use miju_core::{
    backend::{Backend, BackendConfig, TaskSink, finalize::Ephemeral, memory::MemoryStorage},
    monitor::Monitor,
    task::{Task, task_id::RandomId},
    worker::{Worker, builder::WorkerBuilder, context::WorkerContext, event::Event},
};
use miju_services::{
    Application, DrainObservation, Lifetime, Service, ServiceContext, ServiceSpec,
    miju::{monitor, worker},
    service_fn,
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::oneshot;

async fn bounded<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), f)
        .await
        .expect("miju lifecycle timed out")
}

#[derive(Debug)]
struct GatedBackend {
    tasks: VecDeque<Task<u32>>,
    close: oneshot::Receiver<()>,
    closes: Arc<AtomicUsize>,
    fail_next: bool,
}

impl Backend for GatedBackend {
    type Task = Task<u32>;
    type Error = std::io::Error;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        if self.fail_next {
            self.fail_next = false;
            return Poll::Ready(Some(Err(std::io::Error::other("backend failed"))));
        }
        Poll::Ready(self.tasks.pop_front().map(Ok))
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.close).poll(cx).map(|_| Ok(()))
    }
}

impl BackendConfig for GatedBackend {
    type Args = u32;
    type Config = ();
    type Kind = Ephemeral;
    type Layer = tower::layer::util::Identity;
    type Id = RandomId;

    fn config(&self) -> &() {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}

fn finite(values: &[u32]) -> GatedBackend {
    let (send, close) = oneshot::channel();
    send.send(()).unwrap();
    GatedBackend {
        tasks: values.iter().copied().map(Task::new).collect(),
        close,
        closes: Arc::new(AtomicUsize::new(0)),
        fail_next: false,
    }
}

#[tokio::test]
async fn finite_eof_waits_for_real_backend_close() {
    let (release, close) = oneshot::channel();
    let closes = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let record = handled.clone();
    let control = WorkerContext::new("finite");
    let jobs = WorkerBuilder::new(control.clone())
        .backend(GatedBackend {
            tasks: VecDeque::from([Task::new(7)]),
            close,
            closes: closes.clone(),
            fail_next: false,
        })
        .build(move |v: u32| {
            let record = record.clone();
            async move {
                record.fetch_add(v as usize, Ordering::SeqCst);
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("jobs").lifetime(Lifetime::Finite),
            worker(control, jobs.stream()),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        DrainObservation::Pending(_)
    ));
    assert_eq!(handled.load(Ordering::SeqCst), 7);
    assert!(closes.load(Ordering::SeqCst) > 0);
    release.send(()).unwrap();
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(exit.is_success());
    assert!(!exit.services[0].backend_cleanup_unconfirmed);
}

#[tokio::test]
async fn stop_drains_inflight_without_taking_second_queued_job() {
    let (started, start) = oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let (release, finish) = oneshot::channel();
    let finish = Arc::new(Mutex::new(Some(finish)));
    let count = Arc::new(AtomicUsize::new(0));
    let handled = count.clone();
    let mut backend = MemoryStorage::new();
    backend.push(1_u32).await.unwrap();
    backend.push(2_u32).await.unwrap();
    let context = WorkerContext::new("inflight");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(tower::limit::ConcurrencyLimitLayer::new(1))
        .build(move |_: u32| {
            let started = started.lock().unwrap().take();
            let finish = finish.lock().unwrap().take();
            let handled = handled.clone();
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                started.unwrap().send(()).unwrap();
                finish.unwrap().await.unwrap();
            }
        });
    let app = Application::builder()
        .service(ServiceSpec::new("jobs"), worker(context, jobs.stream()))
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(start).await.unwrap();
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        DrainObservation::Pending(_)
    ));
    release.send(()).unwrap();
    assert!(bounded(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn backend_error_preserves_unconfirmed_backend_cleanup() {
    let mut backend = finite(&[]);
    backend.fail_next = true;
    let closes = backend.closes.clone();
    let context = WorkerContext::new("backend-error");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .build(|_: u32| async {});
    let app = Application::builder()
        .service(ServiceSpec::new("jobs"), worker(context, jobs.stream()))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(!exit.is_success());
    assert!(exit.services[0].failure.is_some());
    assert!(exit.services[0].backend_cleanup_unconfirmed);
    assert_eq!(closes.load(Ordering::SeqCst), 0);
}

struct ReadinessFailure;

impl tower::Service<Task<u32>> for ReadinessFailure {
    type Response = ();
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<(), Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(std::io::Error::other("readiness failed")))
    }

    fn call(&mut self, _: Task<u32>) -> Self::Future {
        panic!("readiness failure must prevent call")
    }
}

#[tokio::test]
async fn readiness_error_is_warning_for_finite_but_unexpected_exit_for_resident() {
    for lifetime in [Lifetime::Finite, Lifetime::Resident] {
        let context = WorkerContext::new("readiness");
        let backend = finite(&[7]);
        let closes = backend.closes.clone();
        let jobs = Worker::<u32, _, _, _>::new(
            context.clone(),
            backend,
            ReadinessFailure,
            tower::layer::util::Identity::new(),
        );
        let app = Application::builder()
            .service(
                ServiceSpec::new("jobs").lifetime(lifetime),
                worker(context, jobs.stream()),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        let exit = bounded(app.observer().wait()).await.unwrap();
        assert_eq!(exit.is_success(), lifetime == Lifetime::Finite);
        assert_eq!(exit.services[0].execution_errors, 1);
        assert_eq!(exit.services[0].recent_execution_errors.len(), 1);
        assert!(!exit.services[0].backend_cleanup_unconfirmed);
        assert!(closes.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn call_error_is_recorded_and_later_task_still_runs() {
    let count = Arc::new(AtomicUsize::new(0));
    let handled = count.clone();
    let context = WorkerContext::new("call-error");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(finite(&[1, 2]))
        .build(move |v: u32| {
            let handled = handled.clone();
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                if v == 1 {
                    Err(std::io::Error::other("job failed"))
                } else {
                    Ok(())
                }
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("jobs").lifetime(Lifetime::Finite),
            worker(context, jobs.stream()),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(exit.is_success());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(exit.services[0].execution_errors, 1);
    assert!(exit.services[0].failure.is_none());
}

#[tokio::test]
async fn stop_before_start_notification_does_not_drop_non_unpin_worker_stream() {
    let (started, start) = oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let (release, close) = oneshot::channel();
    let closes = Arc::new(AtomicUsize::new(0));
    let close_count = closes.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let handled = count.clone();
    let context = WorkerContext::new("notification-race");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(GatedBackend {
            tasks: VecDeque::from([Task::new(1)]),
            close,
            closes,
            fail_next: false,
        })
        .build(move |_: u32| {
            let handled = handled.clone();
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("jobs"),
            service_fn(move |service: ServiceContext| {
                let wait_stop = service.clone();
                let events = jobs.stream().then(move |event| {
                    let stop = wait_stop.clone();
                    let started = started.clone();
                    async move {
                        if matches!(event, Ok(Event::Start)) {
                            started.lock().unwrap().take().unwrap().send(()).unwrap();
                            stop.stopping().await;
                        }
                        event
                    }
                });
                worker(context, events).run(service)
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(start).await.unwrap();
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        DrainObservation::Pending(_)
    ));
    assert!(close_count.load(Ordering::SeqCst) > 0);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(exit.is_success());
    assert!(exit.services[0].failure.is_none());
    assert!(!exit.services[0].backend_cleanup_unconfirmed);
}

#[tokio::test]
async fn empty_and_finite_monitors_complete_without_external_stop() {
    for jobs in [
        Monitor::new(),
        Monitor::new().register(|_| {
            WorkerBuilder::new("finite")
                .backend(finite(&[1]))
                .build(|_: u32| async {})
        }),
    ] {
        let app = Application::builder()
            .service(
                ServiceSpec::new("monitor").lifetime(Lifetime::Finite),
                monitor(jobs),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        assert!(bounded(app.observer().wait()).await.unwrap().is_success());
    }
}

#[tokio::test]
async fn resident_monitor_shuts_down_on_application_stop() {
    let jobs = Monitor::new().register(|_| {
        WorkerBuilder::new("resident")
            .backend(MemoryStorage::<u32>::new())
            .build(|_: u32| async {})
    });
    let app = Application::builder()
        .service(ServiceSpec::new("monitor"), monitor(jobs))
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(app.observer().ready(Duration::from_secs(2)))
        .await
        .unwrap();
    app.stop();
    assert!(bounded(app.observer().wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn monitor_internal_error_is_reported_only_after_resident_sibling_ends() {
    let jobs = Monitor::new()
        .register(|_| {
            let mut backend = finite(&[]);
            backend.fail_next = true;
            WorkerBuilder::new("failed")
                .backend(backend)
                .build(|_: u32| async {})
        })
        .register(|_| {
            WorkerBuilder::new("resident")
                .backend(MemoryStorage::<u32>::new())
                .build(|_: u32| async {})
        });
    let app = Application::builder()
        .service(ServiceSpec::new("monitor"), monitor(jobs))
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(app.observer().ready(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        DrainObservation::Pending(_)
    ));
    app.stop();
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(!exit.is_success());
    assert!(exit.services[0].failure.is_some());
}

#[tokio::test]
async fn recovered_monitor_failure_does_not_certify_old_backend_close() {
    let closes = Arc::new(AtomicUsize::new(0));
    let counts = closes.clone();
    let jobs = Monitor::new()
        .register(move |restarts| {
            let mut backend = finite(&[]);
            backend.closes = counts.clone();
            backend.fail_next = restarts == 0;
            WorkerBuilder::new("restart")
                .backend(backend)
                .build(|_: u32| async {})
        })
        .should_restart(|context, _| context.restarts() < 1);
    let app = Application::builder()
        .service(
            ServiceSpec::new("monitor").lifetime(Lifetime::Finite),
            monitor(jobs),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = bounded(app.observer().wait()).await.unwrap();
    eprintln!(
        "recovered exit={exit:?}; closes={}",
        closes.load(Ordering::SeqCst)
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        1,
        "only replacement backend closed"
    );
    assert!(
        exit.services[0].backend_cleanup_unconfirmed,
        "old failed backend never closed; monitor Ok does not certify it"
    );
    assert!(!exit.is_success());
}
