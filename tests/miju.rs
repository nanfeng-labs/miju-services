//! Actual miju backends, events, shutdown, and unconfirmed close responsibility.
#![cfg(feature = "miju")]

use miju::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    prelude::{Monitor, Task, WorkerBuilder, WorkerContext},
    task::task_id::RandomId,
};
use miju_services::{
    App, ShutdownProgress,
    app::AppOptions,
    miju::{monitor, worker_from},
    service::{Lifetime, ServiceSpec},
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::oneshot;

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("miju path stalled")
}

struct Source {
    jobs: VecDeque<Task<u32>>,
    close: oneshot::Receiver<()>,
    closing: Option<oneshot::Sender<()>>,
    released: Arc<AtomicBool>,
    fail: bool,
}

impl Backend for Source {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, io::Error>>> {
        if std::mem::take(&mut self.fail) {
            Poll::Ready(Some(Err(io::Error::other("backend-failure"))))
        } else {
            Poll::Ready(self.jobs.pop_front().map(Ok))
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), io::Error>> {
        if let Some(send) = self.closing.take() {
            let _ = send.send(());
        }
        match Pin::new(&mut self.close).poll(cx) {
            Poll::Ready(_) => {
                self.released.store(true, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl BackendConfig for Source {
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

fn source(jobs: &[u32]) -> (Source, oneshot::Sender<()>, oneshot::Receiver<()>) {
    let (release, close) = oneshot::channel();
    let (closing, entered) = oneshot::channel();
    (
        Source {
            jobs: jobs.iter().copied().map(Task::new).collect(),
            close,
            closing: Some(closing),
            released: Arc::default(),
            fail: false,
        },
        release,
        entered,
    )
}

fn finite() -> ServiceSpec {
    ServiceSpec::new("jobs").lifetime(Lifetime::Finite)
}

#[tokio::test]
async fn factory_runs_once_with_service_name_and_eof_waits_for_backend_close() {
    let (backend, release, closing) = source(&[2, 3]);
    let closed = backend.released.clone();
    let cleaned = closed.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let sum = Arc::new(AtomicUsize::new(0));
    let output = sum.clone();
    let app = App::builder()
        .cleanup("after-backend", move || async move {
            assert!(cleaned.load(Ordering::SeqCst));
            Ok(())
        })
        .service(
            finite(),
            worker_from(move |context| {
                assert_eq!(context.name(), "jobs");
                invoked.fetch_add(1, Ordering::SeqCst);
                WorkerBuilder::new(context)
                    .backend(backend)
                    .build(move |value: u32| {
                        let output = output.clone();
                        async move {
                            output.fetch_add(value as usize, Ordering::SeqCst);
                        }
                    })
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(closing).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(sum.load(Ordering::SeqCst), 5);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), app.wait())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.snapshot.services[0].work.unconfirmed, 0);
}

#[tokio::test]
async fn job_errors_are_observable_and_do_not_fail_a_finite_lifecycle() {
    let (backend, release, _) = source(&[1, 2, 3]);
    release.send(()).unwrap();
    let app = App::builder()
        .options(AppOptions {
            recent_results: 2,
            ..Default::default()
        })
        .service(
            finite(),
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .build(|_: u32| async { Err::<(), _>(io::Error::other("job-failure")) })
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    let jobs = &report.snapshot.services[0];
    assert_eq!(jobs.execution_errors, 3);
    assert_eq!(jobs.recent_execution_errors.len(), 2);
    assert!(jobs.recent_execution_errors[0].source().is_some());
    assert!(
        jobs.recent_execution_errors[0]
            .to_string()
            .contains("job-failure")
    );
}

#[tokio::test]
async fn backend_error_retains_unconfirmed_work_and_prevents_cleanup() {
    let (mut backend, _release, _) = source(&[]);
    backend.fail = true;
    let cleaned = Arc::new(AtomicBool::new(false));
    let close = cleaned.clone();
    let app = App::builder()
        .cleanup("resource", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service(
            finite(),
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .build(|_: u32| async {})
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(async {
        while app.snapshot().services[0].failures.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let progress = bounded(app.shutdown_within(Duration::from_millis(20)))
        .await
        .unwrap();
    let ShutdownProgress::Pending(snapshot) = progress else {
        panic!("backend failure falsely certified cleanup")
    };
    assert_eq!(snapshot.services[0].work.unconfirmed, 1);
    assert!(!snapshot.services[0].failures.is_empty());
    assert!(!cleaned.load(Ordering::SeqCst));
    // The test runtime is the explicitly owned host; no final certificate is expected.
}

#[tokio::test]
async fn started_aggregate_survives_root_abort_until_original_close_finishes() {
    let (backend, release, closing) = source(&[]);
    let app = App::builder()
        .options(AppOptions {
            shutdown_grace: Duration::from_millis(5),
            abort_wait: Duration::from_millis(5),
            ..Default::default()
        })
        .service(
            finite(),
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .build(|_: u32| async {})
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(closing).await.unwrap();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(30))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert_eq!(app.snapshot().services[0].work.active.len(), 1);
    release.send(()).unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.snapshot.budget_exceeded);
    assert!(!report.is_success());
    assert!(report.snapshot.services[0].work.active.is_empty());
}

#[tokio::test]
async fn stop_drains_active_handler_and_leaves_queued_job_unstarted() {
    use miju::prelude::{MemoryStorage, TaskSink};
    let mut backend = MemoryStorage::new();
    backend.push(1_u32).await.unwrap();
    backend.push(2_u32).await.unwrap();
    let (entered, start) = oneshot::channel();
    let entered = Arc::new(Mutex::new(Some(entered)));
    let (release, finish) = oneshot::channel();
    let finish = Arc::new(Mutex::new(Some(finish)));
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let app = App::builder()
        .service(
            "jobs",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .layer(tower::limit::ConcurrencyLimitLayer::new(1))
                    .build(move |_: u32| {
                        let send = entered.lock().unwrap().take();
                        let finish = finish.lock().unwrap().take();
                        let seen = seen.clone();
                        async move {
                            seen.fetch_add(1, Ordering::SeqCst);
                            send.unwrap().send(()).unwrap();
                            finish.unwrap().await.unwrap();
                        }
                    })
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(start).await.unwrap();
    app.stop();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    release.send(()).unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn monitor_natural_completion_does_not_wait_for_a_signal() {
    let app = App::builder()
        .service(finite(), monitor(Monitor::new()))
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn idle_monitor_stops_through_the_original_run_future() {
    use miju::prelude::MemoryStorage;
    let group = Monitor::new().register(|_| {
        WorkerBuilder::new("worker")
            .backend(MemoryStorage::new())
            .build(|_: u32| async {})
    });
    let app = App::builder()
        .service("group", monitor(group))
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(app.ready(Duration::from_secs(1))).await.unwrap();
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
}

#[tokio::test]
async fn stop_before_start_is_forwarded_after_the_actual_start_event() {
    use futures_util::{StreamExt, stream};
    use miju::prelude::{Event, WorkerError};
    let (entered, entry) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let app = App::builder()
        .service(
            finite(),
            worker_from(move |mut control| {
                entered.send(()).unwrap();
                let stopping = control.clone();
                stream::once(async move {
                    gate.await.unwrap();
                    control.start()?;
                    Ok::<_, WorkerError>(Event::Start)
                })
                .chain(stream::once(async move {
                    assert!(
                        !stopping.is_running(),
                        "pending stop was never forwarded after Start"
                    );
                    Ok(Event::Exit)
                }))
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(entry).await.unwrap();
    app.stop();
    release.send(()).unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn monitor_restart_success_cannot_clear_previous_unconfirmed_backend() {
    let generations = Arc::new(AtomicUsize::new(0));
    let created = generations.clone();
    let group = Monitor::new()
        .should_restart(|context, _| context.restarts() == 0)
        .register(move |generation| {
            created.fetch_add(1, Ordering::SeqCst);
            let (mut backend, release, _) = source(&[]);
            backend.fail = generation == 0;
            release.send(()).unwrap();
            WorkerBuilder::new("restarted")
                .backend(backend)
                .build(|_: u32| async {})
        });
    let app = App::builder()
        .service(finite(), monitor(group))
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(async {
        loop {
            let snapshot = app.snapshot();
            if snapshot.services[0].work.unconfirmed != 0
                || snapshot.phase == miju_services::report::Phase::Terminated
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(generations.load(Ordering::SeqCst), 2);
    assert!(
        matches!(
            app.shutdown_within(Duration::ZERO).await.unwrap(),
            ShutdownProgress::Pending(_)
        ),
        "restart success falsely certified previous backend cleanup"
    );
}

#[tokio::test]
async fn cron_is_a_real_backend_and_stops_without_a_producer_queue() {
    use miju_cron::{CronScheduler, Tick};
    let schedule: cron::Schedule = "* * * * * *".parse().unwrap();
    let (send, receive) = oneshot::channel();
    let send = Arc::new(Mutex::new(Some(send)));
    let app = App::builder()
        .service(
            "calendar",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(CronScheduler::new(schedule))
                    .build(move |_: Tick| {
                        let send = send.lock().unwrap().take();
                        async move {
                            if let Some(send) = send {
                                let _ = send.send(());
                            }
                        }
                    })
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    bounded(receive).await.unwrap();
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
}
