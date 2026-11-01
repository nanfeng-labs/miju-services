//! Bounded receipts through the actual product DAG and miju retry middleware.
#![cfg(feature = "miju")]
#[path = "../examples/support/queue.rs"]
mod queue;
use miju::{
    layers::{WorkerBuilderExt, retry::RetryPolicy},
    prelude::{WorkerBuilder, WorkerContext},
};
use miju_services::{Application, BoxError, Lifetime, ServiceSpec, service_fn};
use queue::{Job, Queue, Receipt, ReceiptLayer, Submission};
use std::{
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn accepted(submission: Submission) -> tokio::sync::oneshot::Receiver<Receipt> {
    match submission {
        Submission::Accepted(receipt) => receipt,
        other => panic!("not accepted: {other:?}"),
    }
}

#[tokio::test]
async fn live_submission_receipt_covers_all_retries_and_exhaustion() {
    for exhausted in [false, true] {
        let (queue, backend) = Queue::new(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let record = attempts.clone();
        let context = WorkerContext::new("retry");
        let jobs = WorkerBuilder::new(context.clone())
            .backend(backend)
            .layer(ReceiptLayer)
            .retry(RetryPolicy::retries(2))
            .build(move |job: Job| {
                let n = record.fetch_add(1, Ordering::SeqCst);
                async move {
                    if exhausted || n < 2 {
                        Err(std::io::Error::other("retry failed"))
                    } else {
                        Ok(job.value)
                    }
                }
            });
        let app = Application::builder()
            .service(
                ServiceSpec::new("worker"),
                miju_services::miju::worker(context, jobs.stream()),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.observer().ready(Duration::from_secs(2)).await.unwrap();
        let receipt = accepted(queue.submit("device", 7, pending::<()>()).await.unwrap());
        let value = tokio::time::timeout(Duration::from_secs(2), receipt)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value,
            if exhausted {
                Receipt::Failed("retry failed".into())
            } else {
                Receipt::Success(7)
            }
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        app.shutdown().await.unwrap();
        app.observer().wait().await.unwrap();
        assert_eq!(queue.available(), 1);
    }
}

#[tokio::test]
async fn queued_duplicate_and_cancelled_permit_wait_keep_capacity_owned() {
    let (queue, backend) = Queue::new(1);
    let first = accepted(queue.submit("one", 1, pending::<()>()).await.unwrap());
    assert_eq!(queue.available(), 0);
    assert!(matches!(
        queue.submit("one", 2, pending::<()>()).await.unwrap(),
        Submission::Skipped
    ));
    assert!(matches!(
        queue.submit("two", 2, async {}).await.unwrap(),
        Submission::Stopped
    ));
    drop(first);
    assert_eq!(queue.available(), 0);
    drop(backend);
    assert_eq!(queue.available(), 1);
    assert!(matches!(
        queue.submit("two", 2, pending::<()>()).await,
        Err(queue::SubmitError::Push(_))
    ));
    assert_eq!(queue.available(), 1);
}

#[tokio::test]
async fn queued_job_drop_reports_cancelled_and_releases_identity() {
    let (queue, backend) = Queue::new(1);
    let receipt = accepted(queue.submit("one", 1, pending::<()>()).await.unwrap());
    drop(backend);
    assert_eq!(receipt.await.unwrap(), Receipt::Cancelled);
    assert_eq!(queue.available(), 1);
    assert!(matches!(
        queue.submit("one", 2, pending::<()>()).await,
        Err(queue::SubmitError::Push(_))
    ));
    assert_eq!(queue.available(), 1);
}

#[tokio::test]
async fn retry_slot_and_target_survive_receipt_waiter_drop() {
    let (queue, backend) = Queue::new(1);
    let (started, start) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let (release, finish) = tokio::sync::oneshot::channel();
    let finish = Arc::new(Mutex::new(Some(finish)));
    let attempts = Arc::new(AtomicUsize::new(0));
    let record = attempts.clone();
    let context = WorkerContext::new("held-retry");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(ReceiptLayer)
        .retry(RetryPolicy::retries(1))
        .build(move |job: Job| {
            let n = record.fetch_add(1, Ordering::SeqCst);
            let started = started.clone();
            let finish = finish.clone();
            async move {
                if n == 0 {
                    Err(std::io::Error::other("first attempt"))
                } else {
                    let sender = started.lock().unwrap().take().unwrap();
                    let receiver = finish.lock().unwrap().take().unwrap();
                    sender.send(()).unwrap();
                    receiver.await.unwrap();
                    Ok(job.value)
                }
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("worker"),
            miju_services::miju::worker(context, jobs.stream()),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(2)).await.unwrap();
    let receipt = accepted(queue.submit("one", 1, pending::<()>()).await.unwrap());
    start.await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    drop(receipt);
    assert_eq!(queue.available(), 0);
    assert!(matches!(
        queue.submit("one", 2, pending::<()>()).await.unwrap(),
        Submission::Skipped
    ));
    release.send(()).unwrap();
    app.shutdown().await.unwrap();
    assert!(app.observer().wait().await.unwrap().is_success());
    assert_eq!(queue.available(), 1);
}

#[tokio::test(start_paused = true)]
async fn producer_dag_waits_final_receipt_then_five_seconds_before_next_submit() {
    let origin = tokio::time::Instant::now();
    let (queue, backend) = Queue::new(1);
    let inspect = queue.clone();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let record = starts.clone();
    let context = WorkerContext::new("jobs");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(ReceiptLayer)
        .retry(RetryPolicy::retries(1))
        .build(move |job: Job| {
            record.lock().unwrap().push(origin.elapsed());
            async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok::<u32, std::io::Error>(job.value)
            }
        });
    let (send, receive) = tokio::sync::oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("worker"),
            miju_services::miju::worker(context, jobs.stream()),
        )
        .service(
            ServiceSpec::new("producer")
                .depends_on("worker")
                .lifetime(Lifetime::Finite),
            service_fn(move |context| async move {
                let receipts =
                    queue::produce(context, queue, "device".into(), 2, Duration::from_secs(5))
                        .await?;
                send.send(receipts).unwrap();
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(
        receive.await.unwrap(),
        [Receipt::Success(1), Receipt::Success(2)]
    );
    app.shutdown().await.unwrap();
    assert!(app.observer().wait().await.unwrap().is_success());
    assert_eq!(
        *starts.lock().unwrap(),
        [Duration::ZERO, Duration::from_secs(7)]
    );
    assert_eq!(inspect.available(), 1);
}

#[tokio::test]
async fn application_stop_waits_producers_accepted_round_before_worker_stop() {
    let (queue, backend) = Queue::new(1);
    let inspect = queue.clone();
    let (started, start) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let (release, finish) = tokio::sync::oneshot::channel();
    let finish = Arc::new(Mutex::new(Some(finish)));
    let context = WorkerContext::new("jobs");
    let inspect_worker = context.clone();
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(ReceiptLayer)
        .build(move |job: Job| {
            let sender = started.lock().unwrap().take().unwrap();
            let receiver = finish.lock().unwrap().take().unwrap();
            async move {
                sender.send(()).unwrap();
                receiver.await.unwrap();
                Ok::<u32, std::io::Error>(job.value)
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("worker"),
            miju_services::miju::worker(context, jobs.stream()),
        )
        .service(
            ServiceSpec::new("producer")
                .depends_on("worker")
                .lifetime(Lifetime::Finite),
            service_fn(move |context| async move {
                let results =
                    queue::produce(context, queue, "device".into(), 9, Duration::from_secs(5))
                        .await?;
                assert_eq!(results, [Receipt::Success(1)]);
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    start.await.unwrap();
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert!(!inspect_worker.is_stopped());
    assert_eq!(inspect.available(), 0);
    release.send(()).unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success());
    assert_eq!(inspect.available(), 1);
}

#[tokio::test(start_paused = true)]
async fn forced_execution_cancel_releases_body_before_cancelled_receipt() {
    struct Guard(Arc<AtomicUsize>);

    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (queue, backend) = Queue::new(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let record = dropped.clone();
    let (started, start) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let context = WorkerContext::new("cancel");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(ReceiptLayer)
        .build(move |_: Job| {
            let guard = Guard(record.clone());
            let sender = started.lock().unwrap().take().unwrap();
            async move {
                let _guard = guard;
                sender.send(()).unwrap();
                pending::<Result<u32, std::io::Error>>().await
            }
        });
    let app = Application::builder()
        .options(miju_services::AppOptions {
            shutdown_grace: Duration::from_secs(1),
            abort_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .service(
            ServiceSpec::new("worker"),
            miju_services::miju::worker(context, jobs.stream()),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    let receipt = accepted(queue.submit("device", 1, pending::<()>()).await.unwrap());
    start.await.unwrap();
    app.stop();
    assert_eq!(receipt.await.unwrap(), Receipt::Cancelled);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(!app.observer().wait().await.unwrap().is_success());
    assert_eq!(queue.available(), 1);
}
