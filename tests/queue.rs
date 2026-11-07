//! Application admission, retry-chain receipts and explicit queued-job cancellation.
#![cfg(feature = "miju")]
#[path = "../examples/support/queue.rs"]
mod queue;

use miju::{
    layers::retry::RetryPolicy,
    prelude::{WorkerBuilder, WorkerBuilderExt},
};
use miju_services::{App, BoxError, miju::worker_from};
use queue::{Job, Queue, Receipt, Receipts, Submission};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

fn accepted(submission: Submission) -> oneshot::Receiver<Receipt> {
    match submission {
        Submission::Accepted(receipt) => receipt,
        _ => panic!("submission refused"),
    }
}

#[tokio::test]
async fn capacity_includes_running_and_same_target_skips_until_actual_release() {
    let (queue, backend) = Queue::new(2);
    let first = accepted(queue.submit("first"));
    assert!(matches!(queue.submit("first"), Submission::Busy));
    let second = accepted(queue.submit("second"));
    assert!(
        matches!(queue.submit("third"), Submission::Busy),
        "queued work lost its admission permit"
    );
    let (entered, started) = oneshot::channel();
    let entered = Arc::new(Mutex::new(Some(entered)));
    let (release, gate) = oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let app = App::builder()
        .service(
            "jobs",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .layer(Receipts)
                    .layer(tower::limit::ConcurrencyLimitLayer::new(1))
                    .build(move |_: Job| {
                        let entered = entered.lock().unwrap().take();
                        let gate = gate.lock().unwrap().take();
                        async move {
                            if let Some(entered) = entered {
                                entered.send(()).unwrap();
                            }
                            if let Some(gate) = gate {
                                gate.await.unwrap();
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
    started.await.unwrap();
    assert!(
        matches!(queue.submit("third"), Submission::Busy),
        "running job released its admission permit early"
    );
    assert!(matches!(queue.submit("first"), Submission::Busy));
    app.stop();
    release.send(()).unwrap();
    assert!(app.wait().await.unwrap().is_success());
    assert_eq!(first.await.unwrap(), Receipt::Success);
    assert_eq!(second.await.unwrap(), Receipt::Cancelled);
    assert!(matches!(queue.submit("new"), Submission::Closed));
}

#[tokio::test]
async fn receipt_observes_final_retry_result_not_first_attempt() {
    let (queue, backend) = Queue::new(1);
    let receipt = accepted(queue.submit("retry-target"));
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let app = App::builder()
        .service(
            "jobs",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .layer(Receipts)
                    .retry(RetryPolicy::retries(1))
                    .build(move |_: Job| {
                        let index = seen.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if index == 0 {
                                Err::<(), BoxError>(io::Error::other("first-attempt").into())
                            } else {
                                Ok(())
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
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), receipt)
            .await
            .unwrap()
            .unwrap(),
        Receipt::Success
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn business_failure_has_a_final_failed_receipt() {
    let (queue, backend) = Queue::new(1);
    let receipt = accepted(queue.submit("failed"));
    let app = App::builder()
        .service(
            "jobs",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .layer(Receipts)
                    .build(|job: Job| async move {
                        assert_eq!(job.target, "failed");
                        Err::<(), _>(io::Error::other("business-failure"))
                    })
                    .stream()
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert_eq!(
        receipt.await.unwrap(),
        Receipt::Failed("business-failure".into())
    );
    let report = app.shutdown().await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.snapshot.services[0].execution_errors, 1);
}

#[tokio::test]
async fn final_receipt_follows_actual_inner_future_release() {
    use miju::backend::Backend;
    use miju::prelude::WorkerContext;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::AtomicBool,
        task::{Context, Poll},
    };
    use tower::{Layer, Service};
    struct ReadyOnPoll(Arc<AtomicBool>);
    impl Future for ReadyOnPoll {
        type Output = Result<(), BoxError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }
    impl Drop for ReadyOnPoll {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (queue, mut backend) = Queue::new(1);
    let receipt = accepted(queue.submit("drop-check"));
    let context = WorkerContext::new("check");
    let task = std::future::poll_fn(|cx| backend.poll_next(cx, &context))
        .await
        .unwrap()
        .unwrap();
    let released = Arc::new(AtomicBool::new(false));
    let marker = released.clone();
    let mut service = Receipts.layer(tower::service_fn(move |_| ReadyOnPoll(marker.clone())));
    let mut future = service.call(task);
    std::future::poll_fn(|cx| Pin::new(&mut future).poll(cx))
        .await
        .unwrap();
    assert!(
        released.load(Ordering::SeqCst),
        "receipt published before execution future release"
    );
    assert_eq!(receipt.await.unwrap(), Receipt::Success);
}
