//! Two bounded device rounds; next submission waits five seconds after final receipt.

#[path = "support/queue.rs"]
mod queue;
use miju::{
    layers::{WorkerBuilderExt, retry::RetryPolicy},
    prelude::{WorkerBuilder, WorkerContext},
};
use miju_services::{Application, BoxError, Lifetime, ServiceSpec, service_fn};
use queue::{Job, Queue, ReceiptLayer};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let (queue, backend) = Queue::new(1);
    let context = WorkerContext::new("jobs");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .layer(ReceiptLayer)
        .retry(RetryPolicy::retries(2))
        .build(|job: Job| async move {
            println!("device round {}", job.value);
            Ok::<u32, std::io::Error>(job.value)
        });
    let (send, receive) = tokio::sync::oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("jobs"),
            miju_services::miju::worker(context, jobs.stream()),
        )
        .service(
            ServiceSpec::new("producer")
                .depends_on("jobs")
                .lifetime(Lifetime::Finite),
            service_fn(move |context| async move {
                let results =
                    queue::produce(context, queue, "device-1".into(), 2, Duration::from_secs(5))
                        .await?;
                let _ = send.send(results);
                Ok::<(), BoxError>(())
            }),
        )
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let results = receive.await;
    let budget = app.shutdown().await;
    let exit = app.observer().wait().await;
    println!("rounds: {results:?}; budget: {budget:?}; exit: {exit:?}");
    results?;
    budget?;
    if !exit?.is_success() {
        return Err(std::io::Error::other("queue lifecycle failed").into());
    }
    Ok(())
}
