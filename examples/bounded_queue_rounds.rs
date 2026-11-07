//! Advanced composition: bounded queued+running admission and final receipts.
#[path = "support/queue.rs"]
mod queue;
use miju::prelude::WorkerBuilder;
use miju_services::{App, BoxError, miju::worker_from};
use queue::{Job, Queue, Receipts, Submission};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let (queue, backend) = Queue::new(16);
    let app = App::builder()
        .service(
            "device-jobs",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(backend)
                    .layer(Receipts)
                    .build(|job: Job| async move {
                        println!("Collect {}", job.target);
                    })
                    .stream()
            }),
        )
        .build()?
        .start()?;
    if let Submission::Accepted(receipt) = queue.submit("device-01") {
        println!("Final outcome: {:?}", receipt.await?);
    }
    tokio::signal::ctrl_c().await?;
    let report = app.shutdown().await?;
    println!("Stopped: success={}", report.is_success());
    Ok(())
}
