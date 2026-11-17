//! Advanced composition: application-owned admission covering queued and
//! running work, and a final receipt for each accepted job.

#[path = "support/queue.rs"]
mod queue;

use miju::prelude::WorkerBuilder;
use miju_services::{App, Result, miju::worker};
use queue::{Job, Queue, Receipts, Submission};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let (queue, backend) = Queue::new(16);
    let mut app = App::new();
    app.service(
        "device-jobs",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                .backend(backend)
                // Outside any retry layer, so a receipt reports the final result.
                .layer(Receipts)
                .build(|job: Job| async move {
                    println!("collect {}", job.target);
                })
                .stream()
        }),
    );
    let app = app.start()?;
    if let Submission::Accepted(receipt) = queue.submit("device-01") {
        println!("final outcome: {:?}", receipt.await?);
    }
    tokio::signal::ctrl_c().await?;
    let report = app.shutdown().await?;
    println!("{report}");
    Ok(())
}
