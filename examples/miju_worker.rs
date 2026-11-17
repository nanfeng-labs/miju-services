//! miju builds and executes the worker; the application owns its lifecycle.

use miju::prelude::{MemoryStorage, TaskSink, WorkerBuilder};
use miju_services::{App, BoxError, Result, miju::worker};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut jobs = MemoryStorage::new();
    jobs.push("device-01".to_owned()).await?;
    let mut app = App::new();
    app.service(
        "devices",
        worker(move |seat| {
            // The seat is the identity the application stops the worker through.
            WorkerBuilder::new(&seat)
                .backend(jobs)
                .build(read_device)
                .stream()
        }),
    );
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}

async fn read_device(device: String) -> std::result::Result<(), BoxError> {
    println!("read status for {device}");
    Ok(())
}
