//! miju constructs and executes the worker; App owns its service lifecycle.
use miju::prelude::{MemoryStorage, TaskSink, WorkerBuilder};
use miju_services::{App, BoxError, miju::worker_from};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let mut jobs = MemoryStorage::new();
    jobs.push("device-01".to_owned()).await?;
    let report = App::builder()
        .service(
            "devices",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(jobs)
                    .build(read_device)
                    .stream()
            }),
        )
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("Stopped: success={}", report.is_success());
    Ok(())
}

async fn read_device(device: String) -> Result<(), BoxError> {
    println!("Read status for {device}");
    Ok(())
}
