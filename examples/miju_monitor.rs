//! Use a Monitor when its grouped restart and delayed failure semantics are intended.
//! Restart history leaves cleanup unconfirmed; use separate worker services when
//! each worker's failure must stop and drain independently.
use miju::prelude::{MemoryStorage, Monitor, WorkerBuilder};
use miju_services::{App, BoxError, miju::monitor};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let workers = Monitor::new().register(|_| {
        WorkerBuilder::new("devices")
            .backend(MemoryStorage::new())
            .build(|device: String| async move {
                println!("Read status for {device}");
            })
    });
    let report = App::builder()
        .service("workers", monitor(workers))
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("Stopped: success={}", report.is_success());
    Ok(())
}
