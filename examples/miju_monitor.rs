//! Manage a Monitor as one lifecycle service; internal failures may be delayed.

use miju_core::{backend::memory::MemoryStorage, monitor::Monitor, worker::builder::WorkerBuilder};
use miju_services::{Application, BoxError, ServiceSpec, miju::monitor};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let jobs = Monitor::new().register(|_| {
        WorkerBuilder::new("maintenance")
            .backend(MemoryStorage::<u32>::new())
            .build(|value: u32| async move {
                println!("maintenance {value}");
            })
    });
    let app = Application::builder()
        .service(ServiceSpec::new("monitor"), monitor(jobs))
        .build()?
        .start()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let observer = app.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    // A real host waits for its own exit signal here after successful readiness.
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    println!("shutdown: {budget:?}; exit: {exit:?}");
    ready?;
    budget?;
    let exit = exit?;
    if !exit.is_success() {
        return Err(std::io::Error::other("monitor lifecycle failed; inspect report").into());
    }
    Ok(())
}
