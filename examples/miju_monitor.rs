//! A Monitor keeps its grouped restart policy. Jobs run on the seat's executor
//! stay owned by the service across restarts, so stop waits for them.

use miju::prelude::{MemoryStorage, Monitor, ParallelizeExt, WorkerBuilder};
use miju_services::{App, Result, miju::monitor};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service(
        "fleet",
        monitor(|seat| {
            Monitor::new()
                .should_restart(|worker, _error| worker.restarts() < 3)
                .register(move |_restarts| {
                    WorkerBuilder::new("fleet-worker")
                        .backend(MemoryStorage::new())
                        .parallelize(seat.executor())
                        .build(|device: String| async move {
                            println!("read status for {device}");
                        })
                })
        }),
    );
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
