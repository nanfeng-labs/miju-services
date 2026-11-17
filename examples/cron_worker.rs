//! Calendar scheduling is a miju-cron backend: no producer and no queue.

use miju::prelude::WorkerBuilder;
use miju_cron::{CronScheduler, Tick};
use miju_services::{App, Result, miju::worker};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let schedule: cron::Schedule = "0 */5 * * * *".parse()?;
    let mut app = App::new();
    app.service(
        "device-calendar",
        worker(move |seat| {
            WorkerBuilder::new(&seat)
                // Named after the service, so each tick has a stable task ID.
                .backend(CronScheduler::new(schedule).with_name(seat.name()))
                .build(|tick: Tick| async move {
                    println!("collect at {}", tick.get_timestamp());
                })
                .stream()
        }),
    );
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
