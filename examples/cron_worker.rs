//! Calendar scheduling is a miju-cron backend, without a producer and queue.
use miju::prelude::WorkerBuilder;
use miju_cron::{CronScheduler, Tick};
use miju_services::{App, BoxError, miju::worker_from};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let schedule: cron::Schedule = "0 */5 * * * *".parse()?;
    let report = App::builder()
        .service(
            "device-calendar",
            worker_from(move |context| {
                WorkerBuilder::new(context)
                    .backend(CronScheduler::new(schedule))
                    .build(|tick: Tick| async move {
                        println!("Collect at {}", tick.get_timestamp());
                    })
                    .stream()
            }),
        )
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("Stopped: success={}", report.is_success());
    Ok(())
}
