//! Calendar source directly feeding a worker; no periodic producer or memory queue.
//! UTC calendar semantics: prefetched ticks can be delivered late under backpressure.
//! This is not FixedRateSkip; local-time DST behavior is outside this example.

use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
use miju_cron::{CronScheduler, Tick};
use miju_services::{Application, BoxError, ServiceSpec, miju::worker};
use std::{str::FromStr, time::Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let schedule = cron::Schedule::from_str("*/1 * * * * *")?;
    let context = WorkerContext::new("calendar");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(CronScheduler::new(schedule))
        .build(|tick: Tick| async move {
            println!("calendar tick {}", tick.get_timestamp());
            Ok::<(), std::io::Error>(())
        });
    let control = Application::builder()
        .service(ServiceSpec::new("calendar"), worker(context, jobs.stream()))
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = control.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    if ready.is_ok() {
        tokio::time::sleep(Duration::from_millis(2100)).await;
    }
    let budget = control.shutdown().await;
    let exit = observer.wait().await;
    println!("budget={budget:?}; exit={exit:?}");
    ready?;
    budget?;
    if !exit?.is_success() {
        return Err(std::io::Error::other("calendar lifecycle failed").into());
    }
    Ok(())
}
