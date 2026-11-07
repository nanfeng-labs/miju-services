//! One operation, one schedule; no queue or job retry policy is needed.
use miju_services::{App, BoxError, periodic};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = App::builder()
        .service(
            "device-status",
            periodic::fixed_delay(Duration::from_secs(5), || async {
                println!("Collect device status");
                Ok(())
            })?,
        )
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("Stopped: success={}", report.is_success());
    Ok(())
}
