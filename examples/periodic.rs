//! One operation, one schedule; no queue and no retry policy.

use miju_services::{App, Result, periodic::every};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service(
        "device-status",
        every(Duration::from_secs(5), || async {
            println!("collect device status");
            Ok(())
        }),
    );
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
