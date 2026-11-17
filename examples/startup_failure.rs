//! A required initialization failure ends the application without a signal.

use miju_services::{App, Result};
use std::io;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service_fn("device", |_ctx| async {
        Err(io::Error::new(io::ErrorKind::ConnectionRefused, "device unavailable").into())
    });
    // No stop source: the failure itself ends the run, and the error carries the report.
    app.run().await?;
    Ok(())
}
