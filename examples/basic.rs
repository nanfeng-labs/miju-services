//! Register a function and run until Ctrl-C.

#[path = "support/basic.rs"]
mod basic;

use miju_services::{App, Result};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service("heartbeat", basic::heartbeat);
    // Returns after the service has stopped and everything it owned is released.
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
