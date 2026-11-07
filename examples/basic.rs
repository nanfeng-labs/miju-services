//! Register an ordinary service and run until Ctrl-C.

#[path = "support/basic.rs"]
mod heartbeat;

use heartbeat::heartbeat;
use miju_services::{App, BoxError, BuildError};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = assemble()?.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report:?}");
    Ok(())
}

/// Keep the executable's composition visible and reusable by caller tests.
pub(crate) fn assemble() -> Result<App, BuildError> {
    App::builder().service_fn("heartbeat", heartbeat).build()
}
