//! Observe early failure and choose an explicit second-signal force-exit policy.

#[path = "support/basic.rs"]
mod basic;
#[path = "support/operational_host.rs"]
mod operational_host;

use miju_services::{App, BoxError};
use operational_host::HostOutcome;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    match operational_host::run(
        App::builder()
            .service_fn("heartbeat", basic::heartbeat)
            .build()?,
        tokio::signal::ctrl_c(),
        tokio::signal::ctrl_c(),
    )
    .await?
    {
        HostOutcome::Exited(report) => println!("{report:?}"),
        HostOutcome::ForceExit => {
            eprintln!("Second Ctrl-C: terminating the process without a cleanup certificate.");
            std::process::exit(130);
        }
    }
    Ok(())
}
