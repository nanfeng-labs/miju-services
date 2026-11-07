//! Start after a finite prerequisite and drain service-owned background work.

use miju_services::BoxError;

#[path = "support/managed_work.rs"]
mod managed_work;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = managed_work::assemble()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("{report:?}");
    Ok(())
}
