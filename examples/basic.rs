//! Register an ordinary service and run until Ctrl-C.

#[path = "support/basic.rs"]
mod basic;

use miju_services::BoxError;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = basic::assemble()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("{report:?}");
    Ok(())
}
