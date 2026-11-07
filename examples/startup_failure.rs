//! A required initialization failure exits without waiting for the host signal.

use miju_services::{App, BoxError};
use std::{future::pending, io};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    // The application exits on this required service failure without waiting for a signal.
    App::builder()
        .service_fn("device", |_context| async {
            Err(io::Error::new(io::ErrorKind::ConnectionRefused, "device unavailable").into())
        })
        .build()?
        .run_until(pending::<Result<(), io::Error>>())
        .await?;
    Ok(())
}
