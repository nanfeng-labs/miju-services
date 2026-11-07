//! Pending is an observation: keep the runtime alive until actual completion.

use miju_services::{App, BoxError, ShutdownProgress};
use std::time::Duration;

fn main() -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let handle = App::builder()
            .cleanup("flush", || async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            })
            .service_fn("embedded", |context| async move {
                context.ready();
                context.stopping().await;
                Ok(())
            })
            .build()?
            .start()?;
        handle.ready(Duration::from_secs(1)).await?;
        if let ShutdownProgress::Pending(snapshot) =
            handle.shutdown_within(Duration::from_millis(1)).await?
        {
            println!("Still owned: {:?}", snapshot.phase);
        }
        // The runtime remains owned here while cleanup continues.
        let report = handle.shutdown().await?;
        println!("Actually finished: success={}", report.is_success());
        Ok(())
    })
}
