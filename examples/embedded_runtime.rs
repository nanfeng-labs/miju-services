//! A timeout ends an observation, not the application: keep the runtime
//! alive until the report arrives.

use miju_services::{App, BoxError, Result};
use std::time::Duration;

fn main() -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut app = App::new();
        app.service_fn("embedded", |ctx| async move {
            ctx.defer("flush", async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<_, BoxError>(())
            })?;
            ctx.ready();
            ctx.stopping().await;
            Ok(())
        });
        let app = app.start()?;
        tokio::time::timeout(Duration::from_secs(1), app.ready()).await??;

        app.stop();
        if tokio::time::timeout(Duration::from_millis(1), app.wait())
            .await
            .is_err()
        {
            // Only this observation ended. Everything is still owned.
            for item in app.snapshot().outstanding() {
                println!("still owned: {item}");
            }
        }
        let report = app.wait().await?;
        println!("actually finished: {report}");
        Ok(())
    })
}
