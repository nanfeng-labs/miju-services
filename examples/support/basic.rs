use miju_services::{Context, Result};
use std::time::Duration;

pub(crate) async fn heartbeat(ctx: Context) -> Result {
    ctx.ready();
    loop {
        tokio::select! {
            _ = ctx.stopping() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => println!("heartbeat"),
        }
    }
}
