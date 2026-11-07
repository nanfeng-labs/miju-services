use miju_services::{BoxError, ServiceContext};
use std::time::Duration;

pub(crate) async fn heartbeat(context: ServiceContext) -> Result<(), BoxError> {
    context.ready();
    loop {
        tokio::select! {
            _ = context.stopping() => break,
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                println!("heartbeat");
            }
        }
    }
    Ok(())
}
