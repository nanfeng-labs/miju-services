use miju_services::{App, BoxError, BuildError, ServiceContext};
use std::time::Duration;

async fn heartbeat(context: ServiceContext) -> Result<(), BoxError> {
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

pub(crate) fn assemble() -> Result<App, BuildError> {
    App::builder().service_fn("heartbeat", heartbeat).build()
}
