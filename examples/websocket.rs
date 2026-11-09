//! Bounded echo sessions share the application's stop and actual-drain contract.

#[path = "support/http.rs"]
mod http;
#[path = "support/websocket.rs"]
mod websocket;

use miju_services::{App, BoxError, service::ServiceSpec};
use websocket::WebSocketService;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    App::builder()
        .service(
            // 64 sessions plus the HTTP server's aggregate responsibility.
            ServiceSpec::new("websocket").max_work(65.try_into()?),
            WebSocketService::new("127.0.0.1:3001".parse()?),
        )
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
