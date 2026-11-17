//! Bounded echo sessions under the application's stop and release contract.

#[path = "support/http.rs"]
mod http;
#[path = "support/websocket.rs"]
mod websocket;

use miju_services::{App, Result};
use websocket::WebSocketService;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    // At most 64 sessions; the 65th upgrade request is answered 503.
    app.service(
        "websocket",
        WebSocketService::new("127.0.0.1:3001".parse()?, 64),
    );
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
