//! Supply business routes; the example adapter owns binding and actual drain.

#[path = "support/http.rs"]
mod http;

use axum::{Router, routing::get};
use http::HttpService;
use miju_services::{App, BoxError};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let router = Router::new().route("/", get(|| async { "Hello from miju-services" }));
    App::builder()
        .service("http", HttpService::new("127.0.0.1:3000".parse()?, router))
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
