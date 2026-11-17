//! Supply business routes; the example adapter binds, reports ready after the
//! bind, and waits for requests, bodies and router clones to be released.

#[path = "support/http.rs"]
mod http;

use axum::{Router, routing::get};
use http::HttpService;
use miju_services::{App, Result};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let router = Router::new().route("/", get(|| async { "Hello from miju-services" }));
    let mut app = App::new();
    app.service("http", HttpService::new("127.0.0.1:3000".parse()?, router));
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
