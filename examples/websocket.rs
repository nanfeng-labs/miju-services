//! Axum upgrades reserve managed session ownership before returning HTTP 101.

#[path = "support/http.rs"]
mod http;

#[path = "support/websocket.rs"]
mod websocket;
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use miju_services::{Application, BoxError, Health, ServiceSpec, WorkSpec, service_fn};
use std::time::Duration;
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let (address_tx, address_rx) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("websocket"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let router = Router::new()
                    .route("/ws", get(websocket::upgrade))
                    .with_state(context.work().clone());
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                let _ = address_tx.send(address);
                match context
                    .set_health(Health::Healthy)
                    .and_then(|()| context.ready())
                {
                    Ok(()) => {}
                    Err(_) if context.is_stopping() => {}
                    Err(error) => return Err(error.into()),
                }
                let report = server.wait().await;
                if !(matches!(report.outcome(), miju_services::WorkOutcome::Succeeded)
                    || (context.is_stopping()
                        && matches!(
                            report.outcome(),
                            miju_services::WorkOutcome::CancelledBeforeStart
                        )))
                {
                    return Err(std::io::Error::other(format!("HTTP aggregate: {report:?}")).into());
                }
                Ok::<(), BoxError>(())
            }),
        )
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = app.observer();
    let interaction = async {
        observer.ready(Duration::from_secs(5)).await?;
        let address = address_rx.await?;
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/ws")).await?;
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                "hello".into(),
            ))
            .await?;
        println!("echo: {:?}", socket.next().await.transpose()?);
        app.stop();
        // Read and flush the protocol Close acknowledgement during shutdown.
        while let Some(message) = socket.next().await {
            if message?.is_close() {
                socket.flush().await?;
                break;
            }
        }
        Ok::<(), BoxError>(())
    }
    .await;
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    interaction?;
    println!("budget: {:?}; exit: {:?}", budget?, exit?);
    Ok(())
}
