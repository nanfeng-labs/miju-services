//! HTTP readiness and actual request/resource drain on an application-owned runtime.

#[path = "support/http.rs"]
mod http;
use axum::{Router, http::StatusCode, routing::get};
use miju_services::{
    AppObserver, Application, BoxError, Health, ServiceSpec, SpawnError, WorkCompletion,
    WorkOutcome, WorkSpec, service_fn,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

/// Build the public readiness endpoint from the read-only application observer.
///
/// The endpoint intentionally reflects the observer snapshot at request time:
/// startup and stopping return `503`, while a fully healthy running application
/// returns `200`.
pub(crate) fn readiness_router(observer: AppObserver) -> Router {
    Router::new().route(
        "/ready",
        get(move || {
            let observer = observer.clone();
            async move {
                if observer.snapshot().ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    )
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let (send, address) = tokio::sync::oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("http"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let addr = listener.local_addr()?;
                let observer = context.observer();
                let router = readiness_router(observer)
                    .route("/work", get(|| async { "device status saved" }));
                let server = match context
                    .work()
                    .spawn(WorkSpec::aggregate("http-server"), move |work| {
                        http::serve_http(listener, router, work)
                    }) {
                    Ok(server) => server,
                    Err(SpawnError::Closed) if context.is_stopping() => {
                        return Ok::<(), BoxError>(());
                    }
                    Err(e) => return Err(e.into()),
                };
                match context
                    .set_health(Health::Healthy)
                    .and_then(|()| context.ready())
                {
                    Ok(()) => {}
                    Err(_) if context.is_stopping() => {}
                    Err(e) => return Err(e.into()),
                }
                let _ = send.send(addr);
                let report = server.wait().await;
                match (report.completion(), report.outcome()) {
                    (WorkCompletion::Confirmed, WorkOutcome::Succeeded) => Ok(()),
                    (WorkCompletion::Confirmed, WorkOutcome::CancelledBeforeStart)
                        if context.is_stopping() =>
                    {
                        Ok(())
                    }
                    _ => Err(std::io::Error::other(format!(
                        "HTTP did not confirm drain: {report:?}"
                    ))
                    .into()),
                }
            }),
        )
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = app.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    let request = async {
        let addr = address.await?;
        for path in ["/ready", "/work"] {
            let mut stream = TcpStream::connect(addr).await?;
            let request =
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await?;
            let mut response = String::new();
            stream.read_to_string(&mut response).await?;
            if !response.starts_with("HTTP/1.1 200") {
                return Err(std::io::Error::other(format!("{path} failed: {response}")).into());
            }
            println!("{path}: {response}");
        }
        Ok::<(), BoxError>(())
    }
    .await;
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    println!("{budget:?}\n{exit:?}");
    ready?;
    request?;
    budget?;
    if !exit?.is_success() {
        return Err(std::io::Error::other("HTTP lifecycle failed").into());
    }
    Ok(())
}
