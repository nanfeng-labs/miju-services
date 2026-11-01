//! A later required startup failure rolls back live HTTP, WebSocket and UDP work.

#[path = "../examples/support/http.rs"]
mod http;

#[path = "../examples/support/udp.rs"]
mod udp;

#[path = "../examples/support/websocket.rs"]
mod websocket;
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use miju_services::{
    Application, BoxError, DrainObservation, Health, ServiceSpec, WorkCompletion, WorkOutcome,
    WorkSpec, service_fn,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

async fn bounded<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("protocol rollback timed out")
}

#[tokio::test]
async fn later_startup_failure_waits_for_all_live_protocols_before_provider_cleanup() {
    let cleanup = Arc::new(Mutex::new(Vec::new()));
    let provider_cleanup = cleanup.clone();
    let web_cleanup = cleanup.clone();
    let udp_cleanup = cleanup.clone();
    let (web_bound, web_addr) = oneshot::channel();
    let (udp_bound, udp_addr) = oneshot::channel();
    let http_entered = CancellationToken::new();
    let http_release = CancellationToken::new();
    let http_signal = http_entered.clone();
    let http_gate = http_release.clone();
    let udp_entered = CancellationToken::new();
    let udp_release = CancellationToken::new();
    let udp_signal = udp_entered.clone();
    let udp_gate = udp_release.clone();
    let fail = CancellationToken::new();
    let fail_gate = fail.clone();
    let counters = Arc::new(udp::UdpCounters::default());
    let app = Application::builder()
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context| async move {
                context.defer_cleanup("provider", move || async move {
                    provider_cleanup.lock().unwrap().push("provider");
                    Ok::<(), BoxError>(())
                })?;
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("web").depends_on("provider"),
            service_fn(move |context| async move {
                context.defer_cleanup("web", move || async move {
                    web_cleanup.lock().unwrap().push("web");
                    Ok::<(), BoxError>(())
                })?;
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let addr = listener.local_addr()?;
                let router = Router::new()
                    .route("/ws", get(websocket::upgrade))
                    .with_state(context.work().clone())
                    .route(
                        "/held",
                        get(move || {
                            let signal = http_signal.clone();
                            let gate = http_gate.clone();
                            async move {
                                signal.cancel();
                                gate.cancelled().await;
                                "http-finished"
                            }
                        }),
                    );
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                context.set_health(Health::Healthy)?;
                context.ready()?;
                web_bound.send(addr).unwrap();
                let report = server.wait().await;
                assert_eq!(report.completion(), WorkCompletion::Confirmed);
                assert!(matches!(report.outcome(), WorkOutcome::Succeeded));
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("udp").depends_on("provider"),
            service_fn(move |context| async move {
                context.defer_cleanup("udp", move || async move {
                    udp_cleanup.lock().unwrap().push("udp");
                    Ok::<(), BoxError>(())
                })?;
                udp::serve(
                    context,
                    "127.0.0.1:0".parse().unwrap(),
                    udp_bound,
                    counters,
                    move |packet, peer, socket, _| {
                        let signal = udp_signal.clone();
                        let gate = udp_gate.clone();
                        async move {
                            signal.cancel();
                            gate.cancelled().await;
                            socket.send_to(&packet, peer).await?;
                            Ok(())
                        }
                    },
                )
                .await
            }),
        )
        .service(
            ServiceSpec::new("late-failure")
                .depends_on("web")
                .depends_on("udp"),
            service_fn(move |_| async move {
                fail_gate.cancelled().await;
                Err::<(), BoxError>(std::io::Error::other("late initialization failed").into())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let web_addr = bounded(web_addr).await.unwrap();
    let udp_addr = bounded(udp_addr).await.unwrap();
    let mut request = bounded(TcpStream::connect(web_addr)).await.unwrap();
    request
        .write_all(b"GET /held HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    bounded(http_entered.cancelled()).await;
    let (mut ws, response) = bounded(tokio_tungstenite::connect_async(format!(
        "ws://{web_addr}/ws"
    )))
    .await
    .unwrap();
    assert_eq!(response.status(), 101);
    ws.send(tokio_tungstenite::tungstenite::Message::Text("live".into()))
        .await
        .unwrap();
    assert_eq!(
        bounded(ws.next())
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
        "live"
    );
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.send_to(b"udp-held", udp_addr).await.unwrap();
    bounded(udp_entered.cancelled()).await;
    assert!(!app.observer().snapshot().ready);
    fail.cancel();
    bounded(async {
        while !app
            .observer()
            .snapshot()
            .services
            .iter()
            .any(|s| s.id.as_str() == "late-failure" && s.failure.is_some())
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        DrainObservation::Pending(_)
    ));
    assert!(cleanup.lock().unwrap().is_empty());
    assert!(
        !app.observer()
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .stopping
    );
    assert!(bounded(ws.next()).await.unwrap().unwrap().is_close());
    ws.flush().await.unwrap();
    http_release.cancel();
    let mut response = String::new();
    bounded(request.read_to_string(&mut response))
        .await
        .unwrap();
    assert!(response.ends_with("http-finished"));
    drop(request);
    // UDP is still a live responsibility even after HTTP and WS have completed.
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        DrainObservation::Pending(_)
    ));
    assert!(cleanup.lock().unwrap().is_empty());
    udp_release.cancel();
    let mut packet = [0; 32];
    let (len, _) = bounded(socket.recv_from(&mut packet)).await.unwrap();
    assert_eq!(&packet[..len], b"udp-held");
    let exit = bounded(app.observer().wait()).await.unwrap();
    assert!(exit.is_drained());
    assert!(!exit.is_success());
    for service in &exit.services {
        assert!(service.work.active.is_empty());
        if service.id.as_str() != "late-failure" {
            assert!(
                service.failure.is_none(),
                "unexpected protocol failure: {service:?}"
            );
        }
    }
    let cleanup = cleanup.lock().unwrap();
    assert_eq!(cleanup.len(), 3);
    assert_eq!(cleanup.last(), Some(&"provider"));
}
