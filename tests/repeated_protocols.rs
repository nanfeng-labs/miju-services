//! One hundred independent applications with real, in-flight protocol shutdown.

#[path = "../examples/support/http.rs"]
mod http;

#[path = "../examples/support/udp.rs"]
mod udp;

#[path = "../examples/support/websocket.rs"]
mod websocket;
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use miju_services::{
    Application, BoxError, Health, ServiceSpec, WorkCompletion, WorkOutcome, WorkSpec, service_fn,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Notify, oneshot},
};

async fn cycle(index: usize) -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let udp_entered = Arc::new(Notify::new());
    let udp_release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let clean = cleaned.clone();
    let (http_tx, http_rx) = oneshot::channel();
    let (udp_tx, udp_rx) = oneshot::channel();
    let http_entered = entered.clone();
    let http_release = release.clone();
    let packet_entered = udp_entered.clone();
    let packet_release = udp_release.clone();
    let counters = Arc::new(udp::UdpCounters::default());
    let udp_counters = counters.clone();
    let control = Application::builder()
        .service(
            ServiceSpec::new("resource"),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("close", move || async move {
                        clean.store(true, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("http-ws").depends_on("resource"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let router = Router::new()
                    .route("/ws", get(websocket::upgrade))
                    .with_state(context.work().clone())
                    .route(
                        "/inflight",
                        get(move || {
                            let entered = http_entered.clone();
                            let release = http_release.clone();
                            async move {
                                entered.notify_one();
                                release.notified().await;
                                "completed-body"
                            }
                        }),
                    );
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                let _ = http_tx.send(address);
                match context
                    .set_health(Health::Healthy)
                    .and_then(|()| context.ready())
                {
                    Ok(()) => {}
                    Err(_) if context.is_stopping() => {}
                    Err(e) => return Err(e.into()),
                }
                let report = server.wait().await;
                if report.completion() != WorkCompletion::Confirmed
                    || !matches!(report.outcome(), WorkOutcome::Succeeded)
                {
                    return Err(std::io::Error::other("HTTP not confirmed").into());
                }
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("udp").depends_on("resource"),
            service_fn(move |context| {
                udp::serve(
                    context,
                    "127.0.0.1:0".parse().unwrap(),
                    udp_tx,
                    udp_counters,
                    move |packet, peer, socket, _| {
                        let entered = packet_entered.clone();
                        let release = packet_release.clone();
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            socket.send_to(&packet, peer).await?;
                            Ok(())
                        }
                    },
                )
            }),
        )
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = control.observer();
    observer.ready(Duration::from_secs(3)).await?;
    let http_address = http_rx.await?;
    let udp_address = udp_rx.await?;
    let (mut ws, response) =
        tokio_tungstenite::connect_async(format!("ws://{http_address}/ws")).await?;
    assert_eq!(response.status(), 101);
    let payload = format!("cycle-{index}");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        payload.clone().into(),
    ))
    .await?;
    assert_eq!(ws.next().await.transpose()?.unwrap().into_text()?, payload);
    let mut http_client = TcpStream::connect(http_address).await?;
    http_client
        .write_all(
            format!("GET /inflight HTTP/1.1\r\nHost: {http_address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    entered.notified().await;
    let udp_client = UdpSocket::bind("127.0.0.1:0").await?;
    udp_client.send_to(payload.as_bytes(), udp_address).await?;
    udp_entered.notified().await;
    control.stop();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = observer.snapshot();
            if snapshot
                .services
                .iter()
                .filter(|s| s.id.as_str() != "resource")
                .all(|s| s.stopping)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(
        !observer
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "resource")
            .unwrap()
            .stopping
    );
    release.notify_one();
    udp_release.notify_one();
    let mut body = Vec::new();
    http_client.read_to_end(&mut body).await?;
    let response = String::from_utf8(body)?;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.ends_with("completed-body"));
    let mut packet = [0; 64];
    let (n, _) = udp_client.recv_from(&mut packet).await?;
    assert_eq!(&packet[..n], payload.as_bytes());
    let mut close = false;
    while let Some(message) = ws.next().await {
        if message?.is_close() {
            ws.flush().await?;
            close = true;
            break;
        }
    }
    assert!(close, "server must send Close");
    drop(ws);
    drop(http_client);
    drop(udp_client);
    control.shutdown().await?;
    let exit = observer.wait().await?;
    assert!(exit.is_success(), "cycle {index}: {exit:?}");
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(
        exit.services
            .iter()
            .all(|s| s.work.active.is_empty() && s.work.unconfirmed.is_empty())
    );
    assert_eq!(counters.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(counters.dropped_at_capacity.load(Ordering::SeqCst), 0);
    drop(TcpListener::bind(http_address).await?);
    drop(UdpSocket::bind(udp_address).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_hundred_protocol_start_stop_cycles() {
    for index in 0..100 {
        tokio::time::timeout(Duration::from_secs(10), cycle(index))
            .await
            .expect("cycle timeout")
            .expect("protocol lifecycle");
    }
    println!(
        "completed 100/100 real HTTP+WS+UDP cycles with in-flight stop, cleanup, and port rebind"
    );
}
