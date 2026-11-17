//! The protocol checks that need real sockets: bind failure, port reuse after
//! drain, UDP, and a hundred generations reusing every port.

// Shared with other targets; not every item is used here.
#[allow(dead_code)]
#[path = "../examples/support/http.rs"]
mod http;
// Shared with other targets; not every item is used here.
#[allow(dead_code)]
#[path = "support/memory.rs"]
mod memory;
// Shared with other targets; not every item is used here.
#[allow(dead_code)]
#[path = "../examples/support/udp.rs"]
mod udp;
// Shared with other targets; not every item is used here.
#[allow(dead_code)]
#[path = "../examples/support/websocket.rs"]
mod websocket;

use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use memory::bounded;
use miju_services::{App, Service, Spec};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

fn local() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn request(address: SocketAddr, path: &str) -> String {
    let mut socket = bounded(TcpStream::connect(address)).await.unwrap();
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    bounded(socket.read_to_string(&mut response)).await.unwrap();
    response
}

struct Released(Arc<AtomicBool>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

#[tokio::test]
async fn a_bind_failure_never_reports_ready_and_releases_the_router() {
    let occupied = TcpListener::bind(local()).await.unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Released(dropped.clone()));
    let router = Router::new().route(
        "/",
        get(move || {
            let state = state.clone();
            async move {
                drop(state);
                "ok"
            }
        }),
    );
    let mut app = App::new();
    app.service(
        "http",
        http::HttpService::new(occupied.local_addr().unwrap(), router),
    );
    let report = bounded(app.start().unwrap().wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(!report.service("http").unwrap().initialized);
    assert!(dropped.load(SeqCst));
}

#[tokio::test]
async fn udp_drops_overload_and_keeps_accepted_datagrams_owned_until_the_port_is_free() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let (send, receive) = oneshot::channel();
    let mut service = {
        let (entered, release) = (entered.clone(), release.clone());
        udp::UdpService::new(local(), move |packet, peer, socket, _ctx| {
            let (entered, release) = (entered.clone(), release.clone());
            async move {
                entered.cancel();
                release.cancelled().await;
                socket.send_to(&packet, peer).await?;
                Ok(())
            }
        })
    };
    let counters = service.counters.clone();
    service.bound = Some(send);
    let mut app = App::new();
    app.service(Spec::new("udp").capacity(1.try_into().unwrap()), service);
    let app = app.start().unwrap();
    let address = bounded(receive).await.unwrap();
    bounded(app.ready()).await.unwrap();
    let socket = UdpSocket::bind(local()).await.unwrap();
    socket.send_to(b"held", address).await.unwrap();
    bounded(entered.cancelled()).await;
    socket.send_to(b"overload", address).await.unwrap();
    bounded(async {
        while counters.dropped_at_capacity.load(SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    app.stop();
    assert!(
        timeout(Duration::from_millis(100), app.wait())
            .await
            .is_err()
    );
    socket.send_to(b"after stop", address).await.unwrap();
    assert_eq!(counters.accepted.load(SeqCst), 1);
    assert!(
        UdpSocket::bind(address).await.is_err(),
        "the port was released under accepted work"
    );
    release.cancel();
    let mut packet = [0; 32];
    let (length, _) = bounded(socket.recv_from(&mut packet)).await.unwrap();
    assert_eq!(&packet[..length], b"held");
    assert!(bounded(app.wait()).await.unwrap().is_success());
    drop(UdpSocket::bind(address).await.unwrap());
}

#[tokio::test]
async fn a_hundred_generations_of_http_websocket_and_udp_reuse_every_port() {
    for generation in 0..100 {
        let (http_bound, http_address) = oneshot::channel();
        let (ws_bound, ws_address) = oneshot::channel();
        let (udp_bound, udp_address) = oneshot::channel();
        let mut udp = udp::UdpService::new(local(), |packet, peer, socket, _ctx| async move {
            socket.send_to(&packet, peer).await?;
            Ok(())
        });
        udp.bound = Some(udp_bound);
        let mut app = App::new();
        app.service(
            "http",
            http::HttpService::new(local(), Router::new().route("/", get(|| async { "ok" })))
                .report_bound(http_bound),
        );
        app.service_fn("websocket", move |ctx| async move {
            let router = websocket::router(websocket::Sessions::new(&ctx, 4));
            http::HttpService::new(local(), router)
                .report_bound(ws_bound)
                .run(ctx)
                .await
        });
        app.service("udp", udp);
        let app = app.start().unwrap();
        let (http_address, ws_address, udp_address) = (
            bounded(http_address).await.unwrap(),
            bounded(ws_address).await.unwrap(),
            bounded(udp_address).await.unwrap(),
        );
        bounded(app.ready()).await.unwrap();

        assert!(request(http_address, "/").await.ends_with("ok"));
        let (mut ws, _) = bounded(tokio_tungstenite::connect_async(format!(
            "ws://{ws_address}/ws"
        )))
        .await
        .unwrap();
        ws.send(Message::Text(generation.to_string().into()))
            .await
            .unwrap();
        assert_eq!(
            bounded(ws.next())
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap(),
            generation.to_string()
        );
        ws.close(None).await.unwrap();
        let _ = bounded(ws.next()).await;
        let client = UdpSocket::bind(local()).await.unwrap();
        client.send_to(b"ping", udp_address).await.unwrap();
        let mut packet = [0; 16];
        let (length, _) = bounded(client.recv_from(&mut packet)).await.unwrap();
        assert_eq!(&packet[..length], b"ping");

        assert!(bounded(app.shutdown()).await.unwrap().is_success());
        drop(TcpListener::bind(http_address).await.unwrap());
        drop(TcpListener::bind(ws_address).await.unwrap());
        drop(UdpSocket::bind(udp_address).await.unwrap());
    }
}
