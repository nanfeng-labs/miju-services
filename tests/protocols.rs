//! Real sockets and release barriers through the same adapters used by mains.

#[path = "../examples/support/http.rs"]
mod http;
#[path = "../examples/support/udp.rs"]
mod udp;
#[path = "../examples/support/websocket.rs"]
mod websocket;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{State, WebSocketUpgrade, ws::Message},
    response::IntoResponse,
    routing::get,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use miju_services::{
    App, Service, ShutdownProgress,
    app::AppOptions,
    service::ServiceSpec,
    work::{WorkOutcome, WorkScope, WorkSpec},
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("protocol stalled")
}

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
        self.0.store(true, Ordering::SeqCst);
    }
}

struct GatedBody {
    entered: CancellationToken,
    release: CancellationToken,
    waiter: Pin<Box<dyn Future<Output = ()> + Send>>,
    _released: Released,
    done: bool,
    fail: bool,
}

impl http_body::Body for GatedBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        this.entered.cancel();
        if !this.release.is_cancelled() && this.waiter.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        this.done = true;
        Poll::Ready(Some(if this.fail {
            Err(io::Error::other("body-io-failure"))
        } else {
            Ok(http_body::Frame::data(Bytes::from_static(b"finished")))
        }))
    }
}

async fn body_case(fail: bool) {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let body = Arc::new(Mutex::new(Some(GatedBody {
        entered: entered.clone(),
        release: release.clone(),
        waiter: Box::pin(release.clone().cancelled_owned()),
        _released: Released(dropped.clone()),
        done: false,
        fail,
    })));
    let router = Router::new().route(
        "/body",
        get(move || {
            let body = body.lock().unwrap().take().unwrap();
            async { Body::new(body) }
        }),
    );
    let mut service = http::HttpService::new(local(), router);
    let (send, receive) = oneshot::channel();
    service.bound = Some(send);
    let check = dropped.clone();
    let app = App::builder()
        .cleanup("after-body", move || async move {
            assert!(
                check.load(Ordering::SeqCst),
                "cleanup overtook response body release"
            );
            Ok(())
        })
        .service("http", service)
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let client = tokio::spawn(request(address, "/body"));
    bounded(entered.cancelled()).await;
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(!dropped.load(Ordering::SeqCst));
    release.cancel();
    let response = bounded(client).await.unwrap();
    if !fail {
        assert!(response.contains("finished"));
    }
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(dropped.load(Ordering::SeqCst));
    drop(TcpListener::bind(address).await.unwrap());
}

#[tokio::test]
async fn http_body_actual_release_precedes_cleanup_and_port_reuse() {
    body_case(false).await;
}

#[tokio::test]
async fn http_body_io_failure_still_releases_before_cleanup() {
    body_case(true).await;
}

#[tokio::test]
async fn http_router_and_inflight_future_release_before_cleanup() {
    let dropped = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Released(dropped.clone()));
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let signal = entered.clone();
    let gate = release.clone();
    let router = Router::new().route(
        "/held",
        get(move || {
            let user = state.clone();
            let signal = signal.clone();
            let gate = gate.clone();
            async move {
                signal.cancel();
                gate.cancelled().await;
                drop(user);
                "released"
            }
        }),
    );
    let mut service = http::HttpService::new(local(), router);
    let (send, receive) = oneshot::channel();
    service.bound = Some(send);
    let check = dropped.clone();
    let app = App::builder()
        .cleanup("after-router", move || async move {
            assert!(
                check.load(Ordering::SeqCst),
                "cleanup overtook router/future release"
            );
            Ok(())
        })
        .service("http", service)
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    let client = tokio::spawn(request(address, "/held"));
    bounded(entered.cancelled()).await;
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(!dropped.load(Ordering::SeqCst));
    release.cancel();
    assert!(bounded(client).await.unwrap().contains("released"));
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn bind_failure_never_reports_ready_and_releases_assembly() {
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
    let app = App::builder()
        .service(
            "http",
            http::HttpService::new(occupied.local_addr().unwrap(), router),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(!report.snapshot.ready);
    assert!(!report.snapshot.services[0].initialized);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn websocket_echo_capacity_close_and_port_reuse() {
    let mut service = websocket::WebSocketService::new(local());
    let (send, receive) = oneshot::channel();
    service.bound = Some(send);
    let app = App::builder()
        .service(
            ServiceSpec::new("websocket").max_work(2.try_into().unwrap()),
            service,
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let (mut socket, response) = bounded(tokio_tungstenite::connect_async(format!(
        "ws://{address}/ws"
    )))
    .await
    .unwrap();
    assert_eq!(response.status(), 101);
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text("echo".into()))
        .await
        .unwrap();
    assert_eq!(
        bounded(socket.next())
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
        "echo"
    );
    let second = bounded(tokio_tungstenite::connect_async(format!(
        "ws://{address}/ws"
    )))
    .await;
    assert!(second.is_err(), "overload was upgraded before admission");
    let error = second.unwrap_err();
    assert!(
        matches!(error, tokio_tungstenite::tungstenite::Error::Http(ref response) if response.status() == 503),
        "overload was upgraded: {error:?}"
    );
    app.stop();
    assert!(!app.snapshot().ready);
    assert!(bounded(socket.next()).await.unwrap().unwrap().is_close());
    socket.flush().await.unwrap();
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    drop(TcpListener::bind(address).await.unwrap());
}

#[tokio::test]
async fn failed_upgrade_releases_reservation_and_closed_scope_refuses_new_sessions() {
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .service_fn("sessions", move |context| async move {
            let _ = send.send(context.work().clone());
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    let scope = bounded(receive).await.unwrap();
    let users = tokio_util::task::TaskTracker::new();
    let sender = websocket::reserve(&scope, &users).unwrap();
    drop(sender); // Framework upgrade fails before transferring a socket.
    bounded(async {
        while !scope.snapshot().active.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        scope.snapshot().recent[0].outcome(),
        WorkOutcome::Succeeded
    ));
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(matches!(
        websocket::reserve(&scope, &users),
        Err(miju_services::work::SpawnError::Closed)
    ));
}

#[tokio::test]
async fn late_upgraded_socket_cannot_escape_a_stopped_reservation() {
    let callback = CancellationToken::new();
    let release = CancellationToken::new();
    let late_rejected = Arc::new(AtomicBool::new(false));
    let signal = callback.clone();
    let gate = release.clone();
    let refused = late_rejected.clone();
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = cleaned.clone();
    let (send, receive) = oneshot::channel();
    let app = App::builder()
        .cleanup("after-upgrade", move || async move {
            cleanup.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service_fn("websocket", move |context| async move {
            let users = tokio_util::task::TaskTracker::new();
            let router = Router::new()
                .route(
                    "/ws",
                    get(
                        move |State((scope, users)): State<(
                            WorkScope,
                            tokio_util::task::TaskTracker,
                        )>,
                              upgrade: WebSocketUpgrade| {
                            let handoff = websocket::reserve(&scope, &users).unwrap();
                            let signal = signal.clone();
                            let gate = gate.clone();
                            let refused = refused.clone();
                            async move {
                                upgrade
                                    .on_upgrade(move |socket| async move {
                                        signal.cancel();
                                        gate.cancelled().await;
                                        handoff.deliver(socket);
                                        refused.store(true, Ordering::SeqCst);
                                    })
                                    .into_response()
                            }
                        },
                    ),
                )
                .with_state((context.work().clone(), users.clone()));
            let mut service = http::HttpService::new(local(), router);
            service.bound = Some(send);
            service.users = users;
            service.run(context).await
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    let (mut socket, _) = bounded(tokio_tungstenite::connect_async(format!(
        "ws://{address}/ws"
    )))
    .await
    .unwrap();
    bounded(callback.cancelled()).await;
    assert!(
        matches!(
            app.shutdown_within(Duration::from_millis(10))
                .await
                .unwrap(),
            ShutdownProgress::Pending(_)
        ),
        "HTTP certified completion while upgrade callback held a socket"
    );
    assert!(!cleaned.load(Ordering::SeqCst));
    release.cancel();
    bounded(async {
        while !late_rejected.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(bounded(socket.next()).await, None | Some(Err(_))));
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(cleaned.load(Ordering::SeqCst));
}

struct BlockedSocket {
    incoming: VecDeque<Message>,
    dropped: Arc<AtomicBool>,
}

impl Drop for BlockedSocket {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl Stream for BlockedSocket {
    type Item = Result<Message, io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.incoming.pop_front() {
            Some(item) => Poll::Ready(Some(Ok(item))),
            None => Poll::Pending,
        }
    }
}
impl Sink<Message> for BlockedSocket {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }
    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), io::Error> {
        unreachable!("blocked sink was not ready")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn websocket_close_budget_includes_blocked_send_and_drops_transport() {
    let dropped = Arc::new(AtomicBool::new(false));
    let socket = BlockedSocket {
        incoming: VecDeque::new(),
        dropped: dropped.clone(),
    };
    let error = tokio::time::timeout(Duration::from_millis(2100), websocket::close(socket, true))
        .await
        .expect("close exceeded its absolute budget")
        .unwrap_err();
    assert!(error.to_string().contains("two seconds"));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn websocket_stop_interrupts_outbound_work_then_releases_after_close_budget() {
    let dropped = Arc::new(AtomicBool::new(false));
    let socket = BlockedSocket {
        incoming: VecDeque::from([Message::Text("queued".into())]),
        dropped: dropped.clone(),
    };
    let app = App::builder()
        .service_fn("sessions", move |context| async move {
            context
                .work()
                .spawn(WorkSpec::new("outgoing"), move |work| {
                    websocket::session(socket, work)
                })?;
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    tokio::task::yield_now().await;
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(!dropped.load(Ordering::SeqCst));
    tokio::time::advance(Duration::from_secs(2)).await;
    let report = app.shutdown().await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        report.snapshot.services[0].work.recent[0].outcome(),
        WorkOutcome::Failed(_)
    ));
}

#[tokio::test]
async fn udp_capacity_and_stop_keep_inflight_work_owned_until_port_rebind() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let signal = entered.clone();
    let gate = release.clone();
    let (send, receive) = oneshot::channel();
    let mut service = udp::UdpService::new(local(), move |packet, peer, socket, _| {
        let signal = signal.clone();
        let gate = gate.clone();
        async move {
            signal.cancel();
            gate.cancelled().await;
            socket.send_to(&packet, peer).await?;
            Ok(())
        }
    });
    let counters = service.counters.clone();
    service.bound = Some(send);
    let app = App::builder()
        .service(
            ServiceSpec::new("udp").max_work(1.try_into().unwrap()),
            service,
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    app.ready(Duration::from_secs(1)).await.unwrap();
    let socket = UdpSocket::bind(local()).await.unwrap();
    socket.send_to(b"held", address).await.unwrap();
    bounded(entered.cancelled()).await;
    socket.send_to(b"overload", address).await.unwrap();
    bounded(async {
        while counters.dropped_at_capacity.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    socket.send_to(b"after-stop", address).await.unwrap();
    assert_eq!(counters.accepted.load(Ordering::Relaxed), 1);
    assert!(UdpSocket::bind(address).await.is_err());
    release.cancel();
    let mut packet = [0; 32];
    let (len, _) = bounded(socket.recv_from(&mut packet)).await.unwrap();
    assert_eq!(&packet[..len], b"held");
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    drop(UdpSocket::bind(address).await.unwrap());
}

#[tokio::test]
async fn later_failure_drains_protocol_work_before_dependency_and_cleanup() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let signal = entered.clone();
    let gate = release.clone();
    let (bound, address) = oneshot::channel();
    let cleaned = Arc::new(AtomicBool::new(false));
    let close = cleaned.clone();
    let failure = CancellationToken::new();
    let trigger = failure.clone();
    let app = App::builder()
        .cleanup("provider", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service_fn("provider", |context| async move {
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .service_fn(
            ServiceSpec::new("http").depends_on("provider"),
            move |context| async move {
                let router = Router::new().route(
                    "/held",
                    get(move || {
                        let signal = signal.clone();
                        let gate = gate.clone();
                        async move {
                            signal.cancel();
                            gate.cancelled().await;
                            "drained"
                        }
                    }),
                );
                let mut service = http::HttpService::new(local(), router);
                service.bound = Some(bound);
                service.run(context).await
            },
        )
        .service_fn(
            ServiceSpec::new("failing").depends_on("http"),
            move |_| async move {
                trigger.cancelled().await;
                Err(io::Error::other("later startup failed").into())
            },
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(address).await.unwrap();
    let client = tokio::spawn(request(address, "/held"));
    bounded(entered.cancelled()).await;
    failure.cancel();
    bounded(async {
        while app.snapshot().services[2].failures.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(
        !app.snapshot().services[0].stopping,
        "provider stopped before HTTP work released"
    );
    release.cancel();
    assert!(bounded(client).await.unwrap().contains("drained"));
    let report = bounded(app.wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn one_hundred_real_protocol_generations_reuse_all_ports() {
    for generation in 0..100 {
        let (http_send, http_receive) = oneshot::channel();
        let (ws_send, ws_receive) = oneshot::channel();
        let (udp_send, udp_receive) = oneshot::channel();
        let mut http =
            http::HttpService::new(local(), Router::new().route("/", get(|| async { "ok" })));
        http.bound = Some(http_send);
        let mut ws = websocket::WebSocketService::new(local());
        ws.bound = Some(ws_send);
        let mut udp = udp::UdpService::new(local(), |packet, peer, socket, _| async move {
            socket.send_to(&packet, peer).await?;
            Ok(())
        });
        udp.bound = Some(udp_send);
        let app = App::builder()
            .service("http", http)
            .service("websocket", ws)
            .service("udp", udp)
            .build()
            .unwrap()
            .start()
            .unwrap();
        let http_address = bounded(http_receive).await.unwrap();
        let ws_address = bounded(ws_receive).await.unwrap();
        let udp_address = bounded(udp_receive).await.unwrap();
        app.ready(Duration::from_secs(1)).await.unwrap();
        assert!(request(http_address, "/").await.ends_with("ok"));
        let (mut ws, _) = bounded(tokio_tungstenite::connect_async(format!(
            "ws://{ws_address}/ws"
        )))
        .await
        .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            generation.to_string().into(),
        ))
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
        let (len, _) = bounded(client.recv_from(&mut packet)).await.unwrap();
        assert_eq!(&packet[..len], b"ping");
        assert!(bounded(app.shutdown()).await.unwrap().is_success());
        drop(TcpListener::bind(http_address).await.unwrap());
        drop(TcpListener::bind(ws_address).await.unwrap());
        drop(UdpSocket::bind(udp_address).await.unwrap());
    }
}

#[test]
fn embedded_runtime_survives_pending_then_observes_actual_cleanup() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let released = Arc::new(AtomicBool::new(false));
    let close = released.clone();
    runtime.block_on(async {
        let app = App::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_millis(10),
                ..Default::default()
            })
            .cleanup("slow-close", move || async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                close.store(true, Ordering::SeqCst);
                Ok(())
            })
            .service_fn("embedded", |context| async move {
                context.ready();
                context.stopping().await;
                Ok(())
            })
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.ready(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            app.shutdown_within(Duration::from_millis(1)).await.unwrap(),
            ShutdownProgress::Pending(_)
        ));
        assert!(!released.load(Ordering::SeqCst));
        assert!(app.shutdown().await.unwrap().is_success());
    });
    assert!(released.load(Ordering::SeqCst));
}

#[derive(Default)]
struct DropGate {
    entered: CancellationToken,
    released: Mutex<bool>,
    condition: std::sync::Condvar,
}

struct BlockingDrop(Arc<DropGate>);

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        // Let Tokio replace this worker while a real user destructor is blocked.
        tokio::task::block_in_place(|| {
            self.0.entered.cancel();
            let mut released = self.0.released.lock().unwrap();
            while !*released {
                released = self.0.condition.wait(released).unwrap();
            }
        });
    }
}

struct ReleaseDrop(Arc<DropGate>);

impl Drop for ReleaseDrop {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.condition.notify_all();
    }
}

async fn destructor_case(which: &str) {
    let gate = Arc::new(DropGate::default());
    let release = ReleaseDrop(gate.clone());
    let entered = CancellationToken::new();
    let signal = entered.clone();
    let captured = gate.clone();
    let router = match which {
        "future" => Router::new().route(
            "/",
            get(move || {
                let guard = BlockingDrop(captured.clone());
                let signal = signal.clone();
                async move {
                    let _guard = guard;
                    signal.cancel();
                    std::future::pending::<()>().await;
                    "unused"
                }
            }),
        ),
        "body" => Router::new().route(
            "/",
            get(move || {
                let captured = captured.clone();
                let signal = signal.clone();
                async move {
                    Body::from_stream(futures_util::stream::once(async move {
                        let _guard = BlockingDrop(captured);
                        signal.cancel();
                        std::future::pending::<()>().await;
                        Ok::<Bytes, io::Error>(Bytes::new())
                    }))
                }
            }),
        ),
        "router" => Router::new()
            .route(
                "/",
                get(|State(_state): State<Arc<BlockingDrop>>| async { "owned" }),
            )
            .with_state(Arc::new(BlockingDrop(captured))),
        _ => unreachable!(),
    };
    let (send, receive) = oneshot::channel();
    let mut service = http::HttpService::new(local(), router);
    service.bound = Some(send);
    let cleaned = Arc::new(AtomicBool::new(false));
    let close = cleaned.clone();
    let app = App::builder()
        .cleanup("after-destructor", move || async move {
            close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .service_fn("provider", |context| async move {
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .service(ServiceSpec::new("http").depends_on("provider"), service)
        .build()
        .unwrap()
        .start()
        .unwrap();
    let address = bounded(receive).await.unwrap();
    let mut client = TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    if which == "router" {
        let mut response = String::new();
        bounded(client.read_to_string(&mut response)).await.unwrap();
        assert!(response.ends_with("owned"));
        app.stop();
    } else {
        bounded(entered.cancelled()).await;
    }
    drop(client);
    bounded(gate.entered.cancelled()).await;
    assert!(
        matches!(
            app.shutdown_within(Duration::from_millis(20))
                .await
                .unwrap(),
            ShutdownProgress::Pending(_)
        ),
        "HTTP certified completion during {which} destructor"
    );
    assert!(
        !cleaned.load(Ordering::SeqCst),
        "cleanup overtook {which} destructor"
    );
    assert!(
        !app.snapshot().services[0].stopping,
        "provider stopped during {which} destructor"
    );
    drop(release);
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn http_disconnect_keeps_handler_destructor_owned() {
    destructor_case("future").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn http_disconnect_keeps_body_destructor_owned() {
    destructor_case("body").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn http_router_destructor_is_an_actual_release_barrier() {
    destructor_case("router").await;
}
