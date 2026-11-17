//! HTTP and WebSocket release barriers through the example adapters, on an
//! in-memory transport. Real sockets are in `tests/protocols_net.rs`.

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
use memory::{Connector, bounded, memory};
use miju_services::{App, AppHandle, BoxError, Context, Rejected, Spec, report::ServicePhase};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message as ClientMessage;
use tokio_util::sync::CancellationToken;

const SOON: Duration = Duration::from_millis(150);

/// Register an HTTP service that serves `router` on an in-memory listener.
fn serve_in_memory(
    app: &mut App,
    spec: impl Into<Spec>,
    router: impl FnOnce(&Context) -> Router + Send + 'static,
) -> Connector {
    let (listener, connector) = memory();
    app.service_fn(spec, move |ctx| async move {
        let router = router(&ctx);
        ctx.ready();
        http::serve(&ctx, listener, router).await?;
        Ok(())
    });
    connector
}

/// A provider whose cleanup records that it ran, for "nothing overtakes X" checks.
fn provider(app: &mut App, check: impl FnOnce() + Send + 'static) -> Arc<AtomicBool> {
    let cleaned = Arc::new(AtomicBool::new(false));
    let ran = cleaned.clone();
    app.service_fn("provider", move |ctx| async move {
        ctx.defer("provider-resource", async move {
            check();
            ran.store(true, SeqCst);
            Ok::<_, BoxError>(())
        })?;
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    cleaned
}

async fn still_owned(app: &AppHandle) -> bool {
    timeout(SOON, app.wait()).await.is_err()
}

struct Released(Arc<AtomicBool>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
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
        cx: &mut TaskContext<'_>,
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
            Err(io::Error::other("body io failure"))
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
    let mut app = App::new();
    let check = dropped.clone();
    let cleaned = provider(&mut app, move || {
        assert!(
            check.load(SeqCst),
            "cleanup overtook the response body's release"
        );
    });
    let connector = serve_in_memory(&mut app, Spec::new("http").after("provider"), move |_| {
        Router::new().route(
            "/body",
            get(move || {
                let body = body.lock().unwrap().take().unwrap();
                async { Body::new(body) }
            }),
        )
    });
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let client = tokio::spawn(async move { connector.request("/body").await });
    bounded(entered.cancelled()).await;

    app.stop();
    assert!(
        still_owned(&app).await,
        "reported while a response body was streaming"
    );
    assert!(!dropped.load(SeqCst));
    release.cancel();
    let response = bounded(client).await.unwrap();
    if !fail {
        assert!(response.contains("finished"));
    }
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(dropped.load(SeqCst));
    assert!(cleaned.load(SeqCst));
}

#[tokio::test]
async fn an_http_body_is_released_before_drain_and_cleanup() {
    body_case(false).await;
}

#[tokio::test]
async fn an_http_body_that_fails_is_still_released_before_drain_and_cleanup() {
    body_case(true).await;
}

#[tokio::test]
async fn the_router_and_an_inflight_handler_are_released_before_cleanup() {
    let dropped = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Released(dropped.clone()));
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let mut app = App::new();
    let check = dropped.clone();
    let cleaned = provider(&mut app, move || {
        assert!(
            check.load(SeqCst),
            "cleanup overtook the router and its handler"
        );
    });
    let connector = {
        let (entered, release) = (entered.clone(), release.clone());
        serve_in_memory(&mut app, Spec::new("http").after("provider"), move |_| {
            Router::new().route(
                "/held",
                get(move || {
                    let (user, entered, release) =
                        (state.clone(), entered.clone(), release.clone());
                    async move {
                        entered.cancel();
                        release.cancelled().await;
                        drop(user);
                        "released"
                    }
                }),
            )
        })
    };
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let client = tokio::spawn(async move { connector.request("/held").await });
    bounded(entered.cancelled()).await;
    app.stop();
    assert!(
        still_owned(&app).await,
        "reported while a handler was running"
    );
    assert!(!dropped.load(SeqCst));
    release.cancel();
    assert!(bounded(client).await.unwrap().contains("released"));
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(dropped.load(SeqCst));
    assert!(cleaned.load(SeqCst));
}

#[derive(Default)]
struct DropGate {
    entered: CancellationToken,
    released: Mutex<bool>,
    condition: Condvar,
}

/// A destructor that blocks until the test releases it.
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

async fn destructor_case(which: &'static str) {
    let gate = Arc::new(DropGate::default());
    let release = ReleaseDrop(gate.clone());
    let entered = CancellationToken::new();
    let mut app = App::new();
    let cleaned = provider(&mut app, || {});
    let connector = {
        let (signal, captured) = (entered.clone(), gate.clone());
        serve_in_memory(
            &mut app,
            Spec::new("http").after("provider"),
            move |_| match which {
                "future" => Router::new().route(
                    "/",
                    get(move || {
                        let (guard, signal) = (BlockingDrop(captured.clone()), signal.clone());
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
                        let (captured, signal) = (captured.clone(), signal.clone());
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
            },
        )
    };
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let mut client = connector.connect();
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
    // The client goes away; the server side has to notice and release.
    drop(client);
    bounded(gate.entered.cancelled()).await;
    app.stop();
    assert!(
        still_owned(&app).await,
        "reported during the {which} destructor"
    );
    assert!(
        !cleaned.load(SeqCst),
        "cleanup overtook the {which} destructor"
    );
    assert_eq!(
        app.snapshot().service("provider").unwrap().phase,
        ServicePhase::Running,
        "the provider was stopped during the {which} destructor"
    );
    drop(release);
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(cleaned.load(SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_disconnected_handlers_destructor_is_still_owned() {
    destructor_case("future").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_disconnected_bodys_destructor_is_still_owned() {
    destructor_case("body").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_routers_destructor_is_a_release_barrier() {
    destructor_case("router").await;
}

// Grace expiry aborts the root while a request is in flight in a connection
// task axum spawned. The request's lease has gone into its response by the
// time the connection task drops the last router clone; only the
// connection's lease still covers that clone and its state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn after_the_root_is_aborted_the_last_router_clone_is_still_leased() {
    let gate = Arc::new(DropGate::default());
    let release_destructor = ReleaseDrop(gate.clone());
    let entered = CancellationToken::new();
    let finish = CancellationToken::new();
    let mut app = App::new();
    app.shutdown_grace(Duration::from_millis(50));
    let connector = {
        let (entered, finish, state) = (entered.clone(), finish.clone(), gate.clone());
        serve_in_memory(&mut app, "http", move |_| {
            Router::new()
                .route(
                    "/held",
                    get(move |State(_state): State<Arc<BlockingDrop>>| {
                        let (entered, finish) = (entered.clone(), finish.clone());
                        async move {
                            entered.cancel();
                            finish.cancelled().await;
                            "done"
                        }
                    }),
                )
                .with_state(Arc::new(BlockingDrop(state)))
        })
    };
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let client = tokio::spawn(async move { connector.request("/held").await });
    bounded(entered.cancelled()).await;
    app.stop();
    bounded(async {
        while !app.snapshot().service("http").unwrap().forced {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        !gate.entered.is_cancelled(),
        "the root's router template was the last clone"
    );

    // The in-flight request, which no abort reaches, now finishes.
    finish.cancel();
    assert!(bounded(client).await.unwrap().ends_with("done"));
    bounded(gate.entered.cancelled()).await;
    assert!(
        still_owned(&app).await,
        "reported while the last router clone's state was being destroyed"
    );
    drop(release_destructor);
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.service("http").unwrap().forced);
}

#[tokio::test]
async fn a_request_or_connection_without_a_lease_gets_503_and_no_handler_runs() {
    for hold_the_only_slot in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut app = App::new();
        let connector = {
            let calls = calls.clone();
            let (listener, connector) = memory();
            app.service_fn(
                Spec::new("http").capacity(NonZeroUsize::new(1).unwrap()),
                move |ctx| async move {
                    // With the slot held, the connection itself is refused;
                    // otherwise the connection takes it and the request is refused.
                    let _held = if hold_the_only_slot {
                        Some(ctx.lease("held")?)
                    } else {
                        None
                    };
                    let router = Router::new().route(
                        "/",
                        get(move || {
                            calls.fetch_add(1, SeqCst);
                            async { "ok" }
                        }),
                    );
                    ctx.ready();
                    http::serve(&ctx, listener, router).await?;
                    Ok(())
                },
            );
            connector
        };
        let app = app.start().unwrap();
        bounded(app.ready()).await.unwrap();
        let response = connector.request("/").await;
        assert!(
            response.starts_with("HTTP/1.1 503"),
            "held={hold_the_only_slot}: {response:?}"
        );
        assert_eq!(calls.load(SeqCst), 0, "a handler ran without a lease");
        assert!(bounded(app.shutdown()).await.unwrap().is_success());
    }
}

async fn ws_connect(
    connector: &Connector,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
    tokio_tungstenite::tungstenite::Error,
> {
    bounded(tokio_tungstenite::client_async(
        "ws://localhost/ws",
        connector.connect(),
    ))
    .await
    .map(|(socket, response)| {
        assert_eq!(response.status(), 101);
        socket
    })
}

#[tokio::test]
async fn a_websocket_echoes_refuses_overload_with_503_and_closes_at_stop() {
    let mut app = App::new();
    let connector = serve_in_memory(&mut app, "websocket", |ctx| {
        websocket::router(websocket::Sessions::new(ctx, 1))
    });
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let mut socket = ws_connect(&connector).await.unwrap();
    socket
        .send(ClientMessage::Text("echo".into()))
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

    let second = ws_connect(&connector).await;
    assert!(
        matches!(&second, Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status() == 503),
        "the second session was upgraded before admission: {second:?}"
    );

    app.stop();
    assert!(!app.status().ready);
    assert!(bounded(socket.next()).await.unwrap().unwrap().is_close());
    socket.flush().await.unwrap();
    assert!(bounded(app.wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn a_failed_upgrade_releases_its_reservation_and_a_drained_scope_refuses_new_ones() {
    let (send, receive) = oneshot::channel();
    let mut app = App::new();
    app.service_fn("sessions", move |ctx| async move {
        let _ = send.send(websocket::Sessions::new(&ctx, 4));
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    let sessions = bounded(receive).await.unwrap();
    let handoff = websocket::reserve(&sessions).unwrap();
    // The framework's upgrade fails before it hands a socket over.
    drop(handoff);
    bounded(async {
        while app
            .snapshot()
            .service("sessions")
            .unwrap()
            .outstanding_total
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let report = bounded(app.shutdown()).await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.service("sessions").unwrap().work.completed, 1);
    assert!(matches!(
        websocket::reserve(&sessions),
        Err(Rejected::Closed)
    ));
}

#[tokio::test]
async fn a_late_upgraded_socket_cannot_escape_a_stopped_reservation() {
    let callback = CancellationToken::new();
    let release = CancellationToken::new();
    let late_delivered = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let cleaned = provider(&mut app, || {});
    let connector = {
        let (callback, release, late_delivered) =
            (callback.clone(), release.clone(), late_delivered.clone());
        serve_in_memory(
            &mut app,
            Spec::new("websocket").after("provider"),
            move |ctx| {
                Router::new()
                    .route(
                        "/ws",
                        get(
                            move |State(sessions): State<websocket::Sessions>,
                                  upgrade: WebSocketUpgrade| {
                                let handoff = websocket::reserve(&sessions).unwrap();
                                let (callback, release, late_delivered) =
                                    (callback.clone(), release.clone(), late_delivered.clone());
                                async move {
                                    upgrade
                                        .on_upgrade(move |socket| async move {
                                            callback.cancel();
                                            release.cancelled().await;
                                            handoff.deliver(socket);
                                            late_delivered.store(true, SeqCst);
                                        })
                                        .into_response()
                                }
                            },
                        ),
                    )
                    .with_state(websocket::Sessions::new(ctx, 4))
            },
        )
    };
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let mut socket = ws_connect(&connector).await.unwrap();
    bounded(callback.cancelled()).await;
    app.stop();
    assert!(
        still_owned(&app).await,
        "reported while the upgrade callback held a socket"
    );
    assert!(!cleaned.load(SeqCst));
    release.cancel();
    bounded(async {
        while !late_delivered.load(SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(bounded(socket.next()).await, None | Some(Err(_))));
    assert!(bounded(app.wait()).await.unwrap().is_success());
    assert!(cleaned.load(SeqCst));
}

struct BlockedSocket {
    incoming: VecDeque<Message>,
    dropped: Arc<AtomicBool>,
}

impl Drop for BlockedSocket {
    fn drop(&mut self) {
        self.dropped.store(true, SeqCst);
    }
}

impl Stream for BlockedSocket {
    type Item = Result<Message, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.incoming.pop_front() {
            Some(item) => Poll::Ready(Some(Ok(item))),
            None => Poll::Pending,
        }
    }
}

impl Sink<Message> for BlockedSocket {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), io::Error> {
        unreachable!("a blocked sink is never ready")
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn the_websocket_close_budget_covers_a_blocked_send_and_drops_the_transport() {
    let dropped = Arc::new(AtomicBool::new(false));
    let socket = BlockedSocket {
        incoming: VecDeque::new(),
        dropped: dropped.clone(),
    };
    let error = timeout(Duration::from_millis(2100), websocket::close(socket, true))
        .await
        .expect("close exceeded its absolute budget")
        .unwrap_err();
    assert!(error.to_string().contains("two seconds"));
    assert!(dropped.load(SeqCst));
}

#[tokio::test(start_paused = true)]
async fn stop_interrupts_a_blocked_session_which_releases_after_its_close_budget() {
    let dropped = Arc::new(AtomicBool::new(false));
    let socket = BlockedSocket {
        incoming: VecDeque::from([Message::Text("queued".into())]),
        dropped: dropped.clone(),
    };
    let mut app = App::new();
    app.service_fn("sessions", move |ctx| async move {
        ctx.spawn("outgoing", websocket::session(socket, ctx.stop_token()))?;
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    app.ready().await.unwrap();
    tokio::task::yield_now().await;
    app.stop();
    assert!(timeout(Duration::from_secs(1), app.wait()).await.is_err());
    assert!(!dropped.load(SeqCst));
    let report = app.wait().await.unwrap();
    assert!(dropped.load(SeqCst));
    assert_eq!(
        report.service("sessions").unwrap().work.failed,
        1,
        "the close budget expiry is the session's error"
    );
}

#[tokio::test]
async fn a_later_failure_drains_http_work_before_its_provider_and_cleanup() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let failure = CancellationToken::new();
    let mut app = App::new();
    let cleaned = provider(&mut app, || {});
    let connector = {
        let (entered, release) = (entered.clone(), release.clone());
        serve_in_memory(&mut app, Spec::new("http").after("provider"), move |_| {
            Router::new().route(
                "/held",
                get(move || {
                    let (entered, release) = (entered.clone(), release.clone());
                    async move {
                        entered.cancel();
                        release.cancelled().await;
                        "drained"
                    }
                }),
            )
        })
    };
    {
        let failure = failure.clone();
        app.service_fn(Spec::new("failing").after("http"), move |_ctx| async move {
            failure.cancelled().await;
            Err(io::Error::other("later startup failed").into())
        });
    }
    let app = app.start().unwrap();
    let client = tokio::spawn(async move { connector.request("/held").await });
    bounded(entered.cancelled()).await;
    failure.cancel();
    bounded(async {
        while app
            .snapshot()
            .service("failing")
            .unwrap()
            .failures
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(still_owned(&app).await);
    assert!(!cleaned.load(SeqCst));
    assert_eq!(
        app.snapshot().service("provider").unwrap().phase,
        ServicePhase::Running,
        "the provider was stopped before HTTP work released"
    );
    release.cancel();
    assert!(bounded(client).await.unwrap().contains("drained"));
    let report = bounded(app.wait()).await.unwrap();
    assert!(!report.is_success());
    assert!(cleaned.load(SeqCst));
}
