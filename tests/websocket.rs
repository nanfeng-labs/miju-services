//! Real upgrades use the same example-local completion and session adapters.

#[path = "../examples/support/http.rs"]
mod http;

#[path = "../examples/support/websocket.rs"]
mod websocket;
use axum::{Router, routing::get};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use miju_services::{Application, BoxError, Health, ServiceSpec, WorkSpec, service_fn};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::test]
async fn thirty_two_real_upgrades_echo_and_stop_without_detached_sessions() {
    for _ in 0..32 {
        let (tx, rx) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("ws"),
                service_fn(move |context| async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    tx.send(listener.local_addr()?).unwrap();
                    let router = Router::new()
                        .route("/ws", get(websocket::upgrade))
                        .with_state(context.work().clone());
                    let server = context
                        .work()
                        .spawn(WorkSpec::aggregate("http"), move |work| {
                            http::serve_http(listener, router, work)
                        })?;
                    if let Err(error) = context
                        .set_health(Health::Healthy)
                        .and_then(|()| context.ready())
                        && !context.is_stopping()
                    {
                        return Err(error.into());
                    }
                    server.wait().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.observer().ready(Duration::from_secs(2)).await.unwrap();
        let (mut socket, response) =
            tokio_tungstenite::connect_async(format!("ws://{}/ws", rx.await.unwrap()))
                .await
                .unwrap();
        assert_eq!(response.status(), 101);
        socket
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                vec![7; 256].into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap().into_data(),
            vec![7; 256]
        );
        app.stop();
        assert!(socket.next().await.unwrap().unwrap().is_close());
        socket.flush().await.unwrap();
        app.shutdown().await.unwrap();
        let exit = app.observer().wait().await.unwrap();
        assert!(exit.is_success(), "{exit:?}");
        assert_eq!(exit.services[0].work.completed, 2);
        assert!(exit.services[0].work.active.is_empty());
    }
}

struct Stalled {
    dropped: Arc<AtomicBool>,
    ready: bool,
}

impl Drop for Stalled {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl Stream for Stalled {
    type Item = Result<axum::extract::ws::Message, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Sink<axum::extract::ws::Message> for Stalled {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, _: axum::extract::ws::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn close_budget_covers_both_send_readiness_and_flush_and_drops_before_result() {
    for ready in [false, true] {
        let dropped = Arc::new(AtomicBool::new(false));
        let start = tokio::time::Instant::now();
        let result = websocket::close(
            Stalled {
                dropped: dropped.clone(),
                ready,
            },
            true,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn closed_or_full_scope_rejects_real_upgrade_with_503() {
    for closed in [false, true] {
        let (tx, rx) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("ws").max_work(std::num::NonZeroUsize::new(1).unwrap()),
                service_fn(move |context| async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    let address = listener.local_addr()?;
                    let scope = context.work().clone();
                    let router = Router::new()
                        .route("/ws", get(websocket::upgrade))
                        .with_state(scope.clone());
                    // The aggregate occupies the only slot, so an upgrade must fail.
                    let server = scope.spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                    if closed {
                        scope.close();
                    }
                    tx.send(address).unwrap();
                    context.set_health(Health::Healthy)?;
                    context.ready()?;
                    server.wait().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.observer().ready(Duration::from_secs(2)).await.unwrap();
        let error = tokio_tungstenite::connect_async(format!("ws://{}/ws", rx.await.unwrap()))
            .await
            .unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), 503)
            }
            other => panic!("expected HTTP rejection: {other}"),
        }
        app.shutdown().await.unwrap();
        let exit = app.observer().wait().await.unwrap();
        assert!(exit.is_success(), "{exit:?}");
        assert_eq!(exit.services[0].work.completed, 1);
    }
}

#[tokio::test]
async fn late_framework_callback_after_101_cannot_reanimate_stopped_work() {
    use axum::{extract::WebSocketUpgrade, response::IntoResponse};
    use tokio::sync::Notify;
    let release = Arc::new(Notify::new());
    let callback_started = Arc::new(Notify::new());
    let rejected = Arc::new(AtomicBool::new(false));
    let callback_done = Arc::new(Notify::new());
    let (tx, rx) = oneshot::channel();
    let held = release.clone();
    let entered = callback_started.clone();
    let refused = rejected.clone();
    let done = callback_done.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("ws"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                tx.send(listener.local_addr()?).unwrap();
                let scope = context.work().clone();
                let router = Router::new().route(
                    "/ws",
                    get(move |upgrade: WebSocketUpgrade| {
                        let scope = scope.clone();
                        let held = held.clone();
                        let entered = entered.clone();
                        let refused = refused.clone();
                        let done = done.clone();
                        async move {
                            let sender = websocket::reserve(&scope).unwrap();
                            upgrade
                                .on_upgrade(move |socket| async move {
                                    // Test-only transport barrier, with no business resource.
                                    entered.notify_one();
                                    held.notified().await;
                                    let result = sender.send(socket);
                                    refused.store(result.is_err(), Ordering::SeqCst);
                                    drop(result);
                                    done.notify_one();
                                })
                                .into_response()
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
                server.wait().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(2)).await.unwrap();
    let (socket, response) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws", rx.await.unwrap()))
            .await
            .unwrap();
    assert_eq!(response.status(), 101);
    callback_started.notified().await;
    app.shutdown().await.unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success(), "{exit:?}");
    assert_eq!(exit.services[0].work.completed, 2);
    release.notify_one();
    callback_done.notified().await;
    assert!(rejected.load(Ordering::SeqCst));
    drop(socket);
}

struct Backpressured {
    remaining: usize,
    write_polled: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Backpressured {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl Stream for Backpressured {
    type Item = Result<axum::extract::ws::Message, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Pending;
        }
        self.remaining -= 1;
        Poll::Ready(Some(Ok(axum::extract::ws::Message::Binary(
            vec![1; 256].into(),
        ))))
    }
}

impl Sink<axum::extract::ws::Message> for Backpressured {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.write_polled.store(true, Ordering::SeqCst);
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _: axum::extract::ws::Message) -> Result<(), Self::Error> {
        unreachable!()
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn full_queue_and_stop_during_data_write_both_release_transport() {
    for full in [false, true] {
        let dropped = Arc::new(AtomicBool::new(false));
        let write_polled = Arc::new(AtomicBool::new(false));
        let transport = Backpressured {
            remaining: if full { 33 } else { 1 },
            dropped: dropped.clone(),
            write_polled: write_polled.clone(),
        };
        let (tx, rx) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("ws"),
                service_fn(move |context| async move {
                    let session = context
                        .work()
                        .spawn(WorkSpec::new("session"), move |work| {
                            websocket::session(transport, work)
                        })?;
                    tx.send(session).unwrap();
                    context.set_health(Health::Healthy)?;
                    context.ready()?;
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.observer().ready(Duration::from_secs(1)).await.unwrap();
        let session = rx.await.unwrap();
        while !write_polled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        if !full {
            session.stop();
        }
        let report = session.wait().await;
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            report.completion(),
            miju_services::WorkCompletion::Confirmed
        );
        match report.outcome() {
            miju_services::WorkOutcome::Failed(error) => {
                let text = error.to_string();
                assert!(
                    text.contains(if full { "queue full" } else { "two seconds" }),
                    "{text}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
        app.shutdown().await.unwrap();
        assert!(app.observer().wait().await.unwrap().is_success());
    }
}

#[tokio::test]
async fn review_peer_initiated_clean_close_is_not_a_work_failure() {
    let (tx, rx) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("ws"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                tx.send(listener.local_addr()?).unwrap();
                let router = Router::new()
                    .route("/ws", get(websocket::upgrade))
                    .with_state(context.work().clone());
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                context.set_health(Health::Healthy)?;
                context.ready()?;
                server.wait().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(2)).await.unwrap();
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws", rx.await.unwrap()))
            .await
            .unwrap();
    socket.close(None).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), socket.next()).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if app.observer().snapshot().services[0].work.completed >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.stop();
    let exit = app.observer().wait().await.unwrap();
    assert_eq!(
        exit.services[0].work.failed, 0,
        "clean peer close: {exit:?}"
    );
}

struct DelayedFlush {
    timer: Pin<Box<tokio::time::Sleep>>,
    dropped: Arc<AtomicBool>,
}

impl Drop for DelayedFlush {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl Stream for DelayedFlush {
    type Item = Result<axum::extract::ws::Message, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Sink<axum::extract::ws::Message> for DelayedFlush {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _: axum::extract::ws::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        use std::future::Future;
        self.timer.as_mut().poll(cx).map(|()| Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn close_does_not_restart_budget_after_slow_flush() {
    let dropped = Arc::new(AtomicBool::new(false));
    let start = tokio::time::Instant::now();
    let result = websocket::close(
        DelayedFlush {
            timer: Box::pin(tokio::time::sleep(Duration::from_millis(1500))),
            dropped: dropped.clone(),
        },
        true,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropped_upgrade_sender_releases_reservation_without_a_socket() {
    let (tx, rx) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("ws"),
            service_fn(move |context| async move {
                let sender = websocket::reserve(context.work())?;
                // An upgrade failure or callback cancellation drops this sole sender.
                drop(sender);
                context.work().close();
                context.work().wait().await;
                tx.send(()).unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    rx.await.unwrap();
    app.shutdown().await.unwrap();
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success(), "{exit:?}");
    assert_eq!(exit.services[0].work.completed, 1);
}

#[tokio::test]
async fn thirty_two_real_handshakes_racing_stop_leave_no_responsibility() {
    for _ in 0..32 {
        let (tx, rx) = oneshot::channel();
        let app = Application::builder()
            .service(
                ServiceSpec::new("ws"),
                service_fn(move |context| async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    tx.send(listener.local_addr()?).unwrap();
                    let router = Router::new()
                        .route("/ws", get(websocket::upgrade))
                        .with_state(context.work().clone());
                    let server = context
                        .work()
                        .spawn(WorkSpec::aggregate("http"), move |work| {
                            http::serve_http(listener, router, work)
                        })?;
                    if let Err(error) = context
                        .set_health(Health::Healthy)
                        .and_then(|()| context.ready())
                        && !context.is_stopping()
                    {
                        return Err(error.into());
                    }
                    server.wait().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();
        app.observer().ready(Duration::from_secs(2)).await.unwrap();
        let address = rx.await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let client_barrier = barrier.clone();
        let client = tokio::spawn(async move {
            client_barrier.wait().await;
            if let Ok((mut socket, _)) =
                tokio_tungstenite::connect_async(format!("ws://{address}/ws")).await
            {
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(message) if message.is_close() => {
                            let _ = socket.flush().await;
                            break;
                        }
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
        });
        barrier.wait().await;
        app.stop();
        tokio::time::timeout(Duration::from_secs(3), client)
            .await
            .unwrap()
            .unwrap();
        app.shutdown().await.unwrap();
        let exit = app.observer().wait().await.unwrap();
        assert!(exit.is_success(), "{exit:?}");
        assert!((1..=2).contains(&exit.services[0].work.completed));
        assert!(exit.services[0].work.active.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn global_abort_before_close_budget_still_drops_transport() {
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let (tx, rx) = oneshot::channel();
    let app = Application::builder()
        .options(miju_services::AppOptions {
            shutdown_grace: Duration::from_millis(10),
            ..Default::default()
        })
        .service(
            ServiceSpec::new("ws"),
            service_fn(move |context| async move {
                let session = context
                    .work()
                    .spawn(WorkSpec::new("session"), move |work| {
                        websocket::session(
                            Stalled {
                                dropped: observed,
                                ready: false,
                            },
                            work,
                        )
                    })?;
                tx.send(session).unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    let session = rx.await.unwrap();
    // Let the accepted closure start before initiating stop.
    tokio::task::yield_now().await;
    let start = tokio::time::Instant::now();
    app.stop();
    let report = session.wait().await;
    assert!(report.was_aborted(), "{report:?}");
    assert!(dropped.load(Ordering::SeqCst));
    assert!(start.elapsed() < Duration::from_secs(2));
    app.shutdown().await.unwrap();
    app.observer().wait().await.unwrap();
}

#[tokio::test]
async fn retained_framework_sender_keeps_reservation_until_stop() {
    let (tx, rx) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("ws"),
            service_fn(move |context| async move {
                let sender = websocket::reserve(context.work())?;
                tx.send(sender).unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    app.observer().ready(Duration::from_secs(1)).await.unwrap();
    let sender = rx.await.unwrap();
    assert!(!sender.is_closed());
    let snapshot = app.observer().snapshot();
    assert_eq!(snapshot.services[0].work.active.len(), 1);
    assert_eq!(snapshot.services[0].work.completed, 0);
    // The client need not exist; a retained framework sender is still pending.
    app.shutdown().await.unwrap();
    assert!(sender.is_closed());
    let exit = app.observer().wait().await.unwrap();
    assert!(exit.is_success(), "{exit:?}");
    assert_eq!(exit.services[0].work.completed, 1);
    drop(sender);
}
