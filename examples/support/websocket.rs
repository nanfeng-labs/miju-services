//! Example-local echo sessions, admitted before the HTTP upgrade is answered.
//!
//! A session is reserved before `101`: a permit from the session limit, a
//! lease held by the upgrade callback until it hands the socket over or drops
//! it, and the session itself as managed work. Without a permit the answer is
//! 503. Sessions use a bounded outbound queue and close within two seconds.

use super::http::HttpService;
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use miju_services::{BoxError, Context, Lease, Rejected, Scope, Service};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

/// What the upgrade route needs; cloned into each request.
#[derive(Clone)]
pub(crate) struct Sessions {
    scope: Scope,
    stop: CancellationToken,
    permits: Arc<Semaphore>,
}

impl Sessions {
    pub(crate) fn new(ctx: &Context, max_sessions: usize) -> Self {
        Self {
            scope: ctx.scope(),
            stop: ctx.stop_token(),
            permits: Arc::new(Semaphore::new(max_sessions)),
        }
    }
}

/// The `/ws` echo route.
pub(crate) fn router(sessions: Sessions) -> Router {
    Router::new()
        .route("/ws", get(upgrade))
        .with_state(sessions)
}

/// An HTTP service serving only the echo route.
pub(crate) struct WebSocketService {
    address: SocketAddr,
    max_sessions: usize,
}

impl WebSocketService {
    pub(crate) fn new(address: SocketAddr, max_sessions: usize) -> Self {
        Self {
            address,
            max_sessions,
        }
    }
}

impl Service for WebSocketService {
    type Error = BoxError;

    async fn run(self, ctx: Context) -> Result<(), BoxError> {
        let router = router(Sessions::new(&ctx, self.max_sessions));
        HttpService::new(self.address, router).run(ctx).await
    }
}

async fn upgrade(State(sessions): State<Sessions>, upgrade: WebSocketUpgrade) -> Response {
    match reserve(&sessions) {
        Ok(handoff) => upgrade
            .on_upgrade(move |socket| {
                handoff.deliver(socket);
                async {}
            })
            .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Held by the upgrade callback until it hands the socket over.
pub(crate) struct Handoff {
    sender: oneshot::Sender<WebSocket>,
    lease: Lease,
}

impl Handoff {
    pub(crate) fn deliver(self, socket: WebSocket) {
        // A socket the session no longer wants is dropped before the lease.
        if let Err(socket) = self.sender.send(socket) {
            drop(socket);
        }
        drop(self.lease);
    }
}

/// Reserve a session: a permit, a lease for the callback, and the session work.
pub(crate) fn reserve(sessions: &Sessions) -> Result<Handoff, Rejected> {
    let permit = sessions
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| Rejected::Full)?;
    let lease = sessions.scope.lease("websocket-upgrade")?;
    let (sender, receive) = oneshot::channel();
    let stop = sessions.stop.clone();
    sessions
        .scope
        .spawn("websocket-session", run_session(receive, stop, permit))?;
    Ok(Handoff { sender, lease })
}

async fn run_session(
    socket: oneshot::Receiver<WebSocket>,
    stop: CancellationToken,
    _permit: OwnedSemaphorePermit,
) -> Result<(), BoxError> {
    let socket = tokio::select! {
        biased;
        _ = stop.cancelled() => return Ok(()),
        socket = socket => match socket {
            Ok(socket) => socket,
            // The upgrade failed; nothing was handed over.
            Err(_) => return Ok(()),
        },
    };
    session(socket, stop).await
}

/// One deadline covers Close send, flush and acknowledgement, followed by drop.
pub(crate) async fn close<S, E>(mut socket: S, initiate: bool) -> Result<(), BoxError>
where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    let result = timeout_at(deadline, async {
        if !initiate {
            // Reading peer Close queues its reply; flush instead of sending twice.
            socket.flush().await?;
            return Ok::<(), E>(());
        }
        socket.send(Message::Close(None)).await?;
        while let Some(message) = socket.next().await {
            if matches!(message?, Message::Close(_)) {
                break;
            }
        }
        Ok::<(), E>(())
    })
    .await;
    drop(socket);
    match result {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "WebSocket close was not confirmed within two seconds",
        )
        .into()),
    }
}

/// Echo until the peer closes, the service stops, or the outbound queue overflows.
pub(crate) async fn session<S, E>(socket: S, stop: CancellationToken) -> Result<(), BoxError>
where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let (mut sink, mut stream) = socket.split();
    let (send, mut receive) = mpsc::channel(32);
    let outcome = {
        let read = async {
            while let Some(message) = stream.next().await {
                match message? {
                    Message::Close(_) => return Ok::<bool, BoxError>(false),
                    message @ (Message::Text(_) | Message::Binary(_)) => {
                        send.try_send(message).map_err(|_| {
                            std::io::Error::other(
                                "WebSocket outbound queue full; closing slow connection",
                            )
                        })?;
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                }
            }
            Ok(false)
        };
        let write = async {
            while let Some(message) = receive.recv().await {
                sink.send(message).await?;
            }
            Ok::<bool, BoxError>(true)
        };
        // Cancel borrowing IO futures, retaining both halves for the Close phase.
        tokio::select! {
            biased;
            _ = stop.cancelled() => Ok(true),
            result = read => result,
            result = write => result,
        }
    };
    let socket = sink
        .reunite(stream)
        .map_err(|_| std::io::Error::other("WebSocket halves belong to different sessions"))?;
    let closing = close(socket, !matches!(outcome, Ok(false))).await;
    outcome.and(closing)
}
