//! Example-local echo sessions admitted before the HTTP upgrade is accepted.

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
use miju_services::{
    BoxError, Service, ServiceContext,
    work::{SpawnError, WorkContext, WorkScope, WorkSpec},
};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

pub(crate) struct WebSocketService {
    address: SocketAddr,
    pub(crate) bound: Option<oneshot::Sender<SocketAddr>>,
}

impl WebSocketService {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Self {
            address,
            bound: None,
        }
    }
}

impl Service for WebSocketService {
    type Error = BoxError;

    async fn run(self, context: ServiceContext) -> Result<(), BoxError> {
        let users = TaskTracker::new();
        let router = Router::new()
            .route("/ws", get(upgrade))
            .with_state((context.work().clone(), users.clone()));
        let mut service = HttpService::new(self.address, router);
        service.bound = self.bound;
        service.users = users;
        service.run(context).await
    }
}

pub(crate) async fn upgrade(
    State((scope, users)): State<(WorkScope, TaskTracker)>,
    upgrade: WebSocketUpgrade,
) -> Response {
    match reserve(&scope, &users) {
        Ok(handoff) => upgrade
            .on_upgrade(move |socket| {
                handoff.deliver(socket);
                async {}
            })
            .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub(crate) struct Handoff {
    sender: oneshot::Sender<WebSocket>,
    lease: TaskTrackerToken,
}

impl Handoff {
    pub(crate) fn deliver(self, socket: WebSocket) {
        // A rejected socket drops before releasing the framework callback's lease.
        if let Err(socket) = self.sender.send(socket) {
            drop(socket);
        }
        drop(self.lease);
    }
}

pub(crate) fn reserve(scope: &WorkScope, users: &TaskTracker) -> Result<Handoff, SpawnError> {
    let (send, receive) = oneshot::channel();
    scope.spawn(WorkSpec::new("websocket-session"), move |work| async move {
        let socket = tokio::select! {
            biased;
            _ = work.stopping() => return Ok::<(), BoxError>(()),
            socket = receive => match socket {
                Ok(socket) => socket,
                Err(_) => return Ok(()),
            },
        };
        session(socket, work).await
    })?;
    Ok(Handoff {
        sender: send,
        lease: users.token(),
    })
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

pub(crate) async fn session<S, E>(socket: S, work: WorkContext) -> Result<(), BoxError>
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
            _ = work.stopping() => Ok(true),
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
