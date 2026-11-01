//! Example-local pre-admission and bounded WebSocket echo sessions.

use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use miju_services::{BoxError, SpawnError, WorkContext, WorkScope, WorkSpec};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout_at},
};

/// Reserve responsibility before handing the socket to the framework upgrade.
pub(crate) async fn upgrade(State(scope): State<WorkScope>, upgrade: WebSocketUpgrade) -> Response {
    match reserve(&scope) {
        Ok(sender) => upgrade
            .on_upgrade(move |socket| async move {
                // No business resource or scope is captured by the framework callback.
                let _ = sender.send(socket);
            })
            .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

// The transfer boundary is separate so tests can delay a real framework callback
// without adding a delay or a bypass switch to the shipped upgrade handler.
pub(crate) fn reserve(scope: &WorkScope) -> Result<oneshot::Sender<WebSocket>, SpawnError> {
    let (sender, receiver) = oneshot::channel();
    scope.spawn(WorkSpec::new("websocket-session"), move |work| async move {
        let socket = tokio::select! {
            biased;
            _ = work.stopping() => return Ok::<(), BoxError>(()),
            socket = receiver => match socket {
                Ok(socket) => socket,
                Err(_) => return Ok(()),
            },
        };
        session(socket, work).await
    })?;
    Ok(sender)
}

/// One absolute budget includes Close send, flush and peer acknowledgement.
pub(crate) async fn close<S, E>(mut socket: S, initiate: bool) -> Result<(), BoxError>
where
    S: Stream<Item = Result<Message, E>> + Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    let result = timeout_at(deadline, async {
        if !initiate {
            // Tungstenite queues the peer's Close response while reading it.
            // Flush that response rather than starting a second handshake.
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
    // Transport release precedes publication of the ordinary work result.
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
    let (sender, mut receiver) = mpsc::channel(32);
    let outcome = {
        let read = async {
            while let Some(message) = stream.next().await {
                match message? {
                    Message::Close(_) => return Ok::<bool, BoxError>(false),
                    message @ (Message::Text(_) | Message::Binary(_)) => {
                        sender.try_send(message).map_err(|_| {
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
            while let Some(message) = receiver.recv().await {
                sink.send(message).await?;
            }
            Ok::<bool, BoxError>(true)
        };
        // Dropping these borrowing futures interrupts ordinary data writes and
        // leaves both transport halves owned here for the bounded Close phase.
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
