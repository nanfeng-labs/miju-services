//! An in-memory transport for the protocol tests.
//!
//! `MemoryListener` plugs into `axum::serve` through `axum::serve::Listener`,
//! so axum's and hyper's own connection tasks run; only the operating-system
//! socket is replaced by `tokio::io::duplex`. The release ordering the leases
//! guard is the same code on both transports. Socket behaviour itself (bind,
//! port reuse, close timing) is covered by `tests/protocols_net.rs`.

use axum::serve::Listener;
use std::{future::Future, io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::mpsc,
    time::timeout,
};

pub(crate) async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(4), future)
        .await
        .expect("protocol stalled")
}

pub(crate) struct MemoryListener(mpsc::UnboundedReceiver<DuplexStream>);

impl Listener for MemoryListener {
    type Io = DuplexStream;
    type Addr = &'static str;

    async fn accept(&mut self) -> (DuplexStream, &'static str) {
        match self.0.recv().await {
            Some(io) => (io, "memory"),
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<&'static str> {
        Ok("memory")
    }
}

/// The client side: opens in-memory connections to a `MemoryListener`.
#[derive(Clone)]
pub(crate) struct Connector(mpsc::UnboundedSender<DuplexStream>);

pub(crate) fn memory() -> (MemoryListener, Connector) {
    let (accept, incoming) = mpsc::unbounded_channel();
    (MemoryListener(incoming), Connector(accept))
}

impl Connector {
    pub(crate) fn connect(&self) -> DuplexStream {
        let (client, server) = tokio::io::duplex(64 * 1024);
        self.0.send(server).expect("the listener is gone");
        client
    }

    /// One HTTP/1.1 request with `Connection: close`; the whole response.
    pub(crate) async fn request(&self, path: &str) -> String {
        let mut client = self.connect();
        client
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        bounded(client.read_to_string(&mut response)).await.unwrap();
        response
    }
}
