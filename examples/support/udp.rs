//! Bind before ready; drop overload and drain accepted datagrams before closing.

use miju_services::{
    BoxError, Service, ServiceContext,
    work::{SpawnError, WorkContext, WorkSpec},
};
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{net::UdpSocket, sync::oneshot};

#[derive(Debug, Default)]
pub(crate) struct UdpCounters {
    pub(crate) received: AtomicU64,
    pub(crate) accepted: AtomicU64,
    pub(crate) dropped_at_capacity: AtomicU64,
}

pub(crate) struct UdpService<F> {
    address: SocketAddr,
    handle: F,
    pub(crate) counters: Arc<UdpCounters>,
    pub(crate) bound: Option<oneshot::Sender<SocketAddr>>,
}

impl<F> UdpService<F> {
    pub(crate) fn new<Fut>(address: SocketAddr, handle: F) -> Self
    where
        F: Fn(Vec<u8>, SocketAddr, Arc<UdpSocket>, WorkContext) -> Fut,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        Self {
            address,
            handle,
            counters: Arc::default(),
            bound: None,
        }
    }
}

impl<F, Fut> Service for UdpService<F>
where
    F: Fn(Vec<u8>, SocketAddr, Arc<UdpSocket>, WorkContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    type Error = BoxError;

    async fn run(self, context: ServiceContext) -> Result<(), BoxError> {
        let socket = Arc::new(UdpSocket::bind(self.address).await?);
        if let Some(bound) = self.bound {
            let _ = bound.send(socket.local_addr()?);
        }
        let handle = Arc::new(self.handle);
        context.ready();
        let mut buffer = vec![0; 65535];
        loop {
            let received = tokio::select! {
                biased;
                _ = context.stopping() => break,
                received = socket.recv_from(&mut buffer) => received,
            };
            let (length, peer) = received?;
            self.counters.received.fetch_add(1, Ordering::Relaxed);
            let packet = buffer[..length].to_vec();
            let handler = handle.clone();
            let socket = socket.clone();
            match context
                .work()
                .spawn(WorkSpec::new("datagram"), move |work| async move {
                    handler(packet, peer, socket, work).await
                }) {
                Ok(_) => {
                    self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                }
                Err(SpawnError::AtCapacity) => {
                    self.counters
                        .dropped_at_capacity
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(SpawnError::Closed) if context.is_stopping() => break,
                Err(error) => return Err(error.into()),
            }
        }
        // Root return closes admission; App retains accepted work until release.
        Ok(())
    }
}
