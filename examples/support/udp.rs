//! Example-local UDP admission policy, shared with its real socket tests.

use miju_services::{BoxError, Health, ServiceContext, SpawnError, WorkContext, WorkSpec};
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{net::UdpSocket, sync::oneshot};

/// Application counters; rejected datagrams are deliberately not queued.
#[derive(Default, Debug)]
pub(crate) struct UdpCounters {
    pub received: AtomicU64,
    pub accepted: AtomicU64,
    pub dropped_at_capacity: AtomicU64,
}

/// Bind before readiness, then admit each datagram through the owner's bounded scope.
pub(crate) async fn serve<F, Fut>(
    context: ServiceContext,
    address: SocketAddr,
    bound: oneshot::Sender<SocketAddr>,
    counters: Arc<UdpCounters>,
    handle: F,
) -> Result<(), BoxError>
where
    F: Fn(Vec<u8>, SocketAddr, Arc<UdpSocket>, WorkContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    let socket = Arc::new(UdpSocket::bind(address).await?);
    let _ = bound.send(socket.local_addr()?);
    let handle = Arc::new(handle);
    match context
        .set_health(Health::Healthy)
        .and_then(|()| context.ready())
    {
        Ok(()) => {}
        Err(_) if context.is_stopping() => {}
        Err(error) => return Err(error.into()),
    }
    let mut buffer = vec![0; 65535];
    loop {
        let received = tokio::select! {
            biased;
            _ = context.stopping() => break,
            received = socket.recv_from(&mut buffer) => received,
        };
        let (length, peer) = received?;
        counters.received.fetch_add(1, Ordering::Relaxed);
        let packet = buffer[..length].to_vec();
        let handler = handle.clone();
        let socket = socket.clone();
        match context
            .work()
            .spawn(WorkSpec::new("datagram"), move |work| async move {
                handler(packet, peer, socket, work).await
            }) {
            Ok(_) => {
                counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(SpawnError::AtCapacity) => {
                counters.dropped_at_capacity.fetch_add(1, Ordering::Relaxed);
            }
            Err(SpawnError::Closed) if context.is_stopping() => break,
            Err(error) => return Err(error.into()),
        }
    }
    context.work().close();
    context.work().wait().await;
    Ok(())
}
