//! A real loopback echo and orderly shutdown; application code owns output policy.

#[path = "support/udp.rs"]
mod udp;
use miju_services::{Application, BoxError, ServiceSpec, service_fn};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{net::UdpSocket, sync::oneshot};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let (bound, address) = oneshot::channel();
    let counters = Arc::new(udp::UdpCounters::default());
    let observed = counters.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("udp"),
            service_fn(move |context| {
                udp::serve(
                    context,
                    "127.0.0.1:0".parse().expect("literal address"),
                    bound,
                    counters,
                    |packet, peer, socket, _work| async move {
                        socket.send_to(&packet, peer).await?;
                        Ok(())
                    },
                )
            }),
        )
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = app.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    let exchange = async {
        if ready.is_ok() {
            let server = address.await?;
            let client = UdpSocket::bind("127.0.0.1:0").await?;
            client.send_to(b"device-status", server).await?;
            let mut reply = [0; 64];
            let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut reply))
                .await??;
            assert_eq!(&reply[..n], b"device-status");
        }
        Ok::<(), BoxError>(())
    }
    .await;
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    println!(
        "{budget:?}\n{exit:?}; accepted={}, dropped={}",
        observed.accepted.load(Ordering::Relaxed),
        observed.dropped_at_capacity.load(Ordering::Relaxed)
    );
    ready?;
    exchange?;
    budget?;
    if !exit?.is_success() {
        return Err(std::io::Error::other("UDP lifecycle failed").into());
    }
    Ok(())
}
