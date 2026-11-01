//! Real loopback datagrams exercise example admission and actual shutdown.

#[path = "../examples/support/udp.rs"]
mod udp;
use miju_services::{Application, BoxError, DrainObservation, Health, ServiceSpec, service_fn};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{Notify, oneshot},
};

async fn eventually(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observable condition");
}

async fn exercise() {
    let counters = Arc::new(udp::UdpCounters::default());
    let gate = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let c = counters.clone();
    let g = gate.clone();
    let entry = entered.clone();
    let cleanup = cleaned.clone();
    let (bound, address) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("resource"),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("close", move || async move {
                        cleanup.store(true, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context.set_health(Health::Healthy).unwrap();
                context.ready().unwrap();
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("udp")
                .depends_on("resource")
                .max_work(NonZeroUsize::new(1).unwrap()),
            service_fn(move |context| {
                udp::serve(
                    context,
                    "127.0.0.1:0".parse().unwrap(),
                    bound,
                    c,
                    move |packet, peer, socket, _| {
                        let gate = g.clone();
                        let entered = entry.clone();
                        async move {
                            entered.notify_one();
                            gate.notified().await;
                            socket.send_to(&packet, peer).await?;
                            Ok(())
                        }
                    },
                )
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let observer = app.observer();
    observer.ready(Duration::from_secs(3)).await.unwrap();
    let address = address.await.unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"first", address).await.unwrap();
    entered.notified().await;
    client.send_to(b"full", address).await.unwrap();
    eventually(|| counters.dropped_at_capacity.load(Ordering::SeqCst) == 1).await;
    assert_eq!(counters.accepted.load(Ordering::SeqCst), 1);
    app.stop();
    eventually(|| {
        observer
            .snapshot()
            .services
            .iter()
            .any(|s| s.id.as_str() == "udp" && s.stopping)
    })
    .await;
    assert!(!cleaned.load(Ordering::SeqCst));
    assert!(
        !observer
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "resource")
            .unwrap()
            .stopping
    );
    client.send_to(b"late", address).await.unwrap();
    assert!(matches!(
        observer.drain(Duration::from_millis(20)).await,
        DrainObservation::Pending(_)
    ));
    assert_eq!(counters.accepted.load(Ordering::SeqCst), 1);
    gate.notify_one();
    let mut reply = [0; 16];
    let (n, _) = tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply[..n], b"first");
    let exit = observer.wait().await.unwrap();
    assert!(exit.is_success());
    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(counters.accepted.load(Ordering::SeqCst), 1);
    let rebound = UdpSocket::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test(flavor = "current_thread")]
async fn datagrams_capacity_and_drain_current_thread() {
    exercise().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datagrams_capacity_and_drain_multi_thread() {
    exercise().await;
}

#[tokio::test]
async fn bind_failure_is_not_ready_and_rolls_back_provider() {
    let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = cleaned.clone();
    let (bound, _) = oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("close", move || async move {
                        flag.store(true, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context.set_health(Health::Healthy).unwrap();
                context.ready().unwrap();
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("udp").depends_on("provider"),
            service_fn(move |context| {
                udp::serve(
                    context,
                    address,
                    bound,
                    Arc::new(udp::UdpCounters::default()),
                    |_, _, _, _| async { Ok(()) },
                )
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let observer = app.observer();
    assert!(observer.ready(Duration::from_secs(3)).await.is_err());
    let exit = observer.wait().await.unwrap();
    assert!(!exit.is_success());
    assert!(cleaned.load(Ordering::SeqCst));
}
