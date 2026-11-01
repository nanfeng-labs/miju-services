//! Real HTTP sockets through the product aggregate and private destructor witness.

#[path = "../examples/support/http.rs"]
mod http;
use axum::{
    Router,
    body::{Body, Bytes},
    routing::get,
};
use miju_services::{
    AppHandle, AppOptions, Application, BoxError, Health, ServiceSpec, WorkCompletion, WorkOutcome,
    WorkSpec, service_fn,
};
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

async fn done<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), f)
        .await
        .expect("HTTP operation timed out")
}

async fn request(addr: SocketAddr) -> TcpStream {
    let mut stream = done(TcpStream::connect(addr)).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    stream
}

async fn start(
    router: Router,
    options: AppOptions,
    cleaned: Arc<AtomicUsize>,
) -> (AppHandle, SocketAddr) {
    let (send, address) = tokio::sync::oneshot::channel();
    let app = Application::builder()
        .options(options)
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("resource", move || async move {
                        cleaned.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("http").depends_on("provider"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let addr = listener.local_addr()?;
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("server"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                match context
                    .set_health(Health::Healthy)
                    .and_then(|()| context.ready())
                {
                    Ok(()) => {}
                    Err(_) if context.is_stopping() => {}
                    Err(e) => return Err(BoxError::from(e)),
                }
                send.send(addr).unwrap();
                let result = server.wait().await;
                match (result.completion(), result.outcome()) {
                    (WorkCompletion::Confirmed, WorkOutcome::Succeeded) => Ok(()),
                    (WorkCompletion::Confirmed, WorkOutcome::CancelledBeforeStart)
                        if context.is_stopping() =>
                    {
                        Ok(())
                    }
                    _ => Err(std::io::Error::other(format!("HTTP aggregate {result:?}")).into()),
                }
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let addr = done(address).await.unwrap();
    done(app.observer().ready(Duration::from_secs(2)))
        .await
        .unwrap();
    (app, addr)
}

#[tokio::test]
async fn normal_http_response_then_shutdown_releases_listener() {
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(
        Router::new().route("/", get(|| async { "hello" })),
        AppOptions::default(),
        cleaned.clone(),
    )
    .await;
    let mut stream = request(addr).await;
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    assert!(response.contains("200 OK"));
    assert!(response.ends_with("hello"));
    drop(stream);
    app.stop();
    assert!(done(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    done(async {
        loop {
            if TcpStream::connect(addr).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let rebound = TcpListener::bind(addr).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn pending_handler_keeps_aggregate_provider_and_cleanup_after_budget() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let enter = entered.clone();
    let gate = release.clone();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let router = Router::new().route(
        "/",
        get(move || {
            let enter = enter.clone();
            let gate = gate.clone();
            async move {
                enter.cancel();
                gate.cancelled().await;
                "finished"
            }
        }),
    );
    let (app, addr) = start(
        router,
        AppOptions {
            shutdown_grace: Duration::from_millis(20),
            abort_wait: Duration::from_millis(20),
            ..Default::default()
        },
        cleaned.clone(),
    )
    .await;
    let mut stream = request(addr).await;
    done(entered.cancelled()).await;
    let budget = done(app.shutdown()).await.unwrap();
    assert!(matches!(
        budget.as_ref(),
        miju_services::ShutdownReport::Incomplete(_)
    ));
    let snapshot = app.observer().snapshot();
    let provider = snapshot
        .services
        .iter()
        .find(|s| s.id.as_str() == "provider")
        .unwrap();
    assert!(!provider.stopping);
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    release.cancel();
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    assert!(response.ends_with("finished"));
    drop(stream);
    let exit = done(app.observer().wait()).await.unwrap();
    assert!(exit.is_drained());
    assert!(!exit.is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn streaming_body_remains_owned_until_body_finishes() {
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let enter = entered.clone();
    let gate = release.clone();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let router = Router::new().route(
        "/",
        get(move || {
            let enter = enter.clone();
            let gate = gate.clone();
            async move {
                Body::from_stream(futures_util::stream::once(async move {
                    enter.cancel();
                    gate.cancelled().await;
                    Ok::<Bytes, Infallible>(Bytes::from_static(b"body-finished"))
                }))
            }
        }),
    );
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let mut stream = request(addr).await;
    done(entered.cancelled()).await;
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(10)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    release.cancel();
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    assert!(response.contains("body-finished"));
    drop(stream);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
}

#[derive(Default)]
struct DropGate {
    entered: CancellationToken,
    released: Mutex<bool>,
    condition: Condvar,
}

struct BlockingDrop(Arc<DropGate>);

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        // Keep the destructor blocked, but let Tokio hand off its scheduling core.
        // Otherwise the observer's timer can stall on the same runtime, preventing
        // the test from reaching Release and leaving this Condvar blocked forever.
        tokio::task::block_in_place(|| {
            self.0.entered.cancel();
            let mut released = self.0.released.lock().unwrap();
            while !*released {
                released = self.0.condition.wait(released).unwrap();
            }
        });
    }
}

struct Release(Arc<DropGate>);

impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.condition.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn disconnect_waits_for_handler_destructor_before_confirming_aggregate() {
    let gate = Arc::new(DropGate::default());
    let release = Release(gate.clone());
    let entered = CancellationToken::new();
    let enter = entered.clone();
    let captured = gate.clone();
    let router = Router::new().route(
        "/",
        get(move || {
            let captured = captured.clone();
            let enter = enter.clone();
            async move {
                let _guard = BlockingDrop(captured);
                enter.cancel();
                std::future::pending::<()>().await;
                "unused"
            }
        }),
    );
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let stream = request(addr).await;
    done(entered.cancelled()).await;
    drop(stream);
    done(gate.entered.cancelled()).await;
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    assert!(
        !app.observer()
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .stopping
    );
    drop(release);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn disconnect_waits_for_body_destructor_before_resource_cleanup() {
    let gate = Arc::new(DropGate::default());
    let release = Release(gate.clone());
    let entered = CancellationToken::new();
    let enter = entered.clone();
    let captured = gate.clone();
    let router = Router::new().route(
        "/",
        get(move || {
            let captured = captured.clone();
            let enter = enter.clone();
            async move {
                Body::from_stream(futures_util::stream::once(async move {
                    let _guard = BlockingDrop(captured);
                    enter.cancel();
                    std::future::pending::<()>().await;
                    Ok::<Bytes, Infallible>(Bytes::new())
                }))
            }
        }),
    );
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let stream = request(addr).await;
    done(entered.cancelled()).await;
    drop(stream);
    done(gate.entered.cancelled()).await;
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    drop(release);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
}

#[tokio::test]
async fn bind_conflict_is_required_failure_and_other_resource_rolls_back() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let record = cleaned.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("provider"),
            service_fn(move |context| async move {
                context
                    .defer_cleanup("rollback", move || async move {
                        record.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })
                    .unwrap();
                context.set_health(Health::Healthy)?;
                context.ready()?;
                context.stopping().await;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("http").depends_on("provider"),
            service_fn(move |_| async move {
                let _listener = TcpListener::bind(addr).await?;
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let exit = done(app.observer().wait()).await.unwrap();
    assert!(!exit.is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert!(
        exit.services
            .iter()
            .find(|s| s.id.as_str() == "http")
            .unwrap()
            .failure
            .is_some()
    );
    drop(occupied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn router_state_destructor_keeps_provider_until_actual_release() {
    let gate = Arc::new(DropGate::default());
    let release = Release(gate.clone());
    let state = Arc::new(BlockingDrop(gate.clone()));
    let router = Router::new()
        .route(
            "/",
            get(
                |axum::extract::State(_state): axum::extract::State<Arc<BlockingDrop>>| async {
                    "state-owned"
                },
            ),
        )
        .with_state(state);
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let mut stream = request(addr).await;
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    assert!(response.ends_with("state-owned"));
    drop(stream);
    app.stop();
    done(gate.entered.cancelled()).await;
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    assert!(
        !app.observer()
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .stopping
    );
    drop(release);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

async fn panic_destructor_still_holds_product_aggregate(body_panics: bool) {
    let gate = Arc::new(DropGate::default());
    let release = Release(gate.clone());
    let captured = gate.clone();
    let router = Router::new().route(
        "/",
        get(move || {
            let captured = captured.clone();
            async move {
                if body_panics {
                    Body::from_stream(futures_util::stream::once(async move {
                        let _guard = BlockingDrop(captured);
                        panic!("injected response body panic");
                        #[allow(unreachable_code)]
                        Ok::<Bytes, Infallible>(Bytes::new())
                    }))
                } else {
                    let _guard = BlockingDrop(captured);
                    panic!("injected handler panic");
                }
            }
        }),
    );
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let mut stream = request(addr).await;
    done(gate.entered.cancelled()).await;
    app.stop();
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    assert!(
        !app.observer()
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .stopping
    );
    drop(release);
    let mut response = Vec::new();
    // The connection may close cleanly or reset after framework task panic.
    let _ = done(stream.read_to_end(&mut response)).await;
    drop(stream);
    let exit = done(app.observer().wait()).await.unwrap();
    assert!(
        exit.is_success(),
        "framework panic is not a serve error: {exit:?}"
    );
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn handler_panic_drops_capture_before_aggregate_confirmation() {
    panic_destructor_still_holds_product_aggregate(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn body_panic_drops_capture_before_aggregate_confirmation() {
    panic_destructor_still_holds_product_aggregate(true).await;
}

#[tokio::test]
async fn admitted_http_survives_stop_rejecting_health_and_ready() {
    let (send, address) = tokio::sync::oneshot::channel();
    let rejected = Arc::new(AtomicUsize::new(0));
    let record = rejected.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("http"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let addr = listener.local_addr()?;
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("server"), move |work| {
                        http::serve_http(
                            listener,
                            Router::new().route("/", get(|| async { "admitted" })),
                            work,
                        )
                    })?;
                send.send(addr).unwrap();
                // Deterministically linearize admission before global stop and reports after it.
                context.stopping().await;
                assert!(context.set_health(Health::Healthy).is_err());
                assert!(context.ready().is_err());
                record.fetch_add(1, Ordering::SeqCst);
                let result = server.wait().await;
                assert_eq!(result.completion(), WorkCompletion::Confirmed);
                assert!(matches!(result.outcome(), WorkOutcome::Succeeded));
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let addr = done(address).await.unwrap();
    let mut stream = request(addr).await;
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    assert!(response.ends_with("admitted"));
    drop(stream);
    app.stop();
    assert!(done(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
    done(async {
        loop {
            if TcpStream::connect(addr).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[test]
fn http_aggregate_faults_retain_unconfirmed_responsibility_in_isolated_host() {
    use std::process::{Command, Stdio};
    for fault in ["error", "panic"] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "http_aggregate_fault_child", "--nocapture"])
            .env("MIJU_HTTP_FAULT_CHILD", fault)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut timed_out = false;
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                timed_out = true;
                child.kill().unwrap();
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!timed_out, "HTTP fault child timed out: {fault}");
        assert!(
            output.status.success(),
            "HTTP fault child {fault}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("HTTP_FAULT_UNCONFIRMED"));
    }
}

#[test]
fn http_aggregate_fault_child() {
    let Ok(fault) = std::env::var("MIJU_HTTP_FAULT_CHILD") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let cleaned = Arc::new(AtomicUsize::new(0));
        let record = cleaned.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let panics = fault == "panic";
        let app = Application::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_millis(20),
                abort_wait: Duration::from_millis(20),
                ..Default::default()
            })
            .service(
                ServiceSpec::new("provider"),
                service_fn(move |context| async move {
                    context.defer_cleanup("resource", move || async move {
                        record.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), BoxError>(())
                    })?;
                    context.set_health(Health::Healthy)?;
                    context.ready()?;
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .service(
                ServiceSpec::new("http").depends_on("provider"),
                service_fn(move |context| async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    let addr = listener.local_addr()?;
                    let work = context.work().spawn(
                        WorkSpec::aggregate("faulting-http"),
                        move |work| async move {
                            http::serve_http(
                                listener,
                                Router::new().route("/", get(|| async { "before-fault" })),
                                work,
                            )
                            .await?;
                            // Test-local fault after real HTTP, never a product bypass switch.
                            if panics {
                                panic!("injected aggregate failure");
                            }
                            Err::<(), BoxError>(
                                std::io::Error::other("injected aggregate error").into(),
                            )
                        },
                    )?;
                    send.send((addr, work)).unwrap();
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
        let (addr, work) = done(receive).await.unwrap();
        let mut stream = request(addr).await;
        let mut response = String::new();
        done(stream.read_to_string(&mut response)).await.unwrap();
        assert!(response.ends_with("before-fault"));
        drop(stream);
        app.stop();
        let report = done(work.wait()).await;
        assert_eq!(report.completion(), WorkCompletion::Unconfirmed);
        assert!(matches!(
            (panics, report.outcome()),
            (true, WorkOutcome::Panicked(_)) | (false, WorkOutcome::Failed(_))
        ));
        assert!(matches!(
            done(app.shutdown()).await.unwrap().as_ref(),
            miju_services::ShutdownReport::Incomplete(_)
        ));
        assert_eq!(cleaned.load(Ordering::SeqCst), 0);
        assert!(
            !app.observer()
                .snapshot()
                .services
                .iter()
                .find(|s| s.id.as_str() == "provider")
                .unwrap()
                .stopping
        );
        println!("HTTP_FAULT_UNCONFIRMED {fault}");
        // Only this isolated diagnostic host tears down an intentionally undrainable runtime.
    });
}

#[allow(dead_code, clippy::duplicate_mod)]
#[path = "../examples/http.rs"]
mod http_example;

async fn ready_response(addr: SocketAddr) -> String {
    let mut stream = done(TcpStream::connect(addr)).await.unwrap();
    stream
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    done(stream.read_to_string(&mut response)).await.unwrap();
    response
}

#[tokio::test]
async fn example_readiness_endpoint_tracks_actual_snapshot_transitions() {
    let (send, receive) = tokio::sync::oneshot::channel();
    let app = Application::builder()
        .service(
            ServiceSpec::new("http"),
            service_fn(move |context| async move {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let addr = listener.local_addr()?;
                let router = http_example::readiness_router(context.observer());
                let server = context
                    .work()
                    .spawn(WorkSpec::aggregate("http"), move |work| {
                        http::serve_http(listener, router, work)
                    })?;
                send.send((addr, context.clone())).unwrap();
                let report = server.wait().await;
                assert_eq!(report.completion(), WorkCompletion::Confirmed);
                Ok::<(), BoxError>(())
            }),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let (addr, context) = done(receive).await.unwrap();
    assert!(ready_response(addr).await.starts_with("HTTP/1.1 503"));
    context.set_health(Health::Healthy).unwrap();
    context.ready().unwrap();
    done(app.observer().ready(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(ready_response(addr).await.starts_with("HTTP/1.1 200"));
    context.set_health(Health::Unhealthy).unwrap();
    done(async {
        while app.observer().snapshot().health != Health::Unhealthy {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(ready_response(addr).await.starts_with("HTTP/1.1 503"));
    context.set_health(Health::Healthy).unwrap();
    done(app.observer().ready(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(ready_response(addr).await.starts_with("HTTP/1.1 200"));
    app.stop();
    assert!(!app.observer().snapshot().ready);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn disconnected_request_retains_router_state_destructor_through_stop() {
    let gate = Arc::new(DropGate::default());
    let release = Release(gate.clone());
    let entered = CancellationToken::new();
    let signal = entered.clone();
    let state = Arc::new(BlockingDrop(gate.clone()));
    let router = Router::new()
        .route(
            "/",
            get(
                move |axum::extract::State(state): axum::extract::State<Arc<BlockingDrop>>| {
                    let signal = signal.clone();
                    async move {
                        let _state = state;
                        signal.cancel();
                        std::future::pending::<()>().await;
                        "unused"
                    }
                },
            ),
        )
        .with_state(state);
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (app, addr) = start(router, AppOptions::default(), cleaned.clone()).await;
    let stream = request(addr).await;
    done(entered.cancelled()).await;
    drop(stream);
    app.stop();
    done(gate.entered.cancelled()).await;
    assert!(matches!(
        app.observer().drain(Duration::from_millis(20)).await,
        miju_services::DrainObservation::Pending(_)
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    assert!(
        !app.observer()
            .snapshot()
            .services
            .iter()
            .find(|s| s.id.as_str() == "provider")
            .unwrap()
            .stopping
    );
    drop(release);
    assert!(done(app.observer().wait()).await.unwrap().is_success());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}
