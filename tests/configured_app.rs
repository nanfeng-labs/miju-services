//! Exercise configuration-to-resource assembly and real cleanup ownership.

#[path = "../examples/configured_app/bootstrap.rs"]
mod bootstrap;
#[path = "../examples/configured_app/services.rs"]
mod services;
// Parsing/CLI are exercised separately by configuration.rs.
#[allow(dead_code)]
#[path = "../examples/configured_app/settings.rs"]
mod settings;

use miju_services::{
    App, BoxError, BuildError, RunError, observe::ShutdownProgress, report::ServiceFailure,
    work::WorkSpec,
};
use services::SampleFile;
use settings::Settings;
use std::{
    future::{Future, pending},
    io,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::AsyncWrite,
    sync::{Notify, oneshot},
};

static DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "miju-configured-{}-{}",
            std::process::id(),
            DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        Self(directory)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            std::fs::remove_dir_all(&self.0)
                .expect("release test-owned file resources before removing directory");
        }
    }
}

#[derive(Default)]
struct IoState {
    bytes: Vec<u8>,
    events: Vec<String>,
    blocked: bool,
    waker: Option<Waker>,
}

struct Writer {
    label: &'static str,
    state: Arc<Mutex<IoState>>,
    flush_error: bool,
    flush_panic: bool,
}

impl Writer {
    fn output(label: &'static str, state: &Arc<Mutex<IoState>>) -> Arc<SampleFile> {
        Arc::new(SampleFile::from_writer(Self {
            label,
            state: state.clone(),
            flush_error: false,
            flush_panic: false,
        }))
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.state.lock().unwrap().bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        assert!(!self.flush_panic, "writer-flush-panic");
        let mut state = self.state.lock().unwrap();
        if state.blocked {
            state.waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        state.events.push(format!("{}:flush", self.label));
        if self.flush_error {
            Poll::Ready(Err(io::Error::other("writer-flush-error")))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(context)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap()
            .events
            .push(format!("{}:drop", self.label));
    }
}

fn settings() -> Settings {
    Settings::new(5, PathBuf::from("unused-with-injected-writer")).unwrap()
}

#[tokio::test(start_paused = true)]
async fn configured_run_appends_samples_and_flushes_the_real_file() {
    let directory = TestDirectory::new();
    let path = directory.0.join("samples with spaces.log");
    std::fs::write(&path, "user-prefix\n").unwrap();
    let output = Arc::new(SampleFile::open(&path).await.unwrap());
    let handle = bootstrap::assemble(Settings::new(5, path.clone()).unwrap(), output)
        .unwrap()
        .start()
        .unwrap();
    handle.ready(Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_millis(5)).await;
    tokio::task::yield_now().await;
    let report = handle.shutdown().await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.snapshot.cleanup[0].label, "sample-file");
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.starts_with("user-prefix\n"),
        "existing application data was truncated"
    );
    let lines: Vec<_> = text.lines().skip(1).collect();
    assert!(
        !lines.is_empty(),
        "registered cleanup failed to flush buffered samples"
    );
    assert!(lines.iter().all(|line| line.starts_with("elapsed_ms=")
        && line.ends_with(&format!("process_id={}", std::process::id()))));
}

#[tokio::test]
async fn shared_resource_close_is_registered_by_the_composition_root() {
    let directory = TestDirectory::new();
    let path = directory.0.join("output.log");
    let output = Arc::new(SampleFile::open(&path).await.unwrap());
    let handle = bootstrap::assemble(settings(), output.clone())
        .unwrap()
        .start()
        .unwrap();
    handle.ready(Duration::from_secs(1)).await.unwrap();
    output.record("buffered-before-stop\n").await.unwrap();
    handle.shutdown().await.unwrap();
    assert!(
        output.record("late").await.is_err(),
        "composition root omitted resource cleanup"
    );
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "buffered-before-stop\n"
    );
}

#[tokio::test]
async fn bootstrap_rolls_back_build_refusal_and_keeps_rollback_error() {
    for fail in [false, true] {
        let state = Arc::new(Mutex::new(IoState::default()));
        let output = Arc::new(SampleFile::from_writer(Writer {
            label: "rollback",
            state: state.clone(),
            flush_error: fail,
            flush_panic: false,
        }));
        output.record("already-buffered").await.unwrap();
        let error = bootstrap::finish(
            Err(BuildError::Empty),
            output.clone(),
            pending::<io::Result<()>>(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.source.downcast_ref::<BuildError>(),
            Some(BuildError::Empty)
        ));
        assert_eq!(error.rollback.is_some(), fail);
        if fail {
            assert_eq!(error.rollback.unwrap().to_string(), "writer-flush-error");
        }
        assert!(
            output.record("late").await.is_err(),
            "bootstrap lost pre-start rollback responsibility"
        );
        assert_eq!(state.lock().unwrap().bytes, b"already-buffered");
        assert!(
            state
                .lock()
                .unwrap()
                .events
                .contains(&"rollback:drop".to_string())
        );
    }
}

#[test]
fn bootstrap_rolls_back_start_refusal_with_original_error() {
    let state = Arc::new(Mutex::new(IoState::default()));
    let output = Arc::new(SampleFile::from_writer(Writer {
        label: "rollback",
        state: state.clone(),
        flush_error: true,
        flush_panic: false,
    }));
    let app = bootstrap::assemble(settings(), output.clone());
    // The writer and uncontended mutex complete immediately without a runtime;
    // ordinary bootstrap never promises to drive arbitrary async rollback this way.
    let mut running = Box::pin(bootstrap::finish(app, output, pending::<io::Result<()>>()));
    let Poll::Ready(Err(error)) = running
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("immediate fixture rollback did not complete")
    };
    assert!(matches!(
        error.source.downcast_ref::<RunError>(),
        Some(RunError::Start(_))
    ));
    assert_eq!(error.rollback.unwrap().to_string(), "writer-flush-error");
    assert!(
        state
            .lock()
            .unwrap()
            .events
            .contains(&"rollback:drop".to_string())
    );
}

#[test]
fn recover_unstarted_assembly_without_premature_resource_close() {
    let directory = TestDirectory::new();
    let path = directory.0.join("recover.log");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let output = runtime.block_on(async { Arc::new(SampleFile::open(&path).await.unwrap()) });
    drop(runtime);
    let error = bootstrap::assemble(settings(), output.clone())
        .unwrap()
        .start()
        .unwrap_err();
    let app = error.into_app();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        output.record("retained-for-retry\n").await.unwrap();
        let handle = app.start().unwrap();
        handle.ready(Duration::from_secs(1)).await.unwrap();
        assert!(handle.shutdown().await.unwrap().is_success());
        assert!(output.record("late").await.is_err());
    });
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "retained-for-retry\n"
    );
}

#[tokio::test]
async fn partial_initialization_failure_uses_the_same_global_lifo_stack() {
    let state = Arc::new(Mutex::new(IoState::default()));
    let init_state = state.clone();
    let outer = Writer::output("assembly", &state);
    let cleanup = outer.clone();
    let app = App::builder()
        .cleanup("assembly", move || async move {
            cleanup.close().await?;
            Ok(())
        })
        .service_fn("initialization", move |context| async move {
            let acquired = Writer::output("initialization", &init_state);
            acquired.record("initialization-buffer\n").await?;
            context
                .defer_cleanup(
                    "initialized-file",
                    move || async move { acquired.close().await },
                )
                .unwrap();
            Err(io::Error::other("initialization-original-error").into())
        })
        .build()
        .unwrap();
    let error = bootstrap::finish(Ok(app), outer, pending::<io::Result<()>>())
        .await
        .unwrap_err();
    assert!(
        error.rollback.is_none(),
        "bootstrap closed accepted driver resources twice"
    );
    let error = error.source.downcast::<RunError>().unwrap();
    let report = error.report().unwrap();
    assert!(
        matches!(&report.snapshot.services[0].failures[0], ServiceFailure::Error(error) if error.to_string()=="initialization-original-error")
    );
    assert_eq!(
        report
            .snapshot
            .cleanup
            .iter()
            .map(|c| c.label.as_str())
            .collect::<Vec<_>>(),
        ["initialized-file", "assembly"]
    );
    assert_eq!(
        state.lock().unwrap().events,
        [
            "initialization:flush",
            "initialization:drop",
            "assembly:flush",
            "assembly:drop"
        ]
    );
}

#[tokio::test]
async fn resource_flush_error_and_panic_remain_in_the_final_report() {
    for panic in [false, true] {
        let state = Arc::new(Mutex::new(IoState::default()));
        let output = Arc::new(SampleFile::from_writer(Writer {
            label: "fault",
            state: state.clone(),
            flush_error: !panic,
            flush_panic: panic,
        }));
        let handle = bootstrap::assemble(settings(), output)
            .unwrap()
            .start()
            .unwrap();
        handle.ready(Duration::from_secs(1)).await.unwrap();
        let error = handle.shutdown().await.unwrap_err();
        let cleanup = &error.report().unwrap().snapshot.cleanup[0];
        assert!(!cleanup.is_success());
        if panic {
            assert!(
                cleanup
                    .panic
                    .as_ref()
                    .unwrap()
                    .contains("writer-flush-panic")
            );
        } else {
            assert_eq!(
                cleanup.error.as_ref().unwrap().to_string(),
                "writer-flush-error"
            );
        }
        assert!(
            state
                .lock()
                .unwrap()
                .events
                .contains(&"fault:drop".to_string())
        );
    }
}

#[tokio::test]
async fn pending_flush_stays_owned_until_actual_completion() {
    let state = Arc::new(Mutex::new(IoState {
        blocked: true,
        ..IoState::default()
    }));
    let output = Writer::output("slow", &state);
    let handle = bootstrap::assemble(settings(), output.clone())
        .unwrap()
        .start()
        .unwrap();
    handle.ready(Duration::from_secs(1)).await.unwrap();
    let result = handle
        .shutdown_within(Duration::from_millis(10))
        .await
        .unwrap();
    let ShutdownProgress::Pending(snapshot) = result else {
        panic!("unfinished flush was reported complete")
    };
    assert_eq!(snapshot.current_cleanup.as_deref(), Some("sample-file"));
    assert!(output.record("late").await.is_err());
    assert!(state.lock().unwrap().events.is_empty());
    let waker = {
        let mut state = state.lock().unwrap();
        state.blocked = false;
        state.waker.take()
    };
    waker.unwrap().wake();
    assert!(handle.shutdown().await.unwrap().is_success());
    assert_eq!(state.lock().unwrap().events, ["slow:flush", "slow:drop"]);
}

#[tokio::test]
async fn accepted_child_releases_before_file_flush_and_close() {
    let state = Arc::new(Mutex::new(IoState::default()));
    let output = Writer::output("owned", &state);
    let close = output.clone();
    let child = output.clone();
    let release = Arc::new(Notify::new());
    let child_release = release.clone();
    let (started, running) = oneshot::channel();
    let app = App::builder()
        .cleanup("file", move || async move {
            close.close().await?;
            Ok(())
        })
        .service_fn("writer", move |context| async move {
            context
                .work()
                .spawn(WorkSpec::new("last-sample"), move |work| async move {
                    started.send(()).unwrap();
                    work.stopping().await;
                    child_release.notified().await;
                    child.record("last-child-write\n").await?;
                    Ok::<(), BoxError>(())
                })?;
            context.ready();
            context.stopping().await;
            Ok(())
        })
        .build()
        .unwrap()
        .start()
        .unwrap();
    running.await.unwrap();
    assert!(matches!(
        app.shutdown_within(Duration::from_millis(10))
            .await
            .unwrap(),
        ShutdownProgress::Pending(_)
    ));
    assert!(state.lock().unwrap().events.is_empty());
    release.notify_one();
    assert!(app.shutdown().await.unwrap().is_success());
    assert_eq!(state.lock().unwrap().bytes, b"last-child-write\n");
    assert_eq!(state.lock().unwrap().events, ["owned:flush", "owned:drop"]);
    assert!(output.record("late").await.is_err());
}

#[tokio::test]
async fn invalid_output_directory_keeps_the_original_open_error() {
    let directory = TestDirectory::new();
    let path = directory.0.join("absent/output.log");
    let error = bootstrap::run(
        Settings::new(5, path.clone()).unwrap(),
        pending::<io::Result<()>>(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.source.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::NotFound
    );
    assert!(error.rollback.is_none());
    assert!(!path.exists());
}
