//! Lifecycle events emitted with the `tracing` feature, captured by a
//! test-local subscriber. The library installs none.
#![cfg(feature = "tracing")]

use miju_services::{App, BoxError};
use std::{
    fmt::{self, Write as _},
    future::pending,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone, Debug)]
struct Captured {
    level: Level,
    message: String,
    fields: String,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

impl Capture {
    fn events(&self) -> Vec<Captured> {
        self.0.lock().unwrap().clone()
    }

    fn count(&self, message: &str) -> usize {
        self.events()
            .iter()
            .filter(|event| event.message == message)
            .count()
    }

    fn find(&self, message: &str) -> Option<Captured> {
        self.events()
            .into_iter()
            .find(|event| event.message.starts_with(message))
    }
}

struct Fields {
    message: String,
    fields: String,
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, "{}={value:?} ", field.name());
        }
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields {
            message: String::new(),
            fields: String::new(),
        };
        event.record(&mut fields);
        self.0.lock().unwrap().push(Captured {
            level: *event.metadata().level(),
            message: fields.message,
            fields: fields.fields,
        });
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// Run `body` on a current-thread runtime with `capture` as this thread's subscriber.
fn captured<F: std::future::Future>(capture: &Capture, body: F) -> F::Output {
    let _default = tracing::subscriber::set_default(capture.clone());
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(body)
}

#[test]
fn a_failure_stops_the_application_and_every_step_is_an_event() {
    let capture = Capture::default();
    captured(&capture, async {
        let mut app = App::new();
        app.service_fn("broken", |_| async {
            Err::<(), BoxError>(io::Error::other("broken on purpose").into())
        });
        let error = app.run_until(pending::<io::Result<()>>()).await;
        assert!(error.is_err());
    });
    let failure = capture.find("service failure recorded").unwrap();
    assert_eq!(failure.level, Level::WARN);
    assert!(failure.fields.contains("service=broken"), "{failure:?}");
    assert!(failure.fields.contains("broken on purpose"), "{failure:?}");
    assert_eq!(
        capture.find("application stopping").unwrap().level,
        Level::INFO
    );
    let terminated = capture.find("application terminated").unwrap();
    assert!(
        terminated.fields.contains("success=false"),
        "{terminated:?}"
    );
    // One event each, however many passes the driver made.
    assert_eq!(capture.count("service failure recorded"), 1);
    assert_eq!(capture.count("application stopping"), 1);
}

#[test]
fn grace_expiry_and_a_failed_cleanup_are_warnings() {
    let capture = Capture::default();
    captured(&capture, async {
        tokio::time::pause();
        let mut app = App::new();
        app.shutdown_grace(Duration::from_secs(1));
        app.service_fn("stubborn", |ctx| async move {
            ctx.defer("flush", async {
                Err::<(), io::Error>(io::Error::other("flush failed"))
            })?;
            ctx.ready();
            // Ignores stop until it is aborted.
            pending::<()>().await;
            Ok(())
        });
        let handle = app.start().unwrap();
        handle.ready().await.unwrap();
        let report = handle.shutdown().await.unwrap();
        assert!(!report.is_success());
    });
    let grace = capture.find("stop grace expired").unwrap();
    assert_eq!(grace.level, Level::WARN);
    assert!(grace.fields.contains("service=stubborn"), "{grace:?}");
    assert!(grace.fields.contains("outstanding=1"), "{grace:?}");
    let cleanup = capture.find("cleanup action failed").unwrap();
    assert_eq!(cleanup.level, Level::WARN);
    assert!(cleanup.fields.contains("cleanup=flush"), "{cleanup:?}");
    assert!(cleanup.fields.contains("flush failed"), "{cleanup:?}");
}

#[test]
fn losing_the_driver_is_an_error_event() {
    let capture = Capture::default();
    let _default = tracing::subscriber::set_default(capture.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let handle = runtime.block_on(async {
        let mut app = App::new();
        app.service_fn("resident", |ctx| async move {
            ctx.ready();
            ctx.stopping().await;
            Ok(())
        });
        let handle = app.start().unwrap();
        handle.ready().await.unwrap();
        handle
    });
    // Dropping the runtime drops the driver task before any report exists.
    drop(runtime);
    assert_eq!(handle.status().phase, miju_services::Phase::Lost);
    let lost = capture.find("lifecycle driver lost").unwrap();
    assert_eq!(lost.level, Level::ERROR);
}

#[test]
fn a_clean_run_emits_no_warnings() {
    let capture = Capture::default();
    captured(&capture, async {
        let mut app = App::new();
        app.service_fn("resident", |ctx| async move {
            ctx.defer("flush", async { Ok::<(), io::Error>(()) })?;
            ctx.ready();
            ctx.stopping().await;
            Ok(())
        });
        let handle = app.start().unwrap();
        handle.ready().await.unwrap();
        assert!(handle.shutdown().await.unwrap().is_success());
    });
    let events = capture.events();
    assert!(
        events.iter().all(|event| event.level > Level::WARN),
        "{events:?}"
    );
    assert_eq!(capture.count("application stopping"), 1);
    let terminated = capture.find("application terminated").unwrap();
    assert!(terminated.fields.contains("success=true"), "{terminated:?}");
}
