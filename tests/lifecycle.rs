//! Assembly, readiness, dependency-ordered stop, cleanup and the final report.

use miju_services::{
    App, AssemblyError, BoxError, Context, NotReady, Resource, ResourceError, Result, RunError,
    Service, Spec, StartError, critical,
    report::{Failure, Health, Outcome, Phase, ServicePhase},
    scope::Kind,
};
use std::{
    future::Future,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc,
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, oneshot},
    time::timeout,
};

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("the lifecycle stalled")
}

/// A resident service with nothing to do.
async fn idle(ctx: Context) -> Result {
    ctx.ready();
    ctx.stopping().await;
    Ok(())
}

type Log = Arc<Mutex<Vec<&'static str>>>;

fn note(log: &Log, event: &'static str) {
    log.lock().unwrap().push(event);
}

struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

#[tokio::test]
async fn readiness_follows_real_initialization_and_declared_health() {
    let (send, receive) = oneshot::channel();
    let initialize = Arc::new(Notify::new());
    let mut app = App::new();
    {
        let initialize = initialize.clone();
        app.service_fn("device", move |ctx| async move {
            let _ = send.send(ctx.clone());
            initialize.notified().await;
            ctx.ready();
            ctx.stopping().await;
            Ok(())
        });
    }
    let app = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    assert!(!app.status().ready);
    assert_eq!(app.status().phase, Phase::Starting);
    assert_eq!(
        app.snapshot().service("device").unwrap().phase,
        ServicePhase::Starting
    );

    initialize.notify_one();
    bounded(app.ready()).await.unwrap();
    assert_eq!(
        (app.status().phase, app.status().health),
        (Phase::Running, Health::Healthy)
    );

    ctx.set_health(Health::Degraded);
    while app.status().ready {
        bounded(app.changed()).await;
    }
    assert_eq!(app.status().health, Health::Degraded);
    assert_eq!(
        app.status().phase,
        Phase::Running,
        "health does not stop anything"
    );

    ctx.set_health(Health::Healthy);
    bounded(app.ready()).await.unwrap();
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
    // Late notifications are ignored.
    ctx.ready();
    ctx.set_health(Health::Unhealthy);
    assert_eq!(app.status().phase, Phase::Terminated);
}

#[tokio::test]
async fn an_explicit_health_declaration_survives_ready() {
    let mut app = App::new();
    app.service_fn("device", |ctx| async move {
        ctx.set_health(Health::Unknown);
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    while app.status().phase != Phase::Running {
        bounded(app.changed()).await;
    }
    assert!(
        !app.status().ready,
        "initialized, but its declared health is not Healthy"
    );
    assert_eq!(app.status().health, Health::Unknown);
    assert!(bounded(app.shutdown()).await.unwrap().is_success());
}

#[tokio::test]
async fn stop_withdraws_readiness_at_once_and_is_only_a_request() {
    let mut app = App::new();
    app.service("idle", idle);
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    app.stop();
    // No await in between: the driver has not run yet.
    assert!(!app.status().ready);
    assert_eq!(app.status().phase, Phase::Stopping);
    assert!(matches!(app.ready().await, Err(NotReady::Stopped(_))));
    app.stop();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    assert_eq!(app.status().phase, Phase::Terminated);
    // A later wait resolves the same way.
    assert!(Arc::ptr_eq(&report, &bounded(app.wait()).await.unwrap()));
}

#[tokio::test]
async fn a_required_failure_or_natural_completion_ends_the_application_without_a_signal() {
    let mut failing = App::new();
    failing
        .service("idle", idle)
        .service_fn("device", |_ctx| async {
            Err(io::Error::new(io::ErrorKind::ConnectionRefused, "device unavailable").into())
        });
    let Err(RunError::Failed { report }) = bounded(failing.run()).await else {
        panic!("a required failure must end the run with the report");
    };
    let failures = report.failures();
    assert!(matches!(&failures[..], [(service, Failure::Error(error))]
        if &**service == "device" && error.to_string() == "device unavailable"));
    assert!(matches!(
        report.service("idle").unwrap().outcome,
        Some(Outcome::Succeeded)
    ));

    let mut finite = App::new();
    finite
        .service_fn(Spec::new("first").finite(), |_ctx| async { Ok(()) })
        .service_fn(Spec::new("second").finite().after("first"), |_ctx| async {
            Ok(())
        });
    let report = bounded(finite.run()).await.unwrap();
    assert!(report.is_success());
    assert!(
        report
            .services
            .iter()
            .all(|service| service.initialized && service.phase == ServicePhase::Done)
    );
}

#[tokio::test(start_paused = true)]
async fn a_finite_step_may_outlast_the_startup_timeout() {
    let started = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    app.startup_timeout(Duration::from_secs(1));
    app.service_fn(Spec::new("migrate").finite(), |_ctx| async {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    });
    {
        let started = started.clone();
        app.service_fn(Spec::new("api").after("migrate"), move |ctx| async move {
            started.store(true, SeqCst);
            idle(ctx).await
        });
    }
    let app = app.start().unwrap();
    bounded_virtual(app.ready()).await.unwrap();
    assert!(started.load(SeqCst));
    assert!(app.shutdown().await.unwrap().is_success());
}

/// Like `bounded`, for tests on a paused clock where minutes pass instantly.
async fn bounded_virtual<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(3600), future)
        .await
        .expect("the lifecycle stalled")
}

#[tokio::test(start_paused = true)]
async fn a_resident_that_never_reports_ready_times_out_and_its_dependents_never_start() {
    let invoked = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.startup_timeout(Duration::from_secs(2));
    app.service_fn("slow", |ctx| async move {
        ctx.stopping().await;
        Ok(())
    });
    {
        let (invoked, capture) = (invoked.clone(), Tracked(drops.clone()));
        app.service_fn(
            Spec::new("dependent").after("slow"),
            move |ctx| async move {
                let _capture = capture;
                invoked.store(true, SeqCst);
                idle(ctx).await
            },
        );
    }
    let app = app.start().unwrap();
    assert!(matches!(
        bounded_virtual(app.ready()).await,
        Err(NotReady::Failed(_))
    ));
    let report = app.wait().await.unwrap();
    assert!(!report.is_success());
    assert!(matches!(
        report.service("slow").unwrap().failures[..],
        [Failure::StartupTimeout]
    ));
    let dependent = report.service("dependent").unwrap();
    assert!(matches!(dependent.outcome, Some(Outcome::NotStarted)));
    assert!(!invoked.load(SeqCst));
    assert_eq!(
        drops.load(SeqCst),
        1,
        "a never-started entry must release its captures"
    );
}

#[tokio::test]
async fn a_failed_stop_source_still_yields_the_report() {
    let mut app = App::new();
    app.service("idle", idle);
    let result =
        bounded(app.run_until(async { Err(io::Error::other("signal handler failed")) })).await;
    let Err(error @ RunError::StopSource { .. }) = result else {
        panic!("the stop source's error must be kept");
    };
    assert!(error.report().unwrap().is_success());
    assert_eq!(
        error.to_string(),
        "the stop source failed: signal handler failed"
    );
}

#[tokio::test]
async fn early_exit_and_panic_fail_a_resident_while_an_optional_failure_stays_local() {
    let mut early = App::new();
    early.service_fn("returns", |ctx| async move {
        ctx.ready();
        Ok(())
    });
    let Err(RunError::Failed { report }) = bounded(early.run()).await else {
        panic!("a resident that returns early has failed");
    };
    assert!(matches!(
        report.service("returns").unwrap().failures[..],
        [Failure::ExitedEarly]
    ));

    let mut panics = App::new();
    panics.service_fn("panics", |_ctx| async {
        if std::hint::black_box(true) {
            panic!("root panic");
        }
        Ok(())
    });
    let Err(RunError::Failed { report }) = bounded(panics.run()).await else {
        panic!("a panicking root has failed");
    };
    assert!(
        matches!(&report.service("panics").unwrap().failures[..], [Failure::Panic(message)] if &**message == "root panic")
    );

    let mut mixed = App::new();
    mixed
        .service("core", idle)
        .service_fn(Spec::new("metrics").optional(), |_ctx| async {
            Err("exporter unreachable".into())
        });
    let app = mixed.start().unwrap();
    bounded(app.ready()).await.unwrap();
    while app.status().health != Health::Degraded {
        bounded(app.changed()).await;
    }
    assert!(
        app.status().ready,
        "an optional failure does not withdraw readiness"
    );
    assert_eq!(
        app.snapshot().service("metrics").unwrap().phase,
        ServicePhase::Done
    );
    let report = bounded(app.shutdown()).await.unwrap();
    assert!(
        report.is_success(),
        "an optional failure does not fail the application"
    );
    assert_eq!(report.failures().len(), 1);
}

#[tokio::test]
async fn work_errors_block_a_finite_provider_only_when_the_work_is_critical() {
    let consumer_ran = Arc::new(AtomicBool::new(false));
    let mut tolerant = App::new();
    tolerant.service_fn(Spec::new("prepare").finite(), |ctx| async move {
        ctx.spawn("best-effort", async {
            Err::<(), _>(io::Error::other("cache miss"))
        })?;
        Ok(())
    });
    {
        let consumer_ran = consumer_ran.clone();
        tolerant.service_fn(
            Spec::new("consumer").finite().after("prepare"),
            move |_ctx| async move {
                consumer_ran.store(true, SeqCst);
                Ok(())
            },
        );
    }
    let report = bounded(tolerant.run()).await.unwrap();
    assert!(consumer_ran.load(SeqCst));
    assert_eq!(report.service("prepare").unwrap().work.failed, 1);

    let consumer_ran = Arc::new(AtomicBool::new(false));
    let mut strict = App::new();
    strict.service_fn(Spec::new("prepare").finite(), |ctx| async move {
        ctx.spawn(critical("schema"), async {
            Err::<(), _>(io::Error::other("migration failed"))
        })?;
        Ok(())
    });
    {
        let consumer_ran = consumer_ran.clone();
        strict.service_fn(
            Spec::new("consumer").after("prepare"),
            move |ctx| async move {
                consumer_ran.store(true, SeqCst);
                idle(ctx).await
            },
        );
    }
    let Err(RunError::Failed { report }) = bounded(strict.run()).await else {
        panic!("critical work failed; the run cannot succeed");
    };
    assert!(
        !consumer_ran.load(SeqCst),
        "a provider with a critical fault unlocked its consumer"
    );
    let prepare = report.service("prepare").unwrap();
    assert!(!prepare.initialized);
    assert!(
        matches!(prepare.outcome, Some(Outcome::Succeeded)),
        "the root itself returned Ok"
    );
    assert!(matches!(prepare.failures[..], [Failure::Work(_)]));
}

#[tokio::test]
async fn a_provider_stops_after_its_consumer_and_the_consumers_work_then_cleanup_runs() {
    let log = Log::default();
    let release_child = Arc::new(Notify::new());
    let mut app = App::new();
    {
        let log = log.clone();
        app.service_fn("provider", move |ctx| async move {
            let closing = log.clone();
            ctx.defer("provider-resource", async move {
                note(&closing, "cleanup provider");
                Ok::<_, BoxError>(())
            })?;
            ctx.ready();
            ctx.stopping().await;
            note(&log, "provider asked to stop");
            Ok(())
        });
    }
    {
        let (log, release_child) = (log.clone(), release_child.clone());
        app.service_fn(
            Spec::new("consumer").after("provider"),
            move |ctx| async move {
                let (closing, child_log) = (log.clone(), log.clone());
                ctx.defer("consumer-resource", async move {
                    note(&closing, "cleanup consumer");
                    Ok::<_, BoxError>(())
                })?;
                ctx.spawn("child", async move {
                    release_child.notified().await;
                    note(&child_log, "consumer child released");
                    Ok::<_, BoxError>(())
                })?;
                ctx.ready();
                ctx.stopping().await;
                note(&log, "consumer root returned");
                Ok(())
            },
        );
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    app.stop();
    while app.snapshot().service("consumer").unwrap().phase != ServicePhase::Draining {
        tokio::task::yield_now().await;
    }
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    let snapshot = app.snapshot();
    assert_eq!(
        snapshot.service("provider").unwrap().phase,
        ServicePhase::Running,
        "the provider was stopped under its consumer"
    );
    assert_eq!(snapshot.pending_cleanup.len(), 2);
    assert_eq!(*log.lock().unwrap(), ["consumer root returned"]);

    release_child.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    assert_eq!(
        *log.lock().unwrap(),
        [
            "consumer root returned",
            "consumer child released",
            "provider asked to stop",
            "cleanup consumer",
            "cleanup provider",
        ]
    );
    let order: Vec<_> = report
        .cleanup
        .iter()
        .map(|action| action.name.name().to_owned())
        .collect();
    assert_eq!(order, ["consumer-resource", "provider-resource"]);
}

#[tokio::test]
async fn cleanup_runs_consumers_before_providers_even_when_registered_late() {
    let consumer_ready = Arc::new(Notify::new());
    let mut app = App::new();
    {
        let consumer_ready = consumer_ready.clone();
        app.service_fn("provider", move |ctx| async move {
            ctx.defer("provider-early", async { Ok::<_, BoxError>(()) })?;
            ctx.ready();
            // Registered after the consumer registered its own.
            consumer_ready.notified().await;
            ctx.defer("provider-late", async { Ok::<_, BoxError>(()) })?;
            ctx.stopping().await;
            Ok(())
        });
    }
    app.service_fn(
        Spec::new("consumer").after("provider"),
        move |ctx| async move {
            ctx.defer("consumer", async { Ok::<_, BoxError>(()) })?;
            consumer_ready.notify_one();
            idle(ctx).await
        },
    );
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    while app.snapshot().pending_cleanup.len() != 3 {
        tokio::task::yield_now().await;
    }
    let report = bounded(app.shutdown()).await.unwrap();
    let order: Vec<_> = report
        .cleanup
        .iter()
        .map(|action| action.name.name().to_owned())
        .collect();
    assert_eq!(order, ["consumer", "provider-late", "provider-early"]);
}

#[tokio::test(start_paused = true)]
async fn a_hanging_cleanup_keeps_the_wait_pending_and_is_named() {
    let finish = Arc::new(Notify::new());
    let mut app = App::new();
    {
        let finish = finish.clone();
        app.service_fn("store", move |ctx| async move {
            ctx.defer("first", async { Ok::<_, BoxError>(()) })?;
            ctx.defer("flush", async move {
                finish.notified().await;
                Ok::<_, BoxError>(())
            })?;
            idle(ctx).await
        });
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    app.stop();
    assert!(
        timeout(Duration::from_secs(600), app.wait()).await.is_err(),
        "a hanging cleanup is not completion"
    );
    assert_eq!(app.status().phase, Phase::Cleaning);
    let snapshot = app.snapshot();
    assert_eq!(snapshot.current_cleanup.as_ref().unwrap().1.name(), "flush");
    assert_eq!(snapshot.pending_cleanup.len(), 1);
    let outstanding = snapshot.outstanding();
    assert!(outstanding.iter().all(|item| item.kind == Kind::Cleanup));
    assert_eq!(snapshot.outstanding_total(), 2);

    finish.notify_one();
    let report = app.wait().await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.cleanup.len(), 2);
}

#[tokio::test]
async fn a_failing_or_panicking_cleanup_does_not_skip_the_rest() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    {
        let ran = ran.clone();
        app.service_fn("store", move |ctx| async move {
            let (first, second, third) = (ran.clone(), ran.clone(), ran.clone());
            ctx.defer("ok", async move {
                first.fetch_add(1, SeqCst);
                Ok::<_, BoxError>(())
            })?;
            ctx.defer("fails", async move {
                second.fetch_add(1, SeqCst);
                Err::<(), _>(io::Error::other("disk full"))
            })?;
            ctx.defer("panics", async move {
                third.fetch_add(1, SeqCst);
                if std::hint::black_box(true) {
                    panic!("cleanup panic");
                }
                Ok::<_, BoxError>(())
            })?;
            idle(ctx).await
        });
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    let report = bounded(app.shutdown()).await.unwrap();
    assert_eq!(ran.load(SeqCst), 3);
    assert!(!report.is_success());
    let [panics, fails, ok] = &report.cleanup[..] else {
        panic!("three actions, last registered first");
    };
    assert_eq!(panics.panic.as_deref(), Some("cleanup panic"));
    assert_eq!(fails.error.as_ref().unwrap().to_string(), "disk full");
    assert!(ok.is_success());
    assert!(
        report.service("store").unwrap().failures.is_empty(),
        "a cleanup failure is not a service failure"
    );
}

#[tokio::test]
async fn defer_is_accepted_after_ready_and_from_work_and_handed_back_after_drain() {
    let (send, receive) = oneshot::channel();
    let mut app = App::new();
    app.service_fn("store", move |ctx| async move {
        let _ = send.send(ctx.clone());
        ctx.ready();
        ctx.defer("after-ready", async { Ok::<_, BoxError>(()) })?;
        let worker = ctx.clone();
        ctx.spawn("acquires", async move {
            worker.defer("from-work", async { Ok::<_, BoxError>(()) })?;
            Ok::<_, BoxError>(())
        })?
        .await?;
        ctx.stopping().await;
        Ok(())
    });
    let app = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    bounded(app.ready()).await.unwrap();
    let report = bounded(app.shutdown()).await.unwrap();
    assert_eq!(report.cleanup.len(), 2);

    // Reachable only from code outside any lease: the service has drained.
    let ran = Arc::new(AtomicBool::new(false));
    let late = {
        let ran = ran.clone();
        async move {
            ran.store(true, SeqCst);
            Ok::<_, BoxError>(())
        }
    };
    let rejected = ctx
        .defer("too-late", late)
        .expect_err("registration is closed");
    assert_eq!(
        rejected.to_string(),
        "cleanup `too-late` was registered after its service had drained"
    );
    rejected
        .into_inner()
        .expect("the action is handed back")
        .await
        .unwrap();
    assert!(ran.load(SeqCst));
}

#[tokio::test]
async fn a_root_that_returns_closes_admission_and_its_work_and_cleanup_still_finish() {
    let (send, receive) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    {
        let (release, cleaned) = (release.clone(), cleaned.clone());
        app.service_fn(Spec::new("batch").finite(), move |ctx| async move {
            ctx.defer("batch-output", async move {
                cleaned.store(true, SeqCst);
                Ok::<_, BoxError>(())
            })?;
            ctx.spawn("tail", async move {
                release.notified().await;
                Ok::<_, BoxError>(())
            })?;
            let _ = send.send(ctx.clone());
            Ok(())
        });
    }
    let app = app.start().unwrap();
    let ctx = bounded(receive).await.unwrap();
    while app.snapshot().service("batch").unwrap().phase != ServicePhase::Draining {
        tokio::task::yield_now().await;
    }
    assert!(
        ctx.spawn("late", async { Ok::<_, BoxError>(()) }).is_err(),
        "the root's return closes admission"
    );
    assert!(
        !app.snapshot().service("batch").unwrap().initialized,
        "finite: not initialized until drained"
    );
    assert!(!cleaned.load(SeqCst));

    release.notify_one();
    let report = bounded(app.wait()).await.unwrap();
    assert!(report.is_success());
    assert!(cleaned.load(SeqCst));
    assert!(report.service("batch").unwrap().initialized);
}

#[tokio::test]
async fn dropping_the_last_handle_or_the_run_future_requests_stop_and_draining_continues() {
    let stopped = Arc::new(AtomicUsize::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let assemble = || {
        let (stopped, cleaned) = (stopped.clone(), cleaned.clone());
        let mut app = App::new();
        app.service_fn("service", move |ctx| async move {
            ctx.defer("resource", async move {
                cleaned.fetch_add(1, SeqCst);
                Ok::<_, BoxError>(())
            })?;
            ctx.ready();
            ctx.stopping().await;
            stopped.fetch_add(1, SeqCst);
            Ok(())
        });
        app
    };

    let app = assemble().start().unwrap();
    bounded(app.ready()).await.unwrap();
    let observer = app.observer();
    let clone = app.clone();
    drop(app);
    assert_eq!(
        observer.status().phase,
        Phase::Running,
        "another control handle is still alive"
    );
    drop(clone);
    assert!(bounded(observer.wait()).await.unwrap().is_success());
    assert_eq!((stopped.load(SeqCst), cleaned.load(SeqCst)), (1, 1));

    // A cancelled `run_until` drops its handle; the application still drains.
    let running = assemble().run_until(std::future::pending::<Result<(), io::Error>>());
    assert!(timeout(Duration::from_millis(50), running).await.is_err());
    bounded(async {
        while cleaned.load(SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(stopped.load(SeqCst), 2);
}

#[test]
fn a_refused_start_returns_the_application() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StartError>();

    let invoked = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    {
        let invoked = invoked.clone();
        app.service_fn("service", move |ctx| async move {
            invoked.store(true, SeqCst);
            idle(ctx).await
        });
    }
    let error = app.start().expect_err("there is no runtime");
    assert!(matches!(error, StartError::NoRuntime { .. }));
    let app = error.into_app();

    let untimed = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let error = untimed
        .block_on(async { app.start() })
        .expect_err("there is no time driver");
    assert!(matches!(error, StartError::NoTimeDriver { .. }), "{error}");
    assert!(
        untimed
            .block_on(async { tokio::spawn(async { 1 }).await })
            .is_ok(),
        "the probe harmed the runtime"
    );
    let app = error.into_app();
    assert!(!invoked.load(SeqCst));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let report = runtime.block_on(async {
        let app = app.start().unwrap();
        app.ready().await.unwrap();
        app.shutdown().await.unwrap()
    });
    assert!(report.is_success());
    assert!(invoked.load(SeqCst));
}

struct Sampler {
    period: Duration,
}

impl Service for Sampler {
    type Error = io::Error;

    async fn run(self, ctx: Context) -> std::result::Result<(), io::Error> {
        ctx.ready();
        ctx.stopping().await;
        Ok(())
    }

    fn check(&self) -> Result {
        if self.period.is_zero() {
            return Err("period must be positive".into());
        }
        Ok(())
    }
}

#[tokio::test]
async fn an_invalid_assembly_is_rejected_before_anything_runs() {
    let invoked = Arc::new(AtomicBool::new(false));
    let entry = || {
        let invoked = invoked.clone();
        move |ctx: Context| async move {
            invoked.store(true, SeqCst);
            idle(ctx).await
        }
    };
    let assembled = |build: &dyn Fn(&mut App)| {
        let mut app = App::new();
        build(&mut app);
        app.validate()
    };

    assert_eq!(App::new().validate(), Err(AssemblyError::Empty));
    assert_eq!(
        assembled(&|app| {
            app.service("  ", entry());
        }),
        Err(AssemblyError::EmptyName)
    );
    assert_eq!(
        assembled(&|app| {
            app.service("a", entry()).service("a", entry());
        }),
        Err(AssemblyError::Duplicate("a".into()))
    );
    assert_eq!(
        assembled(&|app| {
            app.service(Spec::new("a").after("missing"), entry());
        }),
        Err(AssemblyError::UnknownDependency {
            service: "a".into(),
            provider: "missing".into()
        })
    );
    assert_eq!(
        assembled(&|app| {
            app.service("p", entry())
                .service(Spec::new("a").after("p").after("p"), entry());
        }),
        Err(AssemblyError::DuplicateDependency {
            service: "a".into(),
            provider: "p".into()
        })
    );
    assert_eq!(
        assembled(&|app| {
            app.service(Spec::new("p").optional(), entry())
                .service(Spec::new("a").after("p"), entry());
        }),
        Err(AssemblyError::OptionalProvider("p".into()))
    );
    assert_eq!(
        assembled(&|app| {
            app.service(Spec::new("a").after("b"), entry())
                .service(Spec::new("b").after("a"), entry());
        }),
        Err(AssemblyError::Cycle)
    );
    assert_eq!(
        assembled(&|app| {
            app.service(Spec::new("a").after("a"), entry());
        }),
        Err(AssemblyError::Cycle)
    );
    assert_eq!(
        assembled(&|app| {
            app.service("a", entry()).shutdown_grace(Duration::ZERO);
        }),
        Err(AssemblyError::InvalidBudget("shutdown_grace"))
    );
    assert_eq!(
        assembled(&|app| {
            app.service("a", entry()).startup_timeout(Duration::MAX);
        }),
        Err(AssemblyError::InvalidBudget("startup_timeout"))
    );
    assert_eq!(
        assembled(&|app| {
            app.service(
                "sampler",
                Sampler {
                    period: Duration::ZERO,
                },
            );
        }),
        Err(AssemblyError::Service {
            service: "sampler".into(),
            error: "period must be positive".into()
        })
    );

    let mut invalid = App::new();
    invalid.service(Spec::new("a").after("missing"), entry());
    let error = invalid.start().expect_err("the assembly is invalid");
    assert!(matches!(
        error,
        StartError::Invalid {
            error: AssemblyError::UnknownDependency { .. },
            ..
        }
    ));
    assert!(!invoked.load(SeqCst), "an entry ran in a rejected assembly");

    let mut valid = App::new();
    valid.service(
        "sampler",
        Sampler {
            period: Duration::from_secs(1),
        },
    );
    assert!(
        bounded(valid.start().unwrap().shutdown())
            .await
            .unwrap()
            .is_success()
    );
}

#[derive(Clone)]
struct Store(Arc<AtomicBool>);

async fn open_store(ctx: Context, closed: Arc<AtomicBool>) -> Result<Store> {
    let store = Store(closed);
    let closing = store.clone();
    ctx.defer("store", async move {
        closing.0.store(true, SeqCst);
        Ok::<_, BoxError>(())
    })?;
    Ok(store)
}

#[tokio::test]
async fn a_resource_reaches_declared_consumers_and_is_refused_to_others() {
    let closed = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let store: Resource<Store> = {
        let closed = closed.clone();
        app.resource("store", move |ctx| open_store(ctx, closed))
    };
    {
        let store = store.clone();
        app.service_fn(Spec::new("declared").after(&store), move |ctx| async move {
            let opened = store.get(&ctx)?;
            assert!(
                !opened.0.load(SeqCst),
                "the resource was closed under a consumer"
            );
            idle(ctx).await
        });
    }
    {
        // Registered after the store, so the store is very likely initialized
        // by the time this runs. The refusal must not depend on that.
        let store = store.clone();
        app.service_fn(
            Spec::new("undeclared").after("declared").optional(),
            move |ctx| async move {
                let error = store
                    .get(&ctx)
                    .err()
                    .expect("the dependency was not declared");
                assert_eq!(
                    error,
                    ResourceError::Undeclared {
                        consumer: "undeclared".into(),
                        provider: "store".into()
                    }
                );
                Err(error.into())
            },
        );
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    assert!(!closed.load(SeqCst));
    let report = bounded(app.shutdown()).await.unwrap();
    assert!(closed.load(SeqCst));
    assert!(report.is_success());
    let failures = &report.service("undeclared").unwrap().failures;
    assert!(matches!(&failures[..], [Failure::Error(error)]
        if error.to_string() == "service `undeclared` uses resource `store` without declaring `.after(..)` on it"));
}

#[tokio::test]
async fn a_partial_startup_failure_runs_exactly_the_cleanup_that_was_registered() {
    let closed = Arc::new(AtomicBool::new(false));
    let never_started = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let store = {
        let closed = closed.clone();
        app.resource("store", move |ctx| open_store(ctx, closed))
    };
    app.service_fn(Spec::new("migrate").finite().after(&store), |_ctx| async {
        Err(io::Error::other("schema is newer than this build").into())
    });
    {
        let capture = Tracked(never_started.clone());
        app.service_fn(Spec::new("api").after("migrate"), move |ctx| async move {
            let _capture = capture;
            idle(ctx).await
        });
    }
    let Err(RunError::Failed { report }) = bounded(app.run()).await else {
        panic!("the startup failure must end the run");
    };
    assert!(
        closed.load(SeqCst),
        "the resource acquired before the failure was not cleaned up"
    );
    assert_eq!(report.cleanup.len(), 1);
    assert!(matches!(
        report.service("api").unwrap().outcome,
        Some(Outcome::NotStarted)
    ));
    assert_eq!(never_started.load(SeqCst), 1);
    assert!(report.service("store").unwrap().initialized);
}

#[tokio::test(start_paused = true)]
async fn each_service_gets_its_own_grace_from_its_own_stop_request() {
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    app.service_fn("provider", |ctx| async move {
        ctx.ready();
        ctx.stopping().await;
        // Two seconds of cooperative teardown, after the consumer used two.
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    });
    app.service_fn(Spec::new("consumer").after("provider"), |ctx| async move {
        ctx.ready();
        ctx.stopping().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    });
    let app = app.start().unwrap();
    app.ready().await.unwrap();
    let report = app.shutdown().await.unwrap();
    assert!(
        report.elapsed >= Duration::from_secs(4),
        "{:?}",
        report.elapsed
    );
    assert!(report.services.iter().all(|service| !service.forced));
    assert!(
        report.is_success(),
        "the provider was aborted on a budget its consumer spent"
    );
}

#[tokio::test(start_paused = true)]
async fn a_root_that_ignores_stop_is_aborted_after_its_grace_and_reported_as_forced() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.shutdown_grace(Duration::from_secs(3));
    {
        let capture = Tracked(drops.clone());
        app.service_fn("stubborn", move |ctx| async move {
            let _capture = capture;
            ctx.ready();
            std::future::pending::<Result>().await
        });
    }
    let app = app.start().unwrap();
    app.ready().await.unwrap();
    app.stop();
    assert!(timeout(Duration::from_secs(2), app.wait()).await.is_err());
    let report = app.wait().await.unwrap();
    assert_eq!(drops.load(SeqCst), 1);
    let service = report.service("stubborn").unwrap();
    assert!(service.forced);
    assert!(matches!(service.outcome, Some(Outcome::Aborted)));
    assert!(matches!(service.failures[..], [Failure::Forced]));
    assert!(!report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocking_acquisition_registers_its_own_cleanup_when_its_root_is_aborted() {
    let (entered, started) = mpsc::channel();
    let (release, gate) = mpsc::channel::<()>();
    let closed = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    app.shutdown_grace(Duration::from_millis(50));
    {
        let closed = closed.clone();
        app.service_fn("journal", move |ctx| async move {
            let registrar = ctx.clone();
            let journal = ctx
                .spawn_blocking("open-journal", move || {
                    entered.send(()).unwrap();
                    let _ = gate.recv();
                    // Registered by the acquirer, before the value leaves this work.
                    registrar.defer("journal", async move {
                        closed.store(true, SeqCst);
                        Ok::<_, BoxError>(())
                    })?;
                    Ok::<_, BoxError>("journal handle")
                })?
                .await?;
            let _ = journal;
            idle(ctx).await
        });
    }
    let app = app.start().unwrap();
    started.recv().unwrap();
    app.stop();
    // The grace period passes with the open still in flight: the root is aborted.
    while !app.snapshot().service("journal").unwrap().forced {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        timeout(Duration::from_millis(100), app.wait())
            .await
            .is_err(),
        "reported while the open was running"
    );
    assert!(!closed.load(SeqCst));

    release.send(()).unwrap();
    let report = bounded(app.wait()).await.unwrap();
    assert!(
        closed.load(SeqCst),
        "the acquired resource was dropped without its asynchronous close"
    );
    assert_eq!(report.cleanup.len(), 1);
    assert!(!report.is_success(), "the root was forced");
}

#[derive(Debug, Default)]
struct Counters {
    received: AtomicUsize,
}

#[tokio::test]
async fn a_published_detail_appears_in_snapshots_and_the_report() {
    let counters = Arc::new(Counters::default());
    let mut app = App::new();
    {
        let counters = counters.clone();
        app.service_fn("udp", move |ctx| async move {
            ctx.publish(counters);
            idle(ctx).await
        });
    }
    let app = app.start().unwrap();
    bounded(app.ready()).await.unwrap();
    counters.received.fetch_add(3, SeqCst);
    let snapshot = app.snapshot();
    let service = snapshot.service("udp").unwrap();
    assert_eq!(
        service.detail::<Counters>().unwrap().received.load(SeqCst),
        3
    );
    assert!(service.detail::<String>().is_none());
    let report = bounded(app.shutdown()).await.unwrap();
    assert!(
        report
            .service("udp")
            .unwrap()
            .detail::<Counters>()
            .is_some()
    );
}

#[test]
fn runtime_loss_before_the_driver_runs_releases_captures_without_invoking_them() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let invoked = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let app = {
        let (invoked, capture) = (invoked.clone(), Tracked(drops.clone()));
        let mut app = App::new();
        app.service_fn("service", move |ctx| async move {
            let _capture = capture;
            invoked.store(true, SeqCst);
            idle(ctx).await
        });
        let _context = runtime.enter();
        app.start().unwrap()
    };
    // Nothing has been polled: the driver never ran.
    drop(runtime);
    assert!(!invoked.load(SeqCst));
    assert_eq!(drops.load(SeqCst), 1);
    assert_eq!(app.status().phase, Phase::Lost);

    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(matches!(
        observer.block_on(app.ready()),
        Err(NotReady::Lost(_))
    ));
    let lost = observer
        .block_on(app.wait())
        .expect_err("a lost driver must not produce a report");
    assert_eq!(lost.snapshot.status.phase, Phase::Lost);
}
