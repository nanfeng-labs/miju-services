#![forbid(unsafe_code)]

use super::*;
use crate::{
    BoxError, Health,
    service::{Lifetime, ServiceSpec, service_fn},
    work::WorkSpec,
};
use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Semaphore;

static FIXTURES: LazyLock<Mutex<HashMap<String, Arc<Fixture>>>> = LazyLock::new(Mutex::default);
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    builds: AtomicUsize,
    drops: AtomicUsize,
    error_drops: AtomicUsize,
    assembly_drops: AtomicUsize,
    cleanup_started: AtomicBool,
    factory_entered: AtomicBool,
    factory_open: Mutex<bool>,
    factory_changed: Condvar,
    cleanup_gate: Semaphore,
    blocking_entered: AtomicBool,
    blocking_open: Mutex<bool>,
    blocking_changed: Condvar,
}

fn fixture() -> (String, Arc<Fixture>) {
    let key = NEXT.fetch_add(1, Ordering::SeqCst).to_string();
    let fixture = Arc::new(Fixture {
        builds: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
        error_drops: AtomicUsize::new(0),
        assembly_drops: AtomicUsize::new(0),
        cleanup_started: AtomicBool::new(false),
        factory_entered: AtomicBool::new(false),
        factory_open: Mutex::new(false),
        factory_changed: Condvar::new(),
        cleanup_gate: Semaphore::new(0),
        blocking_entered: AtomicBool::new(false),
        blocking_open: Mutex::new(false),
        blocking_changed: Condvar::new(),
    });
    FIXTURES
        .lock()
        .unwrap()
        .insert(key.clone(), fixture.clone());
    (key, fixture)
}

struct Resource(Arc<Fixture>);

impl Drop for Resource {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct SecretError(Arc<Fixture>);

impl std::fmt::Debug for Fixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fixture").finish_non_exhaustive()
    }
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("secret-test-credential")
    }
}

impl std::error::Error for SecretError {}

impl Drop for SecretError {
    fn drop(&mut self) {
        self.0.error_drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn factory(args: Vec<String>) -> Result<App, BuildError> {
    let fixture = FIXTURES.lock().unwrap().get(&args[0]).unwrap().clone();
    fixture.builds.fetch_add(1, Ordering::SeqCst);
    let mode = args.get(1).cloned().unwrap_or_default();
    if mode == "factory-block" {
        fixture.factory_entered.store(true, Ordering::SeqCst);
        let mut open = fixture.factory_open.lock().unwrap();
        while !*open {
            open = fixture.factory_changed.wait(open).unwrap();
        }
    }
    if mode == "factory-panic" {
        panic!("private factory diagnostic");
    }
    if mode == "config" {
        return Err(BuildError::new(
            BuildErrorKind::Configuration,
            "safe configuration diagnostic",
        ));
    }
    if mode == "usage" {
        return Err(BuildError::new(
            BuildErrorKind::Usage,
            "safe usage diagnostic",
        ));
    }
    let finite = mode.starts_with("finite");
    let assembly = AssemblyCapture(fixture.clone());
    App::builder()
        .service(
            ServiceSpec::new("root").lifetime(if finite {
                Lifetime::Finite
            } else {
                Lifetime::Resident
            }),
            service_fn(move |context| async move {
                let _assembly = assembly;
                let resource = Resource(fixture.clone());
                let cleanup_fixture = fixture.clone();
                let slow = mode == "slow-cleanup";
                let cleanup_failed = mode == "finite-cleanup-fail";
                context
                    .defer_cleanup("resource", move || async move {
                        cleanup_fixture
                            .cleanup_started
                            .store(true, Ordering::SeqCst);
                        if slow {
                            cleanup_fixture
                                .cleanup_gate
                                .acquire()
                                .await
                                .unwrap()
                                .forget();
                        }
                        drop(resource);
                        if cleanup_failed {
                            Err::<(), BoxError>(Box::new(SecretError(cleanup_fixture)))
                        } else {
                            Ok(())
                        }
                    })
                    .map_err(|_| std::io::Error::other("cleanup registration rejected"))?;
                if mode == "blocking" {
                    let blocking_fixture = fixture.clone();
                    context
                        .work()
                        .spawn_blocking(WorkSpec::new("blocking"), move |_| {
                            blocking_fixture
                                .blocking_entered
                                .store(true, Ordering::SeqCst);
                            let mut open = blocking_fixture.blocking_open.lock().unwrap();
                            while !*open {
                                open = blocking_fixture.blocking_changed.wait(open).unwrap();
                            }
                            Ok::<(), BoxError>(())
                        })?;
                }
                if mode == "unknown" {
                    context.set_health(Health::Unknown);
                }
                context.ready();
                if mode == "finite-fail" {
                    return Err::<(), BoxError>(Box::new(SecretError(fixture)));
                }
                if finite {
                    return Ok::<(), BoxError>(());
                }
                context.stopping().await;
                if mode == "post-fail" {
                    return Err(Box::new(SecretError(fixture)) as BoxError);
                }
                Ok(())
            }),
        )
        .build()
        .map_err(|_| BuildError::new(BuildErrorKind::Startup, "test assembly failed"))
}

fn bridge(timeout: Duration) -> Arc<Bridge> {
    Arc::new(Bridge {
        slot: Mutex::new(None),
        factory,
        timeout,
        version: "test 1".into(),
    })
}

fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "condition did not become observable"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn ordinary_round_trip_restarts_only_after_resource_release() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    assert_eq!(bridge.version(), "test 1");
    assert_eq!(bridge.status(), IDLE);
    assert_eq!(bridge.start(vec![key.clone()]), 0);
    assert_eq!(bridge.status(), RUNNING);
    assert_eq!(bridge.start(vec![key.clone()]), ALREADY_RUNNING);
    assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.stop(2000), 0);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.status(), IDLE);
    assert_eq!(bridge.stop(0), NOT_RUNNING);
    assert_eq!(bridge.start(vec![key]), 0);
    assert_eq!(bridge.stop(2000), 0);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 2);
}

#[test]
fn concurrent_starts_build_only_one_application() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    let gate = Arc::new(std::sync::Barrier::new(3));
    let joins: Vec<_> = (0..2)
        .map(|_| {
            let bridge = bridge.clone();
            let key = key.clone();
            let gate = gate.clone();
            thread::spawn(move || {
                gate.wait();
                bridge.start(vec![key])
            })
        })
        .collect();
    gate.wait();
    let mut codes: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
    codes.sort();
    assert_eq!(codes, [0, ALREADY_RUNNING]);
    assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.stop(2000), 0);
}

#[test]
fn stop_waiters_have_independent_budgets_and_cleanup_keeps_slot() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    assert_eq!(bridge.start(vec![key.clone(), "slow-cleanup".into()]), 0);
    let long = bridge.clone();
    let long = thread::spawn(move || long.stop(2000));
    until(|| fixture.cleanup_started.load(Ordering::SeqCst));
    assert_eq!(bridge.stop(1), STOP_TIMED_OUT);
    assert_eq!(bridge.start(vec![key]), BUSY);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    fixture.cleanup_gate.add_permits(1);
    assert_eq!(long.join().unwrap(), 0);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn stop_during_factory_is_persistent_and_never_publishes_running() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    let starting = bridge.clone();
    let start = thread::spawn(move || starting.start(vec![key, "factory-block".into()]));
    until(|| fixture.factory_entered.load(Ordering::SeqCst));
    assert_eq!(bridge.stop(0), STOP_TIMED_OUT);
    assert_eq!(start.join().unwrap(), CANCELLED);
    assert_eq!(bridge.status(), STOPPING);
    *fixture.factory_open.lock().unwrap() = true;
    fixture.factory_changed.notify_all();
    until(|| bridge.status() == IDLE);
    assert!(!fixture.cleanup_started.load(Ordering::SeqCst));
}

#[test]
fn startup_deadline_latches_stop_even_while_factory_is_blocked() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_millis(30));
    assert_eq!(
        bridge.start(vec![key.clone(), "factory-block".into()]),
        STARTUP_FAILED
    );
    assert_eq!(bridge.start(vec![key]), BUSY);
    *fixture.factory_open.lock().unwrap() = true;
    fixture.factory_changed.notify_all();
    until(|| bridge.status() == FAILED);
    assert_eq!(bridge.stop(100), NOT_RUNNING);
}

#[test]
fn initialized_unknown_health_does_not_satisfy_start() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_millis(30));
    assert_eq!(bridge.start(vec![key, "unknown".into()]), STARTUP_FAILED);
    until(|| bridge.status() == FAILED);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn finite_success_and_cleanup_failure_use_actual_report() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    assert_eq!(bridge.start(vec![key.clone(), "finite".into()]), 0);
    until(|| bridge.status() == IDLE);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        bridge.start(vec![key, "finite-cleanup-fail".into()]),
        STARTUP_FAILED
    );
    until(|| bridge.status() == FAILED);
    assert_eq!(fixture.error_drops.load(Ordering::SeqCst), 1);
    assert!(!bridge.last_error().contains("secret-test"));
}

#[test]
fn old_metadata_waiter_does_not_hold_business_error_or_affect_new_generation() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    assert_eq!(bridge.start(vec![key.clone(), "post-fail".into()]), 0);
    let old = bridge.slot.lock().unwrap().clone().unwrap();
    assert_eq!(bridge.stop(2000), FAULT);
    assert_eq!(fixture.error_drops.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.start(vec![key]), 0);
    assert_eq!(old.stop_and_wait(Instant::now()), NOT_RUNNING);
    assert_eq!(bridge.status(), RUNNING);
    assert_eq!(bridge.stop(2000), 0);
}

#[test]
fn factory_errors_and_unwind_recover_after_runtime_release() {
    let (key, _) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    for (mode, expected) in [
        ("usage", USAGE),
        ("config", CONFIG),
        ("factory-panic", PANICKED),
    ] {
        assert_eq!(bridge.start(vec![key.clone(), mode.into()]), expected);
        until(|| bridge.status() == FAILED);
        // Failed is not itself a release guarantee; stop's NotRunning proves terminal release here.
        until(|| bridge.stop(0) == NOT_RUNNING);
    }
    assert_eq!(bridge.start(vec![key]), 0);
    assert_eq!(bridge.stop(2000), 0);
}

#[test]
fn blocking_work_prevents_reuse_until_its_real_completion() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    assert_eq!(bridge.start(vec![key.clone(), "blocking".into()]), 0);
    until(|| fixture.blocking_entered.load(Ordering::SeqCst));
    assert_eq!(bridge.stop(1), STOP_TIMED_OUT);
    assert_eq!(bridge.start(vec![key]), BUSY);
    *fixture.blocking_open.lock().unwrap() = true;
    fixture.blocking_changed.notify_all();
    assert_eq!(bridge.stop(2000), 0);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn invalid_start_budget_does_not_build_or_pollute_metadata() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::ZERO);
    assert_eq!(bridge.start(vec![key]), USAGE);
    assert_eq!(bridge.status(), IDLE);
    assert_eq!(bridge.last_error(), "");
    assert_eq!(fixture.builds.load(Ordering::SeqCst), 0);
}

#[test]
fn owner_rechecks_ready_deadline_without_a_waiting_java_thread() {
    let generation = Generation::new(Instant::now() - Duration::from_millis(1));
    generation.available().unwrap();
    assert_eq!(generation.wait_start(), STARTUP_FAILED);
    assert!(generation.stop.is_cancelled());
    generation.finish();
    assert_eq!(
        generation.state.lock().unwrap().terminal,
        Some(STARTUP_FAILED)
    );
}

#[test]
fn error_summary_truncates_at_utf8_boundary() {
    let message = "🦀".repeat(1100);
    let bounded = bounded_message(&message);
    assert_eq!(bounded.len(), 4096);
    assert!(message.starts_with(&bounded));
}

struct AssemblyCapture(Arc<Fixture>);

impl Drop for AssemblyCapture {
    fn drop(&mut self) {
        self.0.assembly_drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn abandoned_factory_result_releases_captures_before_reuse_without_async_cleanup() {
    let (key, fixture) = fixture();
    let bridge = bridge(Duration::from_secs(2));
    let starting = bridge.clone();
    let first_key = key.clone();
    let start = thread::spawn(move || starting.start(vec![first_key, "factory-block".into()]));
    until(|| fixture.factory_entered.load(Ordering::SeqCst));
    let old = bridge.slot.lock().unwrap().clone().unwrap();
    assert_eq!(bridge.stop(0), STOP_TIMED_OUT);
    assert_eq!(start.join().unwrap(), CANCELLED);
    assert_eq!(bridge.start(vec![key.clone()]), BUSY);
    *fixture.factory_open.lock().unwrap() = true;
    fixture.factory_changed.notify_all();
    until(|| old.state.lock().unwrap().reusable);
    assert_eq!(fixture.assembly_drops.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    assert!(!fixture.cleanup_started.load(Ordering::SeqCst));
    assert_eq!(bridge.start(vec![key]), 0);
    assert_eq!(bridge.stop(2000), 0);
    assert_eq!(fixture.assembly_drops.load(Ordering::SeqCst), 2);
}

#[test]
fn quarantine_never_opens_the_generation_slot() {
    let bridge = bridge(Duration::from_secs(2));
    let generation = bridge.reserve().unwrap();
    generation.quarantine(FAULT);
    generation.finish();
    assert_eq!(generation.wait_start(), FAULT);
    assert_eq!(bridge.start(Vec::new()), BUSY);
    assert_eq!(bridge.stop(0), FAULT);
    assert!(!generation.state.lock().unwrap().reusable);
}

#[test]
fn poisoned_metadata_cannot_allow_a_replacement_generation() {
    let bridge = bridge(Duration::from_secs(2));
    let generation = bridge.reserve().unwrap();
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _state = generation.state.lock().unwrap();
        panic!("poison metadata");
    }));
    assert_eq!(bridge.start(Vec::new()), FAULT);
    assert_eq!(bridge.status(), FAILED);
    assert_eq!(bridge.stop(0), FAULT);
}

#[test]
fn startup_failure_is_not_overwritten_by_late_readiness_or_clean_drain() {
    let generation = Generation::new(Instant::now() - Duration::from_millis(1));
    generation.expire_start();
    generation.available().unwrap();
    generation.finish();
    assert_eq!(generation.wait_start(), STARTUP_FAILED);
    assert_eq!(
        generation.state.lock().unwrap().terminal,
        Some(STARTUP_FAILED)
    );
}
