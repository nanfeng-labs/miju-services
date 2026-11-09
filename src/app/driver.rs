use super::{AppOptions, Entry, Registration};
use crate::{
    cleanup::{CleanupReport, Registry},
    observe::AppObserver,
    report::{
        AppSnapshot, ExitReport, Health, Phase, ServiceFailure, ServiceOutcome, ServicePhase,
        ServiceSnapshot,
    },
    service::{ContextShared, ContextState, Criticality, Lifetime, ServiceContext, ServiceSpec},
    task::{self, TaskOutcome, TaskResult},
    work::WorkScope,
};
use std::{
    future::pending,
    sync::{Arc, Mutex, atomic::AtomicU64},
};
use tokio::{runtime::Handle, task::AbortHandle, time::Instant};
use tokio_util::sync::CancellationToken;

struct RootResult {
    result: TaskResult,
    stopped: bool,
}

struct Root {
    spec: ServiceSpec,
    providers: Vec<usize>,
    entry: Option<Entry>,
    context: ServiceContext,
    result: Arc<Mutex<Option<RootResult>>>,
    abort: Option<AbortHandle>,
    started_at: Option<Instant>,
    stop_at: Option<Instant>,
    abort_sent: bool,
    budget_exceeded: bool,
    failures: Vec<ServiceFailure>,
    result_seen: bool,
    work_fault_seen: bool,
}

impl Root {
    fn drained(&self) -> bool {
        self.entry.is_none()
            && self.result.lock().unwrap().is_some()
            && self.context.work.is_drained()
    }

    fn initialized(&self) -> bool {
        if self.spec.lifetime == Lifetime::Resident {
            return self.context.shared.state.lock().unwrap().initialized;
        }
        // Once actual drain is observed, the authoritative scope fault cannot change.
        // A cached driver failure list alone can miss the final child's result.
        let drained = self.drained();
        // A scope fault is final after drain; read it afterward so a concurrent
        // completion cannot appear between the fault check and the drain check.
        drained
            && self.context.work.fault().is_none()
            && self.failures.is_empty()
            && self.result.lock().unwrap().as_ref().is_some_and(|root| {
                matches!(root.result.outcome, TaskOutcome::Succeeded)
                    && root.result.release_panic.is_none()
            })
    }

    fn stop(&mut self, at: Instant) {
        self.stop_at = Some(self.stop_at.map_or(at, |old| old.min(at)));
        let state = self.context.shared.state.lock().unwrap();
        self.context.shared.stop.cancel();
        drop(state);
        self.context.work.stop();
    }

    fn spawn(&mut self, runtime: &Handle, entry: Entry) {
        let context = self.context.clone();
        let shared = context.shared.clone();
        let work = context.work.clone();
        let result = self.result.clone();
        self.abort = Some(task::spawn(
            runtime,
            shared.stop.clone(),
            move || entry(context),
            move |completion| {
                // No user capture remains. Closing the gate precedes publishing root release.
                work.close();
                let mut state = shared.state.lock().unwrap();
                state.root_released = true;
                let stopped = shared.stop.is_cancelled();
                drop(state);
                *result.lock().unwrap() = Some(RootResult {
                    result: completion,
                    stopped,
                });
                shared.changed.notify_waiters();
            },
        ));
    }

    fn snapshot(&self) -> ServiceSnapshot {
        let initialized = self.initialized();
        let state = self.context.shared.state.lock().unwrap();
        #[cfg(feature = "periodic")]
        let periodic = state.periodic.clone();
        #[cfg(feature = "miju")]
        let execution_errors = state.execution_errors;
        #[cfg(feature = "miju")]
        let recent_execution_errors = state.recent_execution_errors.iter().cloned().collect();
        let health = if self.spec.lifetime == Lifetime::Finite && initialized {
            Health::Healthy
        } else {
            state.health
        };
        drop(state);
        let outcome = self
            .result
            .lock()
            .unwrap()
            .as_ref()
            .map(|root| match &root.result.outcome {
                TaskOutcome::Succeeded => ServiceOutcome::Succeeded,
                TaskOutcome::Failed(error) => ServiceOutcome::Failed(error.clone()),
                TaskOutcome::Panicked(message) => ServiceOutcome::Panicked(message.clone()),
                TaskOutcome::CancelledBeforeStart => ServiceOutcome::CancelledBeforeStart,
                TaskOutcome::Aborted => ServiceOutcome::Aborted,
            });
        let phase = if self.started_at.is_none() {
            ServicePhase::NotStarted
        } else if self.drained() {
            ServicePhase::Completed
        } else if self.stop_at.is_some() || outcome.is_some() {
            ServicePhase::Draining
        } else if initialized {
            ServicePhase::Running
        } else {
            ServicePhase::Starting
        };
        ServiceSnapshot {
            #[cfg(feature = "periodic")]
            periodic,
            #[cfg(feature = "miju")]
            execution_errors,
            #[cfg(feature = "miju")]
            recent_execution_errors,
            id: self.spec.id.clone(),
            criticality: self.spec.criticality,
            lifetime: self.spec.lifetime,
            phase,
            initialized,
            health,
            outcome,
            failures: self.failures.clone(),
            work: self.context.work.snapshot(),
            stopping: self.context.is_stopping(),
            stop_budget_exceeded: self.budget_exceeded,
        }
    }
}

struct DriverGuard(AppObserver);

impl Drop for DriverGuard {
    fn drop(&mut self) {
        if self.0.snapshot().phase != Phase::Terminated {
            self.0.shared.lost();
        }
    }
}

pub(super) struct Driver {
    options: AppOptions,
    roots: Vec<Root>,
    cleanup: Arc<Registry>,
    observer: AppObserver,
    runtime: Handle,
    begun: Instant,
    budget_exceeded: bool,
    grace_reported: bool,
    guard: DriverGuard,
}

impl Driver {
    pub(super) fn new(
        options: AppOptions,
        entries: Vec<Registration>,
        providers: Vec<Vec<usize>>,
        cleanup: Arc<Registry>,
        observer: AppObserver,
        runtime: Handle,
    ) -> Self {
        let ids = Arc::new(AtomicU64::new(1));
        let roots = entries
            .into_iter()
            .zip(providers)
            .map(|(entry, providers)| {
                let context = ServiceContext {
                    id: entry.spec.id.clone(),
                    shared: Arc::new(ContextShared {
                        state: Mutex::new(ContextState {
                            initialized: false,
                            health: Health::Unknown,
                            declared_health: false,
                            root_released: false,
                            #[cfg(feature = "periodic")]
                            periodic: None,
                            #[cfg(feature = "miju")]
                            execution_errors: 0,
                            #[cfg(feature = "miju")]
                            recent_execution_errors: Default::default(),
                        }),
                        stop: CancellationToken::new(),
                        changed: observer.shared.wake.clone(),
                        #[cfg(feature = "miju")]
                        history_limit: options.recent_results,
                    }),
                    work: WorkScope::new(
                        entry.spec.id.clone(),
                        entry.spec.max_work.get(),
                        options.recent_results,
                        runtime.clone(),
                        observer.shared.wake.clone(),
                        ids.clone(),
                    ),
                    observer: observer.clone(),
                    cleanup: cleanup.clone(),
                };
                Root {
                    spec: entry.spec,
                    providers,
                    entry: Some(entry.run),
                    context,
                    result: Arc::new(Mutex::new(None)),
                    abort: None,
                    started_at: None,
                    stop_at: None,
                    abort_sent: false,
                    budget_exceeded: false,
                    failures: Vec::new(),
                    result_seen: false,
                    work_fault_seen: false,
                }
            })
            .collect();
        let driver = Self {
            options,
            roots,
            cleanup,
            observer: observer.clone(),
            runtime,
            begun: Instant::now(),
            budget_exceeded: false,
            grace_reported: false,
            guard: DriverGuard(observer),
        };
        driver.publish(Phase::Starting, None, Vec::new(), Vec::new());
        driver
    }

    fn check_faults(&mut self) {
        let now = Instant::now();
        for root in &mut self.roots {
            if !root.result_seen {
                let result = root.result.lock().unwrap();
                if let Some(completion) = result.as_ref() {
                    match &completion.result.outcome {
                        TaskOutcome::Failed(error) => {
                            root.failures.push(ServiceFailure::Error(error.clone()))
                        }
                        TaskOutcome::Panicked(message) => {
                            root.failures.push(ServiceFailure::Panic(message.clone()))
                        }
                        TaskOutcome::Aborted => root.failures.push(ServiceFailure::Aborted),
                        TaskOutcome::Succeeded
                            if root.spec.lifetime == Lifetime::Resident && !completion.stopped =>
                        {
                            root.failures.push(ServiceFailure::UnexpectedExit)
                        }
                        _ => {}
                    }
                    if let Some(message) = &completion.result.release_panic {
                        root.failures
                            .push(ServiceFailure::ReleasePanic(message.clone()));
                    }
                    root.result_seen = true;
                }
            }
            if !root.work_fault_seen
                && let Some(fault) = root.context.work.fault()
            {
                root.failures.push(ServiceFailure::Work(fault));
                root.work_fault_seen = true;
            }
            if root
                .started_at
                .is_some_and(|at| now >= deadline(at, self.options.startup_timeout))
                && root.spec.lifetime == Lifetime::Resident
                && !root.initialized()
                && root.stop_at.is_none()
                && root.failures.is_empty()
            {
                root.failures.push(ServiceFailure::StartupTimeout);
            }
            if !root.failures.is_empty() {
                if root.spec.criticality == Criticality::Required {
                    self.observer.shared.stop();
                } else if root.stop_at.is_none() {
                    root.stop(now);
                }
            }
        }
    }

    fn start_roots(&mut self) {
        for index in 0..self.roots.len() {
            if self.observer.shared.stop_at().is_some() {
                return;
            }
            if self.roots[index].entry.is_some()
                && self.roots[index].providers.iter().all(|&provider| {
                    let provider = &self.roots[provider];
                    provider.initialized()
                        && provider.failures.is_empty()
                        && (provider.spec.lifetime == Lifetime::Finite
                            || provider.context.work.fault().is_none())
                })
            {
                let entry = self.observer.shared.admit_start(|| {
                    let root = &mut self.roots[index];
                    root.started_at = Some(Instant::now());
                    root.entry.take().expect("entry checked before admission")
                });
                if let Some(entry) = entry {
                    self.roots[index].spawn(&self.runtime, entry);
                }
            }
        }
    }

    fn stop_roots(&mut self) {
        if let Some(at) = self.observer.shared.stop_at() {
            // Release unstarted factories through the same panic-aware envelope.
            for root in &mut self.roots {
                if let Some(entry) = root.entry.take() {
                    root.stop(at);
                    root.spawn(&self.runtime, entry);
                }
            }
            for provider in 0..self.roots.len() {
                let consumers_drained = self
                    .roots
                    .iter()
                    .all(|root| !root.providers.contains(&provider) || root.drained());
                if consumers_drained && self.roots[provider].stop_at.is_none_or(|local| at < local)
                {
                    self.roots[provider].stop(at);
                }
            }
        }
        let now = Instant::now();
        for root in &mut self.roots {
            if let Some(at) = root.stop_at {
                let grace = deadline(at, self.options.shutdown_grace);
                if now >= grace && !root.abort_sent {
                    if let Some(abort) = &root.abort {
                        abort.abort();
                    }
                    root.context.work.abort();
                    root.abort_sent = true;
                }
                if now >= deadline(grace, self.options.abort_wait) && !root.drained() {
                    root.budget_exceeded = true;
                }
            }
        }
    }

    fn budgets(&mut self, incomplete: bool) -> bool {
        let mut warn = false;
        if let Some(at) = self.observer.shared.stop_at() {
            let grace = deadline(at, self.options.shutdown_grace);
            if Instant::now() >= grace && !self.grace_reported {
                self.grace_reported = true;
                warn = incomplete;
            }
            if Instant::now() >= deadline(grace, self.options.abort_wait) && incomplete {
                self.budget_exceeded = true;
            }
        }
        if !self.grace_reported
            && self.roots.iter().any(|root| {
                root.stop_at
                    .is_some_and(|at| Instant::now() >= deadline(at, self.options.shutdown_grace))
                    && !root.drained()
            })
        {
            self.grace_reported = true;
            warn = true;
        }
        warn
    }

    fn next_deadline(&self, cleaning: bool) -> Option<Instant> {
        let mut deadlines = Vec::new();
        if let Some(at) = self.observer.shared.stop_at() {
            let grace = deadline(at, self.options.shutdown_grace);
            if !self.grace_reported {
                deadlines.push(grace);
            }
            if !self.budget_exceeded {
                deadlines.push(deadline(grace, self.options.abort_wait));
            }
        }
        if !cleaning {
            for root in &self.roots {
                if let Some(at) = root.stop_at {
                    let grace = deadline(at, self.options.shutdown_grace);
                    if !root.abort_sent {
                        deadlines.push(grace);
                    }
                    if !root.budget_exceeded && !root.drained() {
                        deadlines.push(deadline(grace, self.options.abort_wait));
                    }
                } else if let Some(at) = startup_deadline(
                    root.spec.lifetime,
                    root.started_at,
                    !root.initialized(),
                    !root.failures.is_empty(),
                    self.options.startup_timeout,
                ) {
                    deadlines.push(at);
                }
            }
        }
        deadlines.into_iter().min()
    }

    fn publish(
        &self,
        phase: Phase,
        current_cleanup: Option<String>,
        pending_cleanup: Vec<String>,
        cleanup: Vec<CleanupReport>,
    ) -> Arc<AppSnapshot> {
        let services: Vec<_> = self.roots.iter().map(Root::snapshot).collect();
        let required: Vec<_> = services
            .iter()
            .filter(|s| s.criticality == Criticality::Required)
            .collect();
        let initialized = required
            .iter()
            .all(|s| s.initialized && s.failures.is_empty());
        let health = if required
            .iter()
            .any(|s| !s.failures.is_empty() || s.health == Health::Unhealthy)
        {
            Health::Unhealthy
        } else if required
            .iter()
            .any(|s| !s.initialized || s.health == Health::Unknown)
        {
            Health::Unknown
        } else if required.iter().any(|s| s.health == Health::Degraded)
            || services.iter().any(|s| {
                s.criticality == Criticality::Optional
                    && (!s.initialized || s.health != Health::Healthy || !s.failures.is_empty())
            })
        {
            Health::Degraded
        } else {
            Health::Healthy
        };
        let phase = if phase == Phase::Starting && initialized {
            Phase::Running
        } else {
            phase
        };
        self.observer.shared.publish(AppSnapshot {
            phase,
            ready: phase == Phase::Running
                && initialized
                && required.iter().all(|s| s.health == Health::Healthy),
            health,
            services,
            current_cleanup,
            pending_cleanup,
            cleanup,
            budget_exceeded: self.budget_exceeded,
            driver_lost: false,
        })
    }

    pub(super) async fn run(mut self) {
        loop {
            // Register the notification before reading state so no transition is lost.
            let wake = self.observer.shared.wake.clone();
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.check_faults();
            self.start_roots();
            self.stop_roots();
            if self.roots.iter().all(Root::drained) {
                // The last work fault may have arrived after check_faults above.
                self.check_faults();
                break;
            }
            let warn = self.budgets(true);
            let snapshot = self.publish(
                if self.observer.shared.stop_at().is_some() {
                    Phase::Stopping
                } else {
                    Phase::Starting
                },
                None,
                Vec::new(),
                Vec::new(),
            );
            warn_overrun(warn, snapshot);
            let next = self.next_deadline(false);
            tokio::select! {
                _ = notified => {},

                _ = sleep_to(next) => {},
            }
        }
        let mut actions = self.cleanup.take();
        let mut reports = Vec::new();
        while let Some(action) = actions.pop() {
            let label = action.label;
            let id = action.id;
            let current = label.clone();
            let pending: Vec<_> = actions.iter().rev().map(|a| a.label.clone()).collect();
            self.publish(
                Phase::Cleaning,
                Some(current.clone()),
                pending.clone(),
                reports.clone(),
            );
            let (send, mut receive) = tokio::sync::oneshot::channel();
            task::spawn(
                &self.runtime,
                CancellationToken::new(),
                action.close,
                move |result| {
                    let _ = send.send(result);
                },
            );
            let result = loop {
                let wake = self.observer.shared.wake.clone();
                let notified = wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let warn = self.budgets(true);
                let snapshot = self.publish(
                    Phase::Cleaning,
                    Some(current.clone()),
                    pending.clone(),
                    reports.clone(),
                );
                warn_overrun(warn, snapshot);
                tokio::select! {
                    biased;
                    result = &mut receive => match result {
                        Ok(result) => break result,
                        Err(_) => return, // Guard publishes driver loss, never a completion certificate.
                    },
                    _ = notified => {},
                    _ = sleep_to(self.next_deadline(true)) => {},
                }
            };
            let (error, panic) = match result.outcome {
                TaskOutcome::Succeeded => (None, None),
                TaskOutcome::Failed(error) => (Some(error), None),
                TaskOutcome::Panicked(message) => (None, Some(message)),
                _ => return,
            };
            reports.push(CleanupReport {
                id,
                label,
                error,
                panic,
                release_panic: result.release_panic,
            });
        }
        self.budgets(false);
        let snapshot = self.publish(Phase::Terminated, None, Vec::new(), reports);
        self.observer.shared.finish(Arc::new(ExitReport {
            snapshot,
            elapsed: self.begun.elapsed(),
        }));
        // Keep the guard alive through report publication, including cancellation before first poll.
        let _ = &self.guard;
    }
}

fn warn_overrun(warn: bool, snapshot: Arc<AppSnapshot>) {
    #[cfg(feature = "tracing")]
    if warn {
        tracing::warn!(snapshot = ?snapshot, "service stop grace exceeded; actual release is still pending");
    }
    #[cfg(not(feature = "tracing"))]
    let _ = (warn, snapshot);
}

fn deadline(at: Instant, within: std::time::Duration) -> Instant {
    at.checked_add(within)
        .expect("build validated representable lifecycle budgets")
}

fn startup_deadline(
    lifetime: Lifetime,
    started_at: Option<Instant>,
    uninitialized: bool,
    has_failures: bool,
    timeout: std::time::Duration,
) -> Option<Instant> {
    if lifetime != Lifetime::Resident || !uninitialized || has_failures {
        return None;
    }
    started_at.map(|at| deadline(at, timeout))
}

async fn sleep_to(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::App;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Waker},
        time::Duration,
    };
    use tokio::{sync::Notify, time::Instant};

    struct Capture(Arc<AtomicBool>);

    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn controlled_driver(app: App, runtime: &tokio::runtime::Runtime) -> Driver {
        let observer = AppObserver {
            shared: crate::observe::Shared::new(
                AppSnapshot {
                    phase: Phase::Starting,
                    ready: false,
                    health: Health::Unknown,
                    services: Vec::new(),
                    current_cleanup: None,
                    pending_cleanup: Vec::new(),
                    cleanup: Vec::new(),
                    budget_exceeded: false,
                    driver_lost: false,
                },
                Arc::new(Notify::new()),
            ),
        };
        let cleanup = Arc::new(Registry::default());
        for (label, close) in app.cleanup {
            cleanup.register(label, close).unwrap();
        }
        Driver::new(
            app.options,
            app.services,
            app.dependencies,
            cleanup,
            observer,
            runtime.handle().clone(),
        )
    }

    #[test]
    fn runtime_loss_before_root_poll_runs_the_root_release_callback() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = Capture(dropped.clone());
        let called = Arc::new(AtomicBool::new(false));
        let invocation = called.clone();
        let app = App::builder()
            .service_fn("root", move |_| async move {
                let _capture = capture;
                invocation.store(true, Ordering::SeqCst);
                Ok(())
            })
            .build()
            .unwrap();
        let mut driver = controlled_driver(app, &runtime);
        let root = &mut driver.roots[0];
        let entry = root.entry.take().unwrap();
        root.spawn(runtime.handle(), entry);
        drop(runtime);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!called.load(Ordering::SeqCst));
        assert!(root.context.shared.state.lock().unwrap().root_released);
        assert!(root.context.work.is_drained());
        assert!(matches!(
            root.result.lock().unwrap().as_ref().unwrap().result.outcome,
            TaskOutcome::CancelledBeforeStart
        ));
    }

    #[test]
    fn runtime_loss_before_cleanup_poll_releases_it_without_a_certificate() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = Capture(dropped.clone());
        let called = Arc::new(AtomicBool::new(false));
        let invocation = called.clone();
        let app = App::builder()
            .service_fn(
                ServiceSpec::new("finite").lifetime(Lifetime::Finite),
                |_| async { Ok(()) },
            )
            .cleanup("not-polled", move || async move {
                let _capture = capture;
                invocation.store(true, Ordering::SeqCst);
                Ok(())
            })
            .build()
            .unwrap();
        let mut driver = controlled_driver(app, &runtime);
        let root = &mut driver.roots[0];
        let entry = root.entry.take().unwrap();
        root.spawn(runtime.handle(), entry);
        runtime.block_on(root.context.work.wait());
        assert!(root.drained());
        let observer = driver.observer.clone();
        let mut run = Box::pin(driver.run());
        {
            let _entered = runtime.enter();
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(
            observer.snapshot().current_cleanup.as_deref(),
            Some("not-polled")
        );
        runtime.spawn(run);
        drop(runtime);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!called.load(Ordering::SeqCst));
        let other = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        assert!(
            other
                .block_on(observer.wait())
                .unwrap_err()
                .snapshot
                .driver_lost
        );
    }

    #[test]
    fn startup_deadline_only_applies_to_resident_services() {
        let started_at = Instant::now();
        let timeout = Duration::from_secs(30);

        assert_eq!(
            startup_deadline(Lifetime::Finite, Some(started_at), true, false, timeout),
            None
        );
        assert_eq!(
            startup_deadline(Lifetime::Resident, Some(started_at), true, false, timeout),
            Some(started_at + timeout)
        );
        assert_eq!(
            startup_deadline(Lifetime::Resident, None, true, false, timeout),
            None
        );
    }
}
