//! The lifecycle driver: one task that owns every start and stop decision.
//!
//! It wakes for few things — a stop request, `ready()`, a health change, a
//! root finishing, a scope draining, a critical fault, a deadline — and not
//! for ordinary work completing. Each wake it makes one sequential pass:
//! collect faults, start what may start or stop what may stop, enforce grace,
//! publish status. When every scope has drained it runs the cleanup stack and
//! publishes the report.
#![forbid(unsafe_code)]

use crate::{
    BoxError, cleanup,
    contain::drop_contained,
    observe::{Core, Published},
    report::{CleanupReport, Failure, Health, Outcome, Phase, Report, Status},
    scope::{RootEnd, RootLost},
    service::{Context, RootRecord, ServiceCell},
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::time::Instant;

pub(crate) type Entry =
    Box<dyn FnOnce(Context) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>> + Send>;

struct Slot {
    cell: Arc<ServiceCell>,
    entry: Option<Entry>,
    providers: Vec<usize>,
    consumers: Vec<usize>,
    started_at: Option<Instant>,
    stop_at: Option<Instant>,
    abort_sent: bool,
    root_seen: bool,
    critical_seen: bool,
    settled: bool,
}

impl Slot {
    /// Nothing of this service is left: it never started, or its root, work
    /// and leases have all released.
    fn drained(&self) -> bool {
        self.entry.is_none() && self.cell.scope.is_drained()
    }

    fn usable_as_provider(&self) -> bool {
        let state = self.cell.state();
        state.initialized && state.failures.is_empty()
    }
}

/// Publishes the loss of the driver if it is dropped before the report.
struct Guard(Arc<Core>);

impl Drop for Guard {
    fn drop(&mut self) {
        if self.0.report.get().is_none() {
            self.0.mark_lost();
        }
    }
}

pub(crate) struct Driver {
    core: Arc<Core>,
    slots: Vec<Slot>,
    startup_timeout: Duration,
    grace: Duration,
    guard: Guard,
}

impl Driver {
    pub(crate) fn new(
        core: Arc<Core>,
        entries: Vec<(Entry, Vec<usize>)>,
        startup_timeout: Duration,
        grace: Duration,
    ) -> Self {
        let mut consumers = vec![Vec::new(); entries.len()];
        for (consumer, (_, providers)) in entries.iter().enumerate() {
            for &provider in providers {
                consumers[provider].push(consumer);
            }
        }
        let slots = entries
            .into_iter()
            .zip(consumers)
            .zip(core.services.iter().cloned())
            .map(|(((entry, providers), consumers), cell)| Slot {
                cell,
                entry: Some(entry),
                providers,
                consumers,
                started_at: None,
                stop_at: None,
                abort_sent: false,
                root_seen: false,
                critical_seen: false,
                settled: false,
            })
            .collect();
        Self {
            guard: Guard(core.clone()),
            core,
            slots,
            startup_timeout,
            grace,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let now = Instant::now();
            self.collect_faults(now);
            if self.core.stop_requested() {
                self.stop_pass(now);
            } else {
                self.start_pass(now);
            }
            self.enforce_grace(now);
            if self.slots.iter().all(Slot::drained) {
                break;
            }
            self.publish(None);
            let deadline = self.next_deadline();
            tokio::select! {
                _ = self.core.wake_handle().notified() => {}
                _ = sleep_until(deadline) => {}
            }
        }
        // Every scope has drained, so every fault set is final. Read them once more.
        self.collect_faults(Instant::now());
        self.publish(Some(Phase::Cleaning));

        let mut reports = Vec::new();
        for deferred in self.core.cleanup.take_ordered() {
            self.core.cleanup.begin();
            let (error, panic) = cleanup::run(deferred.action).await;
            let report = CleanupReport {
                name: deferred.label,
                service: deferred.service,
                error,
                panic,
            };
            reports.push(report.clone());
            self.core.cleanup.finish(report);
        }

        let report = Arc::new(Report {
            services: self
                .core
                .services
                .iter()
                .map(|cell| Core::service_snapshot(cell))
                .collect(),
            cleanup: reports,
            elapsed: self.core.started.elapsed(),
        });
        let _ = self.core.report.set(report);
        self.core.published.send_modify(|published| {
            published.status.phase = Phase::Terminated;
            published.status.ready = false;
        });
        // The guard must see the report before it is dropped.
        drop(self.guard);
    }

    /// Turn finished roots, critical work faults and expired startup budgets
    /// into failures, settle finite services, and escalate.
    fn collect_faults(&mut self, now: Instant) {
        let stopping = self.core.stop_requested();
        let mut stop_application = false;
        for slot in &mut self.slots {
            if slot.started_at.is_none() {
                continue;
            }
            // Read "drained" first: once it is true the fault set cannot change.
            let drained = slot.cell.scope.is_drained();
            let critical = if slot.critical_seen {
                None
            } else {
                slot.cell.scope.critical_fault()
            };
            let mut state = slot.cell.state();
            if !slot.root_seen
                && let Some(root) = &state.root
            {
                slot.root_seen = true;
                let failure = match &root.outcome {
                    Outcome::Failed(error) => Some(Failure::Error(error.clone())),
                    Outcome::Panicked(message) => Some(Failure::Panic(message.clone())),
                    Outcome::Aborted => Some(Failure::Forced),
                    Outcome::Succeeded if !slot.cell.finite && !root.stopping => {
                        Some(Failure::ExitedEarly)
                    }
                    Outcome::Succeeded | Outcome::NotStarted => None,
                };
                let release_panic = root.release_panic;
                state.failures.extend(failure);
                if release_panic {
                    state.failures.push(Failure::ReleasePanic);
                }
            }
            if let Some(fault) = critical {
                slot.critical_seen = true;
                state.failures.push(Failure::Work(fault));
            }
            if !slot.cell.finite
                && !stopping
                && slot.stop_at.is_none()
                && !state.initialized
                && state.failures.is_empty()
                && slot
                    .started_at
                    .and_then(|at| at.checked_add(self.startup_timeout))
                    .is_some_and(|deadline| now >= deadline)
            {
                state.failures.push(Failure::StartupTimeout);
            }
            // A finite service is initialized by succeeding and draining.
            if slot.cell.finite && drained && !slot.settled {
                slot.settled = true;
                if state.failures.is_empty() {
                    state.initialized = true;
                    if !state.declared_health {
                        state.health = Health::Healthy;
                    }
                }
            }
            if state.failures.is_empty() {
                continue;
            }
            drop(state);
            if slot.cell.required {
                stop_application = true;
            } else if slot.stop_at.is_none() {
                // An optional service stops and drains by itself.
                slot.cell.scope.stop();
                slot.stop_at = Some(now);
            }
        }
        if stop_application {
            self.core.request_stop();
        }
    }

    fn start_pass(&mut self, now: Instant) {
        for index in 0..self.slots.len() {
            if self.slots[index].entry.is_none()
                || !self.slots[index]
                    .providers
                    .iter()
                    .all(|&provider| self.slots[provider].usable_as_provider())
            {
                continue;
            }
            let slot = &mut self.slots[index];
            let entry = slot.entry.take().expect("checked above");
            slot.started_at = Some(now);
            slot.cell.state().started = true;
            let ctx = Context::new(slot.cell.clone(), self.core.clone());
            let record = slot.cell.clone();
            slot.cell.scope.spawn_root(
                // The entry is invoked inside the task, so its panic is contained.
                async move { entry(ctx).await },
                Box::new(move |end: RootEnd| {
                    let outcome = match end.outcome {
                        Ok(Ok(())) => Outcome::Succeeded,
                        Ok(Err(error)) => Outcome::Failed(Arc::from(error)),
                        Err(RootLost::Panicked(message)) => Outcome::Panicked(message),
                        Err(RootLost::Aborted) => Outcome::Aborted,
                    };
                    record.state().root = Some(RootRecord {
                        outcome,
                        stopping: end.stopping,
                        release_panic: end.release_panic,
                    });
                }),
            );
        }
    }

    fn stop_pass(&mut self, now: Instant) {
        for slot in &mut self.slots {
            if let Some(entry) = slot.entry.take() {
                // Never invoked: its captures are released and it counts as drained.
                drop_contained(entry);
                slot.cell.state().not_started = true;
                slot.cell.scope.close();
            }
        }
        for index in 0..self.slots.len() {
            let slot = &self.slots[index];
            if slot.started_at.is_none() || slot.stop_at.is_some() {
                continue;
            }
            // A provider is asked to stop only after its consumers have drained.
            if slot
                .consumers
                .iter()
                .all(|&consumer| self.slots[consumer].drained())
            {
                self.slots[index].cell.scope.stop();
                self.slots[index].stop_at = Some(now);
            }
        }
    }

    /// After a service's own grace period, abort what can be aborted.
    fn enforce_grace(&mut self, now: Instant) {
        for slot in &mut self.slots {
            if slot.abort_sent || slot.drained() {
                continue;
            }
            if slot
                .stop_at
                .and_then(|at| at.checked_add(self.grace))
                .is_some_and(|deadline| now >= deadline)
            {
                slot.abort_sent = true;
                slot.cell.scope.abort();
            }
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let stopping = self.core.stop_requested();
        self.slots
            .iter()
            .filter_map(|slot| {
                if let Some(at) = slot.stop_at {
                    return (!slot.abort_sent && !slot.drained())
                        .then(|| at.checked_add(self.grace))
                        .flatten();
                }
                if stopping || slot.cell.finite {
                    return None;
                }
                let state = slot.cell.state();
                (!state.initialized && state.failures.is_empty())
                    .then(|| slot.started_at?.checked_add(self.startup_timeout))
                    .flatten()
            })
            .min()
    }

    fn publish(&self, phase: Option<Phase>) {
        let mut initialized = true;
        let mut required_failed = false;
        let mut required_healthy = true;
        let mut unknown = false;
        let mut unhealthy = false;
        let mut degraded = false;
        for cell in &self.core.services {
            let state = cell.state();
            let failed = !state.failures.is_empty();
            if cell.required {
                initialized &= state.initialized && !failed;
                required_failed |= failed;
                required_healthy &= state.health == Health::Healthy;
                unhealthy |= failed || state.health == Health::Unhealthy;
                unknown |= !state.initialized || state.health == Health::Unknown;
                degraded |= state.health == Health::Degraded;
            } else {
                degraded |= failed || !state.initialized || state.health != Health::Healthy;
            }
        }
        let health = if unhealthy {
            Health::Unhealthy
        } else if unknown {
            Health::Unknown
        } else if degraded {
            Health::Degraded
        } else {
            Health::Healthy
        };
        let phase = phase.unwrap_or(if self.core.stop_requested() {
            Phase::Stopping
        } else if initialized {
            Phase::Running
        } else {
            Phase::Starting
        });
        let next = Published {
            status: Status {
                phase,
                ready: phase == Phase::Running && initialized && required_healthy,
                health,
            },
            required_failed,
        };
        self.core.published.send_if_modified(|published| {
            // A stop request may have landed since `phase` was read.
            let mut next = next;
            if self.core.stop_requested()
                && matches!(next.status.phase, Phase::Starting | Phase::Running)
            {
                next.status.phase = Phase::Stopping;
                next.status.ready = false;
            }
            let changed = *published != next;
            *published = next;
            changed
        });
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
