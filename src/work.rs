//! Bounded background work owned by a service.
//!
//! Closing a scope prevents new work. Stopping also notifies existing work;
//! neither operation proves that it has finished. Only actual release permits
//! dependencies and cleanup to proceed. An aggregate confirms external work
//! with `Ok(())`; a failed aggregate retains its responsibility indefinitely.
#![forbid(unsafe_code)]

use crate::{
    BoxError, SharedError,
    service::ServiceId,
    task::{self, TaskOutcome, TaskResult},
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    runtime::Handle,
    sync::{Notify, watch},
    task::AbortHandle,
    time::Instant,
};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};

/// An application-local, monotonically allocated work identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkId(u64);

impl WorkId {
    /// Return the numeric identity within this application.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WorkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Admission and error policy for one execution.
#[derive(Clone, Debug)]
pub struct WorkSpec {
    label: Arc<str>,
    aggregate: bool,
    fail_service: bool,
}

impl WorkSpec {
    /// Record errors without stopping the owning service.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: Arc::from(label.into()),
            aggregate: false,
            fail_service: false,
        }
    }

    /// Promote errors, panics and failed release to an owner lifecycle fault.
    #[must_use]
    pub fn fail_service(mut self) -> Self {
        self.fail_service = true;
        self
    }

    /// Own external work whose actual completion is certified by `Ok(())`.
    ///
    /// Start external work inside the admitted factory. Once started, aggregate
    /// work is never forcibly aborted. Errors retain its identity and capacity
    /// because releasing an entry does not prove its external work has ended.
    pub fn aggregate(label: impl Into<String>) -> Self {
        Self {
            label: Arc::from(label.into()),
            aggregate: true,
            fail_service: true,
        }
    }
}

/// Admission failed before the factory was invoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SpawnError {
    /// The scope no longer accepts work.
    #[error("work admission is closed")]
    Closed,
    /// Queued, running and unconfirmed work occupy every slot.
    #[error("work capacity is exhausted")]
    AtCapacity,
    /// The runtime cannot accept this work.
    ///
    /// Reserved for runtime admission failures; currently not returned. Accepted
    /// work uses the captured Tokio handle and reports cancellation through its handle.
    #[error("runtime is unavailable")]
    RuntimeUnavailable,
    /// Blocking work cannot certify aggregate completion.
    #[error("aggregate work requires asynchronous spawn")]
    IncompatibleSpec,
    /// This application's numeric identity space was exhausted.
    #[error("work identity space is exhausted")]
    IdentityExhausted,
}

/// Execution result, independent of external completion.
#[derive(Clone, Debug)]
pub enum WorkOutcome {
    /// The entry returned successfully.
    Succeeded,
    /// The original business error.
    Failed(SharedError),
    /// The entry unwound.
    Panicked(Arc<str>),
    /// Cancellation won before the factory was invoked.
    CancelledBeforeStart,
    /// A started ordinary future was forcibly dropped.
    Aborted,
}

/// Whether all responsibility represented by a work item actually ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkCompletion {
    /// The entry and captures were released, with any external completion certified.
    Confirmed,
    /// External completion is unknown after aggregate failure.
    Unconfirmed,
}

/// Immutable result published after user captures have been released.
#[derive(Clone, Debug)]
pub struct WorkReport {
    owner: ServiceId,
    id: WorkId,
    label: Arc<str>,
    outcome: WorkOutcome,
    completion: WorkCompletion,
    release_panic: Option<Arc<str>>,
    stop_requested: bool,
    finished_at: Instant,
    was_aborted: bool,
}

impl WorkReport {
    /// Owning service.
    pub fn owner(&self) -> &ServiceId {
        &self.owner
    }

    /// Application-local identity.
    pub fn id(&self) -> WorkId {
        self.id
    }

    /// Display label, which need not be unique.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Business execution result.
    pub fn outcome(&self) -> &WorkOutcome {
        &self.outcome
    }

    /// Actual completion certificate.
    pub fn completion(&self) -> WorkCompletion {
        self.completion
    }

    /// A destructor panic, retained alongside any original execution failure.
    pub fn release_panic(&self) -> Option<&Arc<str>> {
        self.release_panic.as_ref()
    }

    /// Whether stop had been requested at publication.
    pub fn stop_requested(&self) -> bool {
        self.stop_requested
    }

    /// Actual release observation time.
    pub fn finished_at(&self) -> Instant {
        self.finished_at
    }

    /// Whether forced cancellation dropped a started ordinary future.
    pub fn was_aborted(&self) -> bool {
        self.was_aborted
    }
}

/// Execution type, determining which stop guarantees apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkKind {
    /// Cooperatively stopped, then abortable after the grace period.
    Async,
    /// Cannot be interrupted after the blocking entry starts.
    Blocking,
    /// Must certify completion of all external responsibility.
    Aggregate,
}

/// One active or externally unconfirmed responsibility.
#[derive(Clone, Debug)]
pub struct WorkSnapshot {
    /// Application-local identity.
    pub id: WorkId,
    /// Display label.
    pub label: Arc<str>,
    /// Execution and cancellation contract.
    pub kind: WorkKind,
    /// Whether the user factory has begun.
    pub started: bool,
    /// Whether a cooperative stop was requested.
    pub stopping: bool,
    /// Entry result when external responsibility remains unconfirmed.
    pub report: Option<Arc<WorkReport>>,
}

/// Current admission, execution and recent-result state for a service.
#[derive(Clone, Debug)]
pub struct ScopeSnapshot {
    /// Whether admission is closed.
    pub closed: bool,
    /// Queued, executing or externally unconfirmed work.
    pub active: Vec<WorkSnapshot>,
    /// Bounded recent results in completion order.
    pub recent: Vec<Arc<WorkReport>>,
    /// Number of external responsibilities without completion certificates.
    pub unconfirmed: usize,
    /// Whether any started ordinary future was forcibly dropped.
    pub was_aborted: bool,
}

struct AbortSlot {
    handle: Option<AbortHandle>,
    requested: bool,
}

struct Control {
    id: WorkId,
    stop: CancellationToken,
    started: AtomicBool,
    abort: Mutex<AbortSlot>,
    result: watch::Sender<Option<Arc<WorkReport>>>,
    changed: Arc<Notify>,
}

impl Control {
    fn install(&self, handle: AbortHandle) {
        let requested = {
            let mut slot = self.abort.lock().expect("abort slot contains no user code");
            let requested = slot.requested;
            slot.handle = Some(handle.clone());
            requested
        };
        if requested {
            handle.abort();
        }
    }

    fn abort(&self) {
        let handle = {
            let mut slot = self.abort.lock().expect("abort slot contains no user code");
            slot.requested = true;
            slot.handle.clone()
        };
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    fn stop(&self) {
        self.stop.cancel();
        self.changed.notify_waiters();
    }
}

struct Entry {
    control: Arc<Control>,
    spec: WorkSpec,
    kind: WorkKind,
    report: Option<Arc<WorkReport>>,
}

struct Gate {
    closed: bool,
    entries: BTreeMap<WorkId, Entry>,
    recent: VecDeque<Arc<WorkReport>>,
    fault: Option<Arc<WorkReport>>,
    was_aborted: bool,
}

struct Inner {
    owner: ServiceId,
    capacity: usize,
    recent: usize,
    runtime: Handle,
    changed: Arc<Notify>,
    ids: Arc<AtomicU64>,
    tracker: TaskTracker,
    gate: Mutex<Gate>,
}

/// A cloneable service-owned admission gate and actual-release barrier.
///
/// `close()` only closes admission; `wait()` does not request cancellation and
/// does not include the service root. Dropping a scope or work handle does not
/// cancel accepted work. Keep the application's runtime alive until it drains.
#[derive(Clone)]
pub struct WorkScope(Arc<Inner>);

impl fmt::Debug for WorkScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkScope")
            .field("owner", &self.0.owner)
            .field("state", &self.snapshot())
            .finish()
    }
}

impl WorkScope {
    pub(crate) fn new(
        owner: ServiceId,
        capacity: usize,
        recent: usize,
        runtime: Handle,
        changed: Arc<Notify>,
        ids: Arc<AtomicU64>,
    ) -> Self {
        Self(Arc::new(Inner {
            owner,
            capacity,
            recent,
            runtime,
            changed,
            ids,
            tracker: TaskTracker::new(),
            gate: Mutex::new(Gate {
                closed: false,
                entries: BTreeMap::new(),
                recent: VecDeque::new(),
                fault: None,
                was_aborted: false,
            }),
        }))
    }

    fn admit(
        &self,
        spec: WorkSpec,
        kind: WorkKind,
    ) -> Result<(Arc<Control>, TaskTrackerToken), SpawnError> {
        let mut gate = self.0.gate.lock().expect("work gate contains no user code");
        if gate.closed {
            return Err(SpawnError::Closed);
        }
        if gate.entries.len() >= self.0.capacity {
            return Err(SpawnError::AtCapacity);
        }
        #[allow(deprecated, reason = "try_update is newer than the Rust 1.89 MSRV")]
        let id = self
            .0
            .ids
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map(WorkId)
            .map_err(|_| SpawnError::IdentityExhausted)?;
        let (result, _) = watch::channel(None);
        let control = Arc::new(Control {
            id,
            stop: CancellationToken::new(),
            started: AtomicBool::new(false),
            abort: Mutex::new(AbortSlot {
                handle: None,
                requested: false,
            }),
            result,
            changed: self.0.changed.clone(),
        });
        let token = self.0.tracker.token();
        gate.entries.insert(
            id,
            Entry {
                control: control.clone(),
                spec,
                kind,
                report: None,
            },
        );
        Ok((control, token))
    }

    /// Accept one ordinary future or external aggregate before invoking its factory.
    ///
    /// Full and closed scopes reject immediately, without running the factory.
    pub fn spawn<F, Fut, E>(&self, spec: WorkSpec, run: F) -> Result<WorkHandle, SpawnError>
    where
        F: FnOnce(WorkContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let kind = if spec.aggregate {
            WorkKind::Aggregate
        } else {
            WorkKind::Async
        };
        let (control, token) = self.admit(spec, kind)?;
        let handle = WorkHandle(control.clone());
        let entry = control.clone();
        let owner = self.clone();
        let completed = control.clone();
        let abort = task::spawn(
            &self.0.runtime,
            control.stop.clone(),
            move || {
                entry.started.store(true, Ordering::Release);
                run(WorkContext {
                    id: entry.id,
                    stop: entry.stop.clone(),
                })
            },
            move |result| {
                owner.finish(&completed, result);
                drop(token);
                owner.0.changed.notify_waiters();
            },
        );
        control.install(abort);
        self.0.changed.notify_waiters();
        Ok(handle)
    }

    /// Accept blocking work; once started it must finish cooperatively.
    pub fn spawn_blocking<F, E>(&self, spec: WorkSpec, run: F) -> Result<WorkHandle, SpawnError>
    where
        F: FnOnce(WorkContext) -> Result<(), E> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        if spec.aggregate {
            return Err(SpawnError::IncompatibleSpec);
        }
        let (control, token) = self.admit(spec, WorkKind::Blocking)?;
        let handle = WorkHandle(control.clone());
        let entry = control.clone();
        let owner = self.clone();
        let completed = control.clone();
        let abort = task::spawn_blocking(
            &self.0.runtime,
            control.stop.clone(),
            move || {
                entry.started.store(true, Ordering::Release);
                run(WorkContext {
                    id: entry.id,
                    stop: entry.stop.clone(),
                })
            },
            move |result| {
                owner.finish(&completed, result);
                drop(token);
                owner.0.changed.notify_waiters();
            },
        );
        control.install(abort);
        self.0.changed.notify_waiters();
        Ok(handle)
    }

    fn finish(&self, control: &Control, result: TaskResult) {
        let outcome = match result.outcome {
            TaskOutcome::Succeeded => WorkOutcome::Succeeded,
            TaskOutcome::Failed(error) => WorkOutcome::Failed(error),
            TaskOutcome::Panicked(message) => WorkOutcome::Panicked(message),
            TaskOutcome::CancelledBeforeStart => WorkOutcome::CancelledBeforeStart,
            TaskOutcome::Aborted => WorkOutcome::Aborted,
        };
        let (removed, evicted, report) = {
            let mut gate = self.0.gate.lock().expect("work gate contains no user code");
            let entry = gate
                .entries
                .get(&control.id)
                .expect("accepted work publishes once");
            let completion = if entry.kind != WorkKind::Aggregate
                || matches!(outcome, WorkOutcome::CancelledBeforeStart)
                || (matches!(outcome, WorkOutcome::Succeeded) && result.release_panic.is_none())
            {
                WorkCompletion::Confirmed
            } else {
                WorkCompletion::Unconfirmed
            };
            let fault = entry.spec.fail_service
                && (!matches!(
                    outcome,
                    WorkOutcome::Succeeded | WorkOutcome::CancelledBeforeStart
                ) || result.release_panic.is_some());
            let report = Arc::new(WorkReport {
                owner: self.0.owner.clone(),
                id: control.id,
                label: entry.spec.label.clone(),
                outcome,
                completion,
                release_panic: result.release_panic,
                stop_requested: control.stop.is_cancelled(),
                finished_at: Instant::now(),
                was_aborted: result.was_aborted,
            });
            gate.was_aborted |= report.was_aborted;
            if fault && gate.fault.is_none() {
                gate.fault = Some(report.clone());
            }
            let removed = if completion == WorkCompletion::Confirmed {
                gate.entries.remove(&control.id)
            } else {
                gate.entries
                    .get_mut(&control.id)
                    .expect("entry retained")
                    .report = Some(report.clone());
                None
            };
            gate.recent.push_back(report.clone());
            let mut evicted = Vec::new();
            while gate.recent.len() > self.0.recent {
                evicted.push(gate.recent.pop_front().expect("queue exceeds capacity"));
            }
            (removed, evicted, report)
        };
        let previous = control.result.send_replace(Some(report));
        task::retire((removed, evicted, previous));
    }

    /// Close admission without stopping already accepted work.
    pub fn close(&self) {
        {
            let mut gate = self.0.gate.lock().expect("work gate contains no user code");
            gate.closed = true;
            self.0.tracker.close();
        }
        self.0.changed.notify_waiters();
    }

    /// Wait for closed admission and actual release of all responsibility.
    pub async fn wait(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_drained() {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn stop(&self) {
        let controls = {
            let mut gate = self.0.gate.lock().expect("work gate contains no user code");
            gate.closed = true;
            self.0.tracker.close();
            gate.entries
                .values()
                .map(|entry| entry.control.clone())
                .collect::<Vec<_>>()
        };
        for control in controls {
            control.stop();
        }
        self.0.changed.notify_waiters();
    }

    pub(crate) fn abort(&self) {
        let controls = {
            let gate = self.0.gate.lock().expect("work gate contains no user code");
            gate.entries
                .values()
                .filter(|entry| {
                    entry.kind != WorkKind::Aggregate
                        && (entry.kind == WorkKind::Async
                            || !entry.control.started.load(Ordering::Acquire))
                })
                .map(|entry| entry.control.clone())
                .collect::<Vec<_>>()
        };
        for control in controls {
            control.abort();
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        let gate = self.0.gate.lock().expect("work gate contains no user code");
        gate.closed && gate.entries.is_empty() && self.0.tracker.is_empty()
    }

    pub(crate) fn fault(&self) -> Option<Arc<WorkReport>> {
        self.0
            .gate
            .lock()
            .expect("work gate contains no user code")
            .fault
            .clone()
    }

    /// Return admission, active work and bounded recent results.
    pub fn snapshot(&self) -> ScopeSnapshot {
        let gate = self.0.gate.lock().expect("work gate contains no user code");
        ScopeSnapshot {
            closed: gate.closed,
            active: gate
                .entries
                .values()
                .map(|entry| WorkSnapshot {
                    id: entry.control.id,
                    label: entry.spec.label.clone(),
                    kind: entry.kind,
                    started: entry.control.started.load(Ordering::Acquire),
                    stopping: entry.control.stop.is_cancelled(),
                    report: entry.report.clone(),
                })
                .collect(),
            recent: gate.recent.iter().cloned().collect(),
            unconfirmed: gate
                .entries
                .values()
                .filter(|entry| entry.report.is_some())
                .count(),
            was_aborted: gate.was_aborted,
        }
    }
}

/// Read-only stop notification for one work item.
#[derive(Clone, Debug)]
pub struct WorkContext {
    id: WorkId,
    stop: CancellationToken,
}

impl WorkContext {
    /// Identity of this execution.
    pub fn id(&self) -> WorkId {
        self.id
    }

    /// Whether cooperative stop was requested.
    pub fn is_stopping(&self) -> bool {
        self.stop.is_cancelled()
    }

    /// Wait for cooperative stop.
    pub async fn stopping(&self) {
        self.stop.cancelled().await;
    }
}

/// Cloneable control and result observation for accepted work.
///
/// Dropping this handle or cancelling `wait()` does not cancel the work.
#[derive(Clone)]
pub struct WorkHandle(Arc<Control>);

impl fmt::Debug for WorkHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkHandle")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl WorkHandle {
    /// Identity of this execution.
    pub fn id(&self) -> WorkId {
        self.0.id
    }

    /// Request cooperative stop for only this work item.
    pub fn stop(&self) {
        self.0.stop();
    }

    /// Wait for the entry result; check its completion certificate separately.
    pub async fn wait(&self) -> Arc<WorkReport> {
        let mut result = self.0.result.subscribe();
        loop {
            if let Some(report) = result.borrow_and_update().clone() {
                return report;
            }
            result
                .changed()
                .await
                .expect("the control retains its result sender");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::Poll;
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct RetiredError {
        panic_on_drop: bool,
        drops: Arc<AtomicUsize>,
    }

    impl fmt::Display for RetiredError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("recorded error with an observable destructor")
        }
    }

    impl std::error::Error for RetiredError {}

    impl Drop for RetiredError {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "recorded-error-drop-panic");
        }
    }

    async fn scope_waiter_wakes_after_retirement(panic_on_drop: bool) -> bool {
        // Construct the real scope directly, without an App snapshot retaining
        // another Arc to the old report. Execution and finish use product code.
        let scope = WorkScope::new(
            ServiceId(Arc::from("scope")),
            2,
            1,
            Handle::current(),
            Arc::new(Notify::new()),
            Arc::new(AtomicU64::new(0)),
        );
        let drops = Arc::new(AtomicUsize::new(0));
        let error = RetiredError {
            panic_on_drop,
            drops: drops.clone(),
        };
        let first = scope
            .spawn(WorkSpec::new("recorded-error"), move |_| async move {
                Err::<(), BoxError>(Box::new(error))
            })
            .unwrap();
        let report = first.wait().await;
        assert!(matches!(report.outcome(), WorkOutcome::Failed(_)));
        drop((first, report));
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let (finish, pending) = oneshot::channel();
        let second = scope
            .spawn(WorkSpec::new("evicts-previous"), move |_| async move {
                pending.await?;
                Ok::<(), BoxError>(())
            })
            .unwrap();
        scope.close();
        let wait = scope.wait();
        tokio::pin!(wait);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        finish.send(()).unwrap();
        let report = second.wait().await;
        assert!(matches!(report.outcome(), WorkOutcome::Succeeded));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(scope.is_drained(), "responsibility has actually ended");
        std::future::poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_ready()
    }

    #[tokio::test]
    async fn retired_error_panic_must_not_strand_a_registered_scope_waiter() {
        assert!(
            scope_waiter_wakes_after_retirement(false).await,
            "baseline retirement did not notify"
        );
        assert!(
            scope_waiter_wakes_after_retirement(true).await,
            "recorded error destructor panic stranded the drained scope waiter"
        );
    }

    #[test]
    fn aborted_queued_blocking_work_releases_its_capture_during_runtime_shutdown() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (entered, entry) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let blocker = runtime.spawn_blocking(move || {
            entered.send(()).unwrap();
            let _ = gate.recv();
        });
        entry
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let scope = WorkScope::new(
            ServiceId(Arc::from("owner")),
            1,
            1,
            runtime.handle().clone(),
            Arc::new(Notify::new()),
            Arc::new(AtomicU64::new(0)),
        );
        let called = Arc::new(AtomicBool::new(false));
        let invocation = called.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = RetiredError {
            panic_on_drop: false,
            drops: drops.clone(),
        };
        let work = scope
            .spawn_blocking(WorkSpec::new("queued"), move |_| {
                invocation.store(true, Ordering::SeqCst);
                drop(capture);
                Ok::<(), BoxError>(())
            })
            .unwrap();
        scope.close();
        scope.abort();
        // Shutdown is established before the occupied blocking worker is released.
        runtime.shutdown_timeout(std::time::Duration::ZERO);
        drop(release);
        let observer = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        observer.block_on(async {
            blocker.await.unwrap();
            let report = tokio::time::timeout(std::time::Duration::from_secs(1), work.wait())
                .await
                .unwrap();
            assert!(matches!(
                report.outcome(),
                WorkOutcome::CancelledBeforeStart
            ));
            assert_eq!(report.completion(), WorkCompletion::Confirmed);
            scope.wait().await;
        });
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
