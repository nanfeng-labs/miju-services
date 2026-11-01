//! Owned background work, bounded admission, and actual-completion observation.
//!
//! A scope belongs to one service. Closing admission does not cancel existing work.
//! Dropping a handle only drops an observer. Aggregate work additionally requires
//! a successful external completion certificate before the scope can drain.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError, service::ServiceId};
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkId(u64);

impl WorkId {
    /// Return the numeric identity within this application instance.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WorkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Admission and failure policy for one background execution.
#[derive(Clone, Debug)]
pub struct WorkSpec {
    label: String,
    aggregate: bool,
    fail_service: bool,
}

impl WorkSpec {
    /// Record failures without stopping the owning service.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            aggregate: false,
            fail_service: false,
        }
    }

    /// Promote an execution error or panic to an owning-service failure.
    #[must_use]
    pub fn fail_service(mut self) -> Self {
        self.fail_service = true;
        self
    }

    /// Track external framework responsibility; only `Ok(())` confirms completion.
    ///
    /// External execution must start inside the admitted closure. Once started,
    /// this work is never forcibly aborted by a scope. Failure retains its identity
    /// and capacity indefinitely because dropping its driver proves no external drain.
    pub fn aggregate(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            aggregate: true,
            fail_service: true,
        }
    }
}

/// A synchronous admission rejection; the submitted closure was not called.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SpawnError {
    /// The service no longer accepts work.
    #[error("work admission is closed")]
    Closed,
    /// All work slots are occupied, including unconfirmed aggregate work.
    #[error("work capacity is exhausted")]
    AtCapacity,
    /// The runtime could not accept the execution.
    #[error("runtime is unavailable")]
    RuntimeUnavailable,
    /// Blocking work cannot provide an aggregate completion certificate.
    #[error("aggregate work requires asynchronous spawn")]
    IncompatibleSpec,
    /// The application has exhausted its work identity space.
    #[error("work identity space is exhausted")]
    IdentityExhausted,
}

/// Result of the execution entry, distinct from external completion.
#[derive(Clone, Debug)]
pub enum WorkOutcome {
    /// The entry returned successfully.
    Succeeded,
    /// The entry returned a business error.
    Failed(SharedError),
    /// The entry unwound; non-string payloads have a descriptive placeholder.
    Panicked(Arc<str>),
    /// Stop won before the entry was invoked.
    CancelledBeforeStart,
    /// A started ordinary asynchronous entry was forcibly cancelled.
    Aborted,
}

/// Whether all responsibility covered by this work has actually ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkCompletion {
    /// Execution captures were released and any external completion was certified.
    Confirmed,
    /// The aggregate entry failed without a certificate for its external execution.
    Unconfirmed,
}

/// Immutable result, published after the entry and its captures are destroyed.
#[derive(Clone, Debug)]
pub struct WorkReport {
    owner: ServiceId,
    id: WorkId,
    label: Arc<str>,
    outcome: WorkOutcome,
    completion: WorkCompletion,
    stop_requested: bool,
    finished_at: Instant,
    forced_abort: bool,
}

impl WorkReport {
    /// Owning service identity.
    pub fn owner(&self) -> &ServiceId {
        &self.owner
    }

    /// Application-local work identity.
    pub fn id(&self) -> WorkId {
        self.id
    }

    /// Display label, which need not be unique.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Entry result; inspect completion separately before releasing dependencies.
    pub fn outcome(&self) -> &WorkOutcome {
        &self.outcome
    }

    /// Actual completion certificate.
    pub fn completion(&self) -> WorkCompletion {
        self.completion
    }

    /// Whether a stop request had been observed when the result was published.
    pub fn stop_requested(&self) -> bool {
        self.stop_requested
    }

    /// Whether a started asynchronous entry was forcibly dropped, even if its destructor panicked.
    pub fn was_aborted(&self) -> bool {
        self.forced_abort
    }

    /// Monotonic instant at which the business future returned or was cancelled.
    pub fn finished_at(&self) -> Instant {
        self.finished_at
    }
}

/// Read-only stop capability for one execution.
#[derive(Clone, Debug)]
pub struct WorkContext {
    id: WorkId,
    stop: CancellationToken,
}

impl WorkContext {
    /// Application-local work identity.
    pub fn id(&self) -> WorkId {
        self.id
    }

    /// Whether this work has been requested to stop.
    pub fn is_stopping(&self) -> bool {
        self.stop.is_cancelled()
    }

    /// Wait for the work's own stop request without taking cancellation authority.
    pub async fn stopping(&self) {
        self.stop.cancelled().await;
    }
}

/// An observer and cooperative stop handle; dropping it does not cancel work.
#[derive(Clone)]
pub struct WorkHandle {
    id: WorkId,
    stop: CancellationToken,
    result: watch::Receiver<Option<Arc<WorkReport>>>,
}

impl fmt::Debug for WorkHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkHandle")
            .field("id", &self.id)
            .field("stop_requested", &self.stop.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl WorkHandle {
    /// Application-local work identity.
    pub fn id(&self) -> WorkId {
        self.id
    }

    /// Request cooperative stop for this work only; this is idempotent.
    pub fn stop(&self) {
        self.stop.cancel();
    }

    /// Await the immutable entry result. Cancelling this wait does not cancel work.
    ///
    /// An unconfirmed aggregate result does not imply that its scope has drained.
    pub async fn wait(&self) -> Arc<WorkReport> {
        let mut receiver = self.result.clone();
        loop {
            if let Some(report) = receiver.borrow().clone() {
                return report;
            }
            // Every accepted execution owns a completion guard, including unpolled tasks.
            receiver
                .changed()
                .await
                .expect("completion guard publishes before closing");
        }
    }
}

/// A bounded owned snapshot of a service's background-work responsibility.
#[derive(Clone, Debug)]
pub struct ScopeSnapshot {
    /// All identities still occupying capacity, including external completion debt.
    pub active: Vec<WorkId>,
    /// Labels for the active identities; labels are diagnostic, not unique keys.
    pub active_labels: Vec<(WorkId, Arc<str>)>,
    /// Number of entries whose responsibility has actually been confirmed complete.
    pub completed: u64,
    /// Number of failed or panicked entries, counted once even when unconfirmed.
    pub failed: u64,
    /// Number of started ordinary entries forcibly cancelled.
    pub aborted: u64,
    /// Most recent results, bounded by the application's configured retention.
    pub recent: Vec<Arc<WorkReport>>,
    /// Aggregate identities awaiting proof that external execution has ended.
    pub unconfirmed: Vec<WorkId>,
    /// First execution failure promoted by a `fail_service` or aggregate policy.
    pub service_failure: Option<SharedError>,
}

struct Entry {
    label: Arc<str>,
    stop: CancellationToken,
    aggregate: bool,
    started: bool,
    abort_requested: bool,
    abort: Option<AbortHandle>,
    unconfirmed: bool,
}

struct State {
    closed: bool,
    stopping: bool,
    active: BTreeMap<WorkId, Entry>,
    completed: u64,
    failed: u64,
    aborted: u64,
    recent: VecDeque<Arc<WorkReport>>,
    service_failure: Option<SharedError>,
}

struct Inner {
    owner: ServiceId,
    max: usize,
    recent_limit: usize,
    runtime: Handle,
    ids: Arc<AtomicU64>,
    tracker: TaskTracker,
    state: Mutex<State>,
    changed: Notify,
    notify: Arc<Notify>,
}

/// A service's bounded admission and completion domain, excluding the root itself.
#[derive(Clone)]
pub struct WorkScope {
    inner: Arc<Inner>,
}

impl fmt::Debug for WorkScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkScope")
            .field("owner", &self.inner.owner)
            .finish_non_exhaustive()
    }
}

impl WorkScope {
    pub(crate) fn new(
        owner: ServiceId,
        max: usize,
        recent: usize,
        runtime: Handle,
        notify: Arc<Notify>,
        ids: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                owner,
                max,
                recent_limit: recent,
                runtime,
                ids,
                tracker: TaskTracker::new(),
                state: Mutex::new(State {
                    closed: false,
                    stopping: false,
                    active: BTreeMap::new(),
                    completed: 0,
                    failed: 0,
                    aborted: 0,
                    recent: VecDeque::new(),
                    service_failure: None,
                }),
                changed: Notify::new(),
                notify,
            }),
        }
    }

    /// Admit and start asynchronous work on the application's saved runtime.
    ///
    /// Admission is synchronous and races with `close` under the same gate.
    /// Rejection drops captures outside the gate and does not invoke `run`.
    pub fn spawn<F, Fut, E>(&self, spec: WorkSpec, run: F) -> Result<WorkHandle, SpawnError>
    where
        F: FnOnce(WorkContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let (handle, completion) = self.admit(spec)?;
        let context = WorkContext {
            id: handle.id,
            stop: handle.stop.clone(),
        };
        let inner = self.inner.clone();
        let id = handle.id;
        let future = async move {
            if !inner.begin(id) {
                drop(run);
                return WorkOutcome::CancelledBeforeStart;
            }
            match run(context).await {
                Ok(()) => WorkOutcome::Succeeded,
                Err(error) => WorkOutcome::Failed(Arc::from(error.into())),
            }
        };
        let execution = Execution {
            future: Some(Box::pin(future)),
            completion,
        };
        let task = self.inner.runtime.spawn(execution);
        self.inner.install_abort(id, task.abort_handle());
        drop(task);
        self.inner.notify.notify_one();
        Ok(handle)
    }

    /// Admit blocking work, retaining responsibility until the closure really exits.
    ///
    /// An already started blocking closure cannot be killed by `abort`. It must
    /// cooperate with its context or finish naturally. Aggregate specs are rejected.
    pub fn spawn_blocking<F, E>(&self, spec: WorkSpec, run: F) -> Result<WorkHandle, SpawnError>
    where
        F: FnOnce(WorkContext) -> Result<(), E> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        if spec.aggregate {
            return Err(SpawnError::IncompatibleSpec);
        }
        let (handle, completion) = self.admit(spec)?;
        let context = WorkContext {
            id: handle.id,
            stop: handle.stop.clone(),
        };
        let mut body = BlockingBody {
            run: Some(run),
            completion,
        };
        let id = handle.id;
        let task = self.inner.runtime.spawn_blocking(move || {
            if !body.completion.inner.begin(id) {
                return;
            }
            let outcome = match catch_unwind(AssertUnwindSafe(|| {
                match body.run.take().expect("blocking entry is invoked once")(context) {
                    Ok(()) => WorkOutcome::Succeeded,
                    Err(error) => WorkOutcome::Failed(Arc::from(error.into())),
                }
            })) {
                Ok(outcome) => outcome,
                Err(payload) => WorkOutcome::Panicked(panic_message(payload)),
            };
            body.completion.finish(outcome, Instant::now());
        });
        self.inner.install_abort(id, task.abort_handle());
        drop(task);
        self.inner.notify.notify_one();
        Ok(handle)
    }

    fn admit(&self, spec: WorkSpec) -> Result<(WorkHandle, Completion), SpawnError> {
        let label: Arc<str> = spec.label.into();
        let stop = CancellationToken::new();
        let (send, receive) = watch::channel(None);
        let mut state = self.inner.state.lock().expect("work gate poisoned");
        if state.closed {
            return Err(SpawnError::Closed);
        }
        if state.active.len() >= self.inner.max {
            return Err(SpawnError::AtCapacity);
        }
        let id = WorkId(
            self.inner
                .ids
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| SpawnError::IdentityExhausted)?,
        );
        let token = self.inner.tracker.token();
        state.active.insert(
            id,
            Entry {
                label: label.clone(),
                stop: stop.clone(),
                aggregate: spec.aggregate,
                started: false,
                abort_requested: false,
                abort: None,
                unconfirmed: false,
            },
        );
        drop(state);
        let completion = Completion {
            inner: self.inner.clone(),
            id,
            label,
            aggregate: spec.aggregate,
            fail_service: spec.fail_service,
            stop: stop.clone(),
            result: Some(send),
            token: Some(token),
            forced_abort: false,
        };
        Ok((
            WorkHandle {
                id,
                stop,
                result: receive,
            },
            completion,
        ))
    }

    /// Close admission without sending stop to any existing work.
    pub fn close(&self) {
        {
            let mut state = self.inner.state.lock().expect("work gate poisoned");
            state.closed = true;
            self.inner.tracker.close();
        }
        self.inner.changed.notify_waiters();
        self.inner.notify.notify_one();
    }

    /// Wait for closed admission, actual execution release, and aggregate confirmation.
    /// Cancelling this observer does not change the scope's responsibility.
    pub async fn wait(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_drained() {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn stop(&self) {
        let stops = {
            let mut state = self.inner.state.lock().expect("work gate poisoned");
            state.closed = true;
            state.stopping = true;
            self.inner.tracker.close();
            state
                .active
                .values()
                .map(|entry| entry.stop.clone())
                .collect::<Vec<_>>()
        };
        for stop in stops {
            stop.cancel();
        }
        self.inner.changed.notify_waiters();
        self.inner.notify.notify_one();
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("work gate poisoned")
            .stopping
    }

    pub(crate) fn abort(&self) {
        let handles = {
            let mut state = self.inner.state.lock().expect("work gate poisoned");
            state
                .active
                .values_mut()
                .filter_map(|entry| {
                    if entry.aggregate && entry.started {
                        return None;
                    }
                    entry.abort_requested = true;
                    entry.abort.clone()
                })
                .collect::<Vec<_>>()
        };
        for handle in handles {
            handle.abort();
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        let state = self.inner.state.lock().expect("work gate poisoned");
        state.closed && state.active.is_empty() && self.inner.tracker.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn has_service_failure(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("work gate poisoned")
            .service_failure
            .is_some()
    }

    pub(crate) fn snapshot(&self) -> ScopeSnapshot {
        let state = self.inner.state.lock().expect("work gate poisoned");
        ScopeSnapshot {
            active: state.active.keys().copied().collect(),
            active_labels: state
                .active
                .iter()
                .map(|(id, entry)| (*id, entry.label.clone()))
                .collect(),
            completed: state.completed,
            failed: state.failed,
            aborted: state.aborted,
            recent: state.recent.iter().cloned().collect(),
            unconfirmed: state
                .active
                .iter()
                .filter_map(|(id, entry)| entry.unconfirmed.then_some(*id))
                .collect(),
            service_failure: state.service_failure.clone(),
        }
    }
}

impl Inner {
    fn begin(&self, id: WorkId) -> bool {
        let mut state = self.state.lock().expect("work gate poisoned");
        let stopping = state.stopping;
        let entry = state
            .active
            .get_mut(&id)
            .expect("accepted work remains registered");
        if stopping || entry.stop.is_cancelled() || entry.abort_requested {
            return false;
        }
        entry.started = true;
        true
    }

    fn install_abort(&self, id: WorkId, handle: AbortHandle) {
        let should_abort = {
            let mut state = self.state.lock().expect("work gate poisoned");
            if let Some(entry) = state.active.get_mut(&id) {
                let abort = entry.abort_requested;
                entry.abort = Some(handle.clone());
                abort
            } else {
                false
            }
        };
        if should_abort {
            handle.abort();
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> Arc<str> {
    if let Some(message) = payload.downcast_ref::<String>() {
        Arc::from(message.as_str())
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        Arc::from(*message)
    } else {
        Arc::from("non-string panic payload")
    }
}

struct Completion {
    inner: Arc<Inner>,
    id: WorkId,
    label: Arc<str>,
    aggregate: bool,
    fail_service: bool,
    stop: CancellationToken,
    result: Option<watch::Sender<Option<Arc<WorkReport>>>>,
    token: Option<TaskTrackerToken>,
    forced_abort: bool,
}

impl Completion {
    fn cancelled_outcome(&self) -> WorkOutcome {
        let state = self.inner.state.lock().expect("work gate poisoned");
        if state
            .active
            .get(&self.id)
            .is_some_and(|entry| entry.started)
        {
            WorkOutcome::Aborted
        } else {
            WorkOutcome::CancelledBeforeStart
        }
    }

    fn finish(&mut self, outcome: WorkOutcome, finished_at: Instant) {
        let Some(result) = self.result.take() else {
            return;
        };
        let failure = match &outcome {
            WorkOutcome::Failed(error) => Some(error.clone()),
            WorkOutcome::Panicked(message) => {
                Some(Arc::new(std::io::Error::other(message.to_string())) as SharedError)
            }
            _ => None,
        };
        let completion = if self.aggregate
            && !matches!(
                outcome,
                WorkOutcome::Succeeded | WorkOutcome::CancelledBeforeStart
            ) {
            WorkCompletion::Unconfirmed
        } else {
            WorkCompletion::Confirmed
        };
        let forced_abort = self.forced_abort || matches!(outcome, WorkOutcome::Aborted);
        let report = Arc::new(WorkReport {
            owner: self.inner.owner.clone(),
            id: self.id,
            label: self.label.clone(),
            outcome,
            completion,
            stop_requested: self.stop.is_cancelled(),
            finished_at,
            forced_abort,
        });
        let (removed, evicted) = {
            let mut state = self.inner.state.lock().expect("work gate poisoned");
            if failure.is_some() {
                state.failed = state.failed.saturating_add(1);
            }
            if report.forced_abort {
                state.aborted = state.aborted.saturating_add(1);
            }
            if self.fail_service && state.service_failure.is_none() {
                state.service_failure = failure;
            }
            let removed = if completion == WorkCompletion::Confirmed {
                state.completed = state.completed.saturating_add(1);
                state.active.remove(&self.id)
            } else {
                let entry = state
                    .active
                    .get_mut(&self.id)
                    .expect("unconfirmed work stays registered");
                entry.unconfirmed = true;
                None
            };
            let evicted = if self.inner.recent_limit > 0 {
                state.recent.push_back(report.clone());
                if state.recent.len() > self.inner.recent_limit {
                    state.recent.pop_front()
                } else {
                    None
                }
            } else {
                None
            };
            (removed, evicted)
        };
        // Last user error references may run reentrant destructors; never drop them under the gate.
        drop(removed);
        drop(evicted);
        let previous = result.send_replace(Some(report));
        drop(previous);
        drop(self.token.take());
        self.inner.changed.notify_waiters();
        self.inner.notify.notify_one();
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.finish(self.cancelled_outcome(), Instant::now());
        }
    }
}

struct Execution<F: Future<Output = WorkOutcome>> {
    future: Option<Pin<Box<F>>>,
    completion: Completion,
}

impl<F: Future<Output = WorkOutcome>> Future for Execution<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let outcome = match catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("execution polled after completion")
                .as_mut()
                .poll(cx)
        })) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(outcome)) => outcome,
            Err(payload) => WorkOutcome::Panicked(panic_message(payload)),
        };
        let finished_at = Instant::now();
        let outcome = match catch_unwind(AssertUnwindSafe(|| drop(this.future.take()))) {
            Ok(()) => outcome,
            Err(payload) => WorkOutcome::Panicked(panic_message(payload)),
        };
        this.completion.finish(outcome, finished_at);
        Poll::Ready(())
    }
}

impl<F: Future<Output = WorkOutcome>> Drop for Execution<F> {
    fn drop(&mut self) {
        if self.completion.result.is_some() {
            self.completion.forced_abort =
                matches!(self.completion.cancelled_outcome(), WorkOutcome::Aborted);
        }
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(self.future.take())));
        if let Err(payload) = dropped {
            self.completion.finish(
                WorkOutcome::Panicked(panic_message(payload)),
                Instant::now(),
            );
        }
        // Completion's field destructor runs only after the user future is gone.
    }
}

struct BlockingBody<F> {
    run: Option<F>,
    completion: Completion,
}

impl<F> Drop for BlockingBody<F> {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(self.run.take()))) {
            self.completion.finish(
                WorkOutcome::Panicked(panic_message(payload)),
                Instant::now(),
            );
        }
    }
}

#[cfg(test)]
mod tests;
