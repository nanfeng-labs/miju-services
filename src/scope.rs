//! Bounded work owned by one service.
//!
//! Every responsibility is a lease on the service's scope: the root, spawned
//! work, blocking work, and a [`Lease`] the application moves into anything
//! the library does not run. A scope is drained when its admission is closed
//! and no lease is held. Dropping a [`Work`] handle or cancelling a wait never
//! releases a lease; only the end of the work does.
#![forbid(unsafe_code)]

pub use crate::{gate::Rejected, registry::Kind};

use crate::{
    BoxError, SharedError,
    contain::{drop_contained, panic_message},
    gate::Gate,
    registry::{Key, Listed, Registry},
    slot,
};
use pin_project_lite::pin_project;
use std::{
    borrow::Cow,
    collections::VecDeque,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{runtime::Handle, sync::Notify, task::AbortHandle};
use tokio_util::sync::CancellationToken;

/// How many recent faults a service keeps for its snapshots.
const RECENT_FAULTS: usize = 64;

/// The name of one responsibility, shown while it is outstanding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Label {
    name: Cow<'static, str>,
    critical: bool,
}

impl Label {
    /// The name given at admission. It need not be unique.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether a failure of this work fails its service.
    pub fn is_critical(&self) -> bool {
        self.critical
    }
}

impl From<&'static str> for Label {
    fn from(name: &'static str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            critical: false,
        }
    }
}

impl From<String> for Label {
    fn from(name: String) -> Self {
        Self {
            name: Cow::Owned(name),
            critical: false,
        }
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Name work whose failure, panic or forced abort fails its service.
pub fn critical(name: impl Into<Label>) -> Label {
    let mut label = name.into();
    label.critical = true;
    label
}

/// Why accepted work produced no value.
#[derive(Clone, Debug, thiserror::Error)]
pub enum WorkError {
    /// The work returned this error.
    #[error("work failed: {0}")]
    Failed(#[source] SharedError),
    /// The work panicked.
    #[error("work panicked: {0}")]
    Panicked(Arc<str>),
    /// A started future was dropped by an abort.
    #[error("work was aborted")]
    Aborted,
    /// Stop arrived before the work started; it never ran.
    #[error("work was cancelled before it started")]
    Cancelled,
}

/// A fault a service recorded, whether or not anyone awaited the work.
#[derive(Clone, Debug)]
pub struct Fault {
    /// The work's label.
    pub label: Label,
    /// What happened to it.
    pub error: WorkError,
}

/// How a service's accepted work has ended so far.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkCounters {
    /// Work that returned `Ok`.
    pub completed: u64,
    /// Work that returned an error.
    pub failed: u64,
    /// Work that panicked.
    pub panicked: u64,
    /// Started work whose future was dropped by an abort.
    pub aborted: u64,
    /// Work that was cancelled before it started.
    pub cancelled: u64,
    /// Destructor panics contained while releasing futures and unclaimed results.
    pub release_panics: u64,
}

#[derive(Default)]
struct Counters {
    completed: AtomicU64,
    failed: AtomicU64,
    panicked: AtomicU64,
    aborted: AtomicU64,
    cancelled: AtomicU64,
    release_panics: AtomicU64,
}

#[derive(Default)]
struct Faults {
    recent: VecDeque<Fault>,
    critical: Option<Fault>,
}

pub(crate) struct ScopeInner {
    gate: Gate,
    registry: Registry,
    stop: CancellationToken,
    runtime: Handle,
    /// The lifecycle driver's private wake.
    wake: Arc<Notify>,
    counters: Counters,
    faults: Mutex<Faults>,
    forced: AtomicBool,
}

/// One service's admission gate and release barrier. Clones share it.
#[derive(Clone)]
pub struct Scope(pub(crate) Arc<ScopeInner>);

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scope")
            .field("outstanding", &self.count())
            .field("closed", &self.0.gate.is_closed())
            .finish()
    }
}

/// Holds one count. Dropping it removes the registry entry, then releases the
/// count, so a drained gate implies an empty registry.
struct LeaseGuard {
    scope: Arc<ScopeInner>,
    key: Key,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.scope.registry.remove(self.key);
        if self.scope.gate.release() {
            self.scope.wake.notify_one();
        }
    }
}

/// Responsibility held by something the library does not run: a response
/// body, an upgraded socket, a callback, an external executor.
///
/// Dropping the lease releases it. It cannot be aborted: while it is held the
/// service is not drained and cleanup does not start.
pub struct Lease(LeaseGuard);

impl fmt::Debug for Lease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease").finish_non_exhaustive()
    }
}

impl Lease {
    /// State that this lease's holder cannot prove it has finished.
    ///
    /// The lease is then never released. The service stays undrained, cleanup
    /// does not start, and the reason is shown with the outstanding item.
    pub fn mark_unconfirmed(self, reason: impl Into<String>) {
        self.0
            .scope
            .registry
            .mark_unconfirmed(self.0.key, Arc::from(reason.into()));
        std::mem::forget(self);
    }
}

impl Scope {
    pub(crate) fn new(capacity: usize, runtime: Handle, wake: Arc<Notify>) -> Self {
        Self(Arc::new(ScopeInner {
            // One more than the work capacity: the root holds a count too.
            // Saturating: `NonZeroUsize::MAX` must not wrap to zero.
            gate: Gate::new((capacity as u64).saturating_add(1)),
            registry: Registry::default(),
            stop: CancellationToken::new(),
            runtime,
            wake,
            counters: Counters::default(),
            faults: Mutex::new(Faults::default()),
            forced: AtomicBool::new(false),
        }))
    }

    fn admit(
        &self,
        label: Label,
        kind: Kind,
        started: Option<Arc<AtomicBool>>,
    ) -> Result<LeaseGuard, Rejected> {
        self.0.gate.admit()?;
        // Named before anything can run under it. The guard now owns the count.
        let key = self.0.registry.reserve(label, kind, started);
        Ok(LeaseGuard {
            scope: self.0.clone(),
            key,
        })
    }

    /// Accept asynchronous work, or reject it without polling it.
    ///
    /// Accepted work stays owned by the service when the handle is dropped.
    /// It is cancelled if stop arrives before its first poll, and can be
    /// aborted once the service's grace period has expired.
    pub fn spawn<F, T, E>(&self, name: impl Into<Label>, work: F) -> Result<Work<T>, Rejected>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        self.spawn_with(name.into(), work, true, || {})
            .map_err(|(rejected, _work)| rejected)
    }

    /// Like [`Scope::spawn`], but the work runs even if stop arrives before
    /// its first poll, and a rejected future is handed back. For work an
    /// outside scheduler has already handed over and considers in flight,
    /// such as a job miju dispatched to `Seat::executor`. It can still be
    /// aborted after the grace period.
    #[cfg(feature = "miju")]
    pub(crate) fn spawn_dispatched<F, T, E>(
        &self,
        name: impl Into<Label>,
        work: F,
    ) -> Result<Work<T>, (Rejected, F)>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        self.spawn_with(name.into(), work, false, || {})
    }

    /// The four admission steps, with a seam between spawn and install that
    /// the abort-handshake test uses to hold the submitter there.
    fn spawn_with<F, T, E>(
        &self,
        label: Label,
        work: F,
        cancel_unstarted: bool,
        between_spawn_and_install: impl FnOnce(),
    ) -> Result<Work<T>, (Rejected, F)>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        // 1 and 2: admit, then name the work before anything can run under it.
        // A refused future goes back to the caller unpolled.
        let lease = match self.admit(label.clone(), Kind::Task, None) {
            Ok(lease) => lease,
            Err(rejected) => return Err((rejected, work)),
        };
        let key = lease.key;
        let (sender, receiver) = slot::slot();
        // 3: spawn. On a multi-thread runtime the task may start right away.
        let abort = self
            .0
            .runtime
            .spawn(Task {
                future: Some(work),
                done: Some(WorkDone {
                    sender,
                    lease,
                    label,
                }),
                stop: cancel_unstarted.then(|| self.0.stop.clone()),
                polled: false,
            })
            .abort_handle();
        between_spawn_and_install();
        // 4: install; aborts at once if the abort pass already ran.
        self.0.registry.install(key, abort.clone());
        Ok(Work { receiver, abort })
    }

    /// Accept blocking work, or reject it without calling it.
    ///
    /// It is cancelled if stop arrives before it starts. Once started it
    /// cannot be interrupted: the service is not drained until it returns.
    pub fn spawn_blocking<F, T, E>(
        &self,
        name: impl Into<Label>,
        work: F,
    ) -> Result<Work<T>, Rejected>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let label = name.into();
        let started = Arc::new(AtomicBool::new(false));
        let lease = self.admit(label.clone(), Kind::Blocking, Some(started.clone()))?;
        let key = lease.key;
        let (sender, receiver) = slot::slot();
        let blocking = Blocking {
            run: Some(work),
            done: Some(WorkDone {
                sender,
                lease,
                label,
            }),
            stop: self.0.stop.clone(),
            started,
        };
        let abort = self
            .0
            .runtime
            .spawn_blocking(move || blocking.run())
            .abort_handle();
        self.0.registry.install(key, abort.clone());
        Ok(Work { receiver, abort })
    }

    /// Take a lease for something the library does not run.
    pub fn lease(&self, name: impl Into<Label>) -> Result<Lease, Rejected> {
        self.admit(name.into(), Kind::Lease, None).map(Lease)
    }

    /// Run the service root under the scope's first count.
    pub(crate) fn spawn_root<F>(&self, root: F, finish: RootFinish)
    where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let lease = self
            .admit(Label::from("root"), Kind::Root, None)
            .expect("a fresh scope admits its root");
        let key = lease.key;
        let abort = self
            .0
            .runtime
            .spawn(Task {
                future: Some(root),
                done: Some(RootDone { finish, lease }),
                stop: None,
                polled: false,
            })
            .abort_handle();
        self.0.registry.install(key, abort);
    }

    /// Local stop: close admission and signal `stopping`.
    pub(crate) fn stop(&self) {
        self.0.stop.cancel();
        self.close();
    }

    /// Close admission without asking accepted work to stop.
    pub(crate) fn close(&self) {
        if self.0.gate.close() {
            self.0.wake.notify_one();
        }
    }

    /// The abort pass after the grace period.
    pub(crate) fn abort(&self) {
        self.0.forced.store(true, Ordering::Release);
        self.0.registry.abort_all();
    }

    pub(crate) fn stop_token(&self) -> &CancellationToken {
        &self.0.stop
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.0.gate.is_drained()
    }

    pub(crate) fn was_forced(&self) -> bool {
        self.0.forced.load(Ordering::Acquire)
    }

    /// The authoritative number of held counts, the root included.
    pub(crate) fn count(&self) -> u64 {
        self.0.gate.count()
    }

    pub(crate) fn list(&self, limit: usize) -> Vec<Listed> {
        self.0.registry.list(limit)
    }

    pub(crate) fn counters(&self) -> WorkCounters {
        let counters = &self.0.counters;
        WorkCounters {
            completed: counters.completed.load(Ordering::Relaxed),
            failed: counters.failed.load(Ordering::Relaxed),
            panicked: counters.panicked.load(Ordering::Relaxed),
            aborted: counters.aborted.load(Ordering::Relaxed),
            cancelled: counters.cancelled.load(Ordering::Relaxed),
            release_panics: counters.release_panics.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn recent_faults(&self) -> Vec<Fault> {
        let faults = self.0.faults.lock().expect("faults run no user code");
        faults.recent.iter().cloned().collect()
    }

    /// The first failure of critical work, if any.
    pub(crate) fn critical_fault(&self) -> Option<Fault> {
        let faults = self.0.faults.lock().expect("faults run no user code");
        faults.critical.clone()
    }
}

impl ScopeInner {
    /// Count one ending and remember its fault. Wakes the driver only for a
    /// critical fault: an ordinary completion is not a lifecycle event.
    fn record(&self, label: &Label, error: Option<&WorkError>, release_panic: bool) {
        let counters = &self.counters;
        let counter = match error {
            None => &counters.completed,
            Some(WorkError::Failed(_)) => &counters.failed,
            Some(WorkError::Panicked(_)) => &counters.panicked,
            Some(WorkError::Aborted) => &counters.aborted,
            Some(WorkError::Cancelled) => &counters.cancelled,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if release_panic {
            counters.release_panics.fetch_add(1, Ordering::Relaxed);
        }
        let Some(error) = error else {
            return;
        };
        let critical = label.is_critical() && !matches!(error, WorkError::Cancelled);
        let fault = Fault {
            label: label.clone(),
            error: error.clone(),
        };
        let evicted = {
            let mut faults = self.faults.lock().expect("faults run no user code");
            if critical && faults.critical.is_none() {
                faults.critical = Some(fault.clone());
            }
            faults.recent.push_back(fault);
            if faults.recent.len() > RECENT_FAULTS {
                faults.recent.pop_front()
            } else {
                None
            }
        };
        // An evicted error is user data: dropped outside the lock, contained.
        if drop_contained(evicted) {
            counters.release_panics.fetch_add(1, Ordering::Relaxed);
        }
        if critical {
            self.wake.notify_one();
        }
    }
}

/// Why a task produced no output of its own.
pub(crate) enum Lost {
    Cancelled,
    Aborted,
    Panicked(Arc<str>),
}

/// What runs once a task's future and captures have been dropped. It publishes
/// the outcome and then releases the task's lease, in that order.
trait Done<O>: Send {
    fn finish(self, outcome: Result<O, Lost>, release_panic: bool);
}

struct WorkDone<T> {
    sender: slot::Sender<Result<T, WorkError>>,
    lease: LeaseGuard,
    label: Label,
}

impl<T> WorkDone<T> {
    /// Record the ending, hand the result over, then release the lease.
    fn publish(self, result: Result<T, WorkError>, release_panic: bool) {
        let scope = &self.lease.scope;
        scope.record(&self.label, result.as_ref().err(), release_panic);
        // Offer the result. One nobody holds is dropped before the release.
        let unclaimed = self.sender.complete(result);
        if drop_contained(unclaimed) {
            scope
                .counters
                .release_panics
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(self.lease);
    }
}

fn work_result<T, E: Into<BoxError>>(outcome: Result<Result<T, E>, Lost>) -> Result<T, WorkError> {
    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(WorkError::Failed(Arc::from(error.into()))),
        Err(Lost::Panicked(message)) => Err(WorkError::Panicked(message)),
        Err(Lost::Aborted) => Err(WorkError::Aborted),
        Err(Lost::Cancelled) => Err(WorkError::Cancelled),
    }
}

impl<T, E> Done<Result<T, E>> for WorkDone<T>
where
    T: Send,
    E: Into<BoxError>,
{
    fn finish(self, outcome: Result<Result<T, E>, Lost>, release_panic: bool) {
        self.publish(work_result(outcome), release_panic);
    }
}

/// How a service root ended, reported after its future was dropped.
pub(crate) struct RootEnd {
    pub(crate) outcome: Result<Result<(), BoxError>, RootLost>,
    pub(crate) release_panic: bool,
    /// Whether stop had been requested when the root ended.
    pub(crate) stopping: bool,
}

/// Why a root produced no result of its own.
pub(crate) enum RootLost {
    Aborted,
    Panicked(Arc<str>),
}

pub(crate) type RootFinish = Box<dyn FnOnce(RootEnd) + Send>;

struct RootDone {
    finish: RootFinish,
    lease: LeaseGuard,
}

impl Done<Result<(), BoxError>> for RootDone {
    fn finish(self, outcome: Result<Result<(), BoxError>, Lost>, release_panic: bool) {
        let scope = self.lease.scope.clone();
        let outcome = outcome.map_err(|lost| match lost {
            Lost::Panicked(message) => RootLost::Panicked(message),
            Lost::Aborted | Lost::Cancelled => RootLost::Aborted,
        });
        (self.finish)(RootEnd {
            outcome,
            release_panic,
            stopping: scope.stop.is_cancelled(),
        });
        // The root closes its gate before it releases its own count.
        scope.gate.close();
        drop(self.lease);
        scope.wake.notify_one();
    }
}

pin_project! {
    /// The task Tokio runs. Generic over the future: nothing is boxed here.
    struct Task<F, D>
    where
        F: Future,
        D: Done<F::Output>,
    {
        #[pin]
        future: Option<F>,
        done: Option<D>,
        stop: Option<CancellationToken>,
        polled: bool,
    }

    impl<F, D> PinnedDrop for Task<F, D>
    where
        F: Future,
        D: Done<F::Output>,
    {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            // Reached only when Tokio drops an unfinished task: abort or runtime loss.
            if let Some(done) = this.done.take() {
                let lost = if *this.polled { Lost::Aborted } else { Lost::Cancelled };
                release(this.future, done, Err(lost));
            }
        }
    }
}

/// Ordered release: the future and its captures first, then the outcome, then
/// the lease.
fn release<F, D>(mut future: Pin<&mut Option<F>>, done: D, outcome: Result<F::Output, Lost>)
where
    F: Future,
    D: Done<F::Output>,
{
    let release_panic = catch_unwind(AssertUnwindSafe(|| future.set(None))).is_err();
    done.finish(outcome, release_panic);
}

impl<F, D> Future for Task<F, D>
where
    F: Future,
    D: Done<F::Output>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        if this.done.is_none() {
            return Poll::Ready(());
        }
        if !*this.polled {
            // Admitted but not yet started when stop arrived: never run it.
            if this
                .stop
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                let done = this.done.take().expect("checked above");
                release(this.future, done, Err(Lost::Cancelled));
                return Poll::Ready(());
            }
            *this.polled = true;
        }
        let Some(future) = this.future.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };
        let outcome = match catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(Lost::Panicked(panic_message(payload))),
        };
        let done = this.done.take().expect("checked above");
        release(this.future, done, outcome);
        Poll::Ready(())
    }
}

/// A blocking closure with its completion. One value owns both, so a queued
/// entry that Tokio drops still publishes its outcome.
struct Blocking<F, T> {
    run: Option<F>,
    done: Option<WorkDone<T>>,
    stop: CancellationToken,
    started: Arc<AtomicBool>,
}

impl<F, T> Blocking<F, T> {
    fn run<E>(mut self)
    where
        F: FnOnce() -> Result<T, E>,
        E: Into<BoxError>,
    {
        let Some(done) = self.done.take() else {
            return;
        };
        let run = self.run.take();
        // Queued when stop arrived, even if the pool starts it during runtime shutdown.
        if self.stop.is_cancelled() {
            let release_panic = drop_contained(run);
            done.publish(Err(WorkError::Cancelled), release_panic);
            return;
        }
        let Some(run) = run else {
            return;
        };
        self.started.store(true, Ordering::Release);
        let outcome = catch_unwind(AssertUnwindSafe(run))
            .map_err(|payload| Lost::Panicked(panic_message(payload)));
        done.publish(work_result(outcome), false);
    }
}

impl<F, T> Drop for Blocking<F, T> {
    fn drop(&mut self) {
        // Reached with `done` still present only when the pool dropped a queued entry.
        if let Some(done) = self.done.take() {
            let release_panic = drop_contained(self.run.take());
            done.publish(Err(WorkError::Cancelled), release_panic);
        }
    }
}

/// Accepted work, as its submitter sees it.
///
/// Awaiting it yields the work's value or the reason there is none. Dropping
/// it, or cancelling the wait, leaves the work running and owned by its
/// service. An unread result belongs to this handle and is dropped with it.
pub struct Work<T> {
    receiver: slot::Receiver<Result<T, WorkError>>,
    abort: AbortHandle,
}

impl<T> Work<T> {
    /// Ask this one work item to stop being polled. Blocking work that has
    /// started is not affected.
    pub fn abort(&self) {
        self.abort.abort();
    }
}

impl<T> fmt::Debug for Work<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Work").finish_non_exhaustive()
    }
}

impl<T> Future for Work<T> {
    type Output = Result<T, WorkError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver).poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    // I13 at product level: the task is already running when its submitter is
    // held between spawn and install, and stop and the abort pass happen in
    // that window. The handshake must still abort it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_running_before_its_handle_is_installed_is_still_aborted() {
        let scope = Scope::new(8, Handle::current(), Arc::new(Notify::new()));
        let (reached, at_barrier) = mpsc::channel();
        let (resume, resumed) = mpsc::channel::<()>();
        let started = Arc::new(AtomicBool::new(false));

        let submitter = {
            let (scope, started) = (scope.clone(), started.clone());
            std::thread::spawn(move || {
                scope.spawn_with(
                    Label::from("held"),
                    async move {
                        started.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), BoxError>>().await
                    },
                    true,
                    move || {
                        reached.send(()).unwrap();
                        let _ = resumed.recv();
                    },
                )
            })
        };
        at_barrier.recv().unwrap();
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        scope.stop();
        scope.abort();
        assert!(
            scope.list(8).iter().any(|item| item.label.name() == "held"),
            "admitted work is listed before its handle is installed"
        );
        resume.send(()).unwrap();
        let Ok(work) = submitter.join().unwrap() else {
            panic!("a fresh scope admits the work")
        };

        let outcome = tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .expect("a running task was missed by the abort pass");
        assert!(matches!(outcome, Err(WorkError::Aborted)));
        assert!(scope.is_drained());
    }
}
