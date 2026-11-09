//! Read-only lifecycle observations that do not keep an application running.
#![forbid(unsafe_code)]

use crate::report::{AppSnapshot, ExitReport};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

/// The independent driver disappeared without a final completion certificate.
#[derive(Clone, Debug, thiserror::Error)]
#[error("lifecycle driver was lost before actual completion")]
pub struct DriverLost {
    /// Last known state. Pending responsibility must not be treated as drained.
    pub snapshot: Arc<AppSnapshot>,
}

/// Readiness observation failed without cancelling initialization.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadyError {
    /// The observation interval expired; initialization may still finish.
    #[error("readiness observation timed out")]
    Timeout(Arc<AppSnapshot>),
    /// Stop or termination occurred before the application was ready.
    #[error("application stopped before readiness")]
    StoppedBeforeReady(Arc<AppSnapshot>),
    /// Required initialization or execution failed.
    #[error("required service failed before readiness")]
    Failed(Arc<AppSnapshot>),
    /// No driver remains to complete initialization.
    #[error(transparent)]
    DriverLost(#[from] DriverLost),
}

/// Bounded observation of actual shutdown, independent of lifecycle success.
#[derive(Clone, Debug)]
pub enum ShutdownProgress {
    /// All ownership and cleanup really ended; inspect success separately.
    Complete(Arc<ExitReport>),
    /// Responsibility is still held by the driver.
    Pending(Arc<AppSnapshot>),
}

struct State {
    snapshot: Arc<AppSnapshot>,
    report: Option<Arc<ExitReport>>,
    stop_at: Option<Instant>,
}

pub(crate) struct Shared {
    state: Mutex<State>,
    updates: watch::Sender<Arc<AppSnapshot>>,
    pub(crate) wake: Arc<Notify>,
}

impl Shared {
    pub(crate) fn new(snapshot: AppSnapshot, wake: Arc<Notify>) -> Arc<Self> {
        let snapshot = Arc::new(snapshot);
        let (updates, _) = watch::channel(snapshot.clone());
        Arc::new(Self {
            state: Mutex::new(State {
                snapshot,
                report: None,
                stop_at: None,
            }),
            updates,
            wake,
        })
    }

    pub(crate) fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        if state.stop_at.is_some() || state.report.is_some() {
            return;
        }
        state.stop_at = Some(Instant::now());
        let mut snapshot = (*state.snapshot).clone();
        snapshot.ready = false;
        let snapshot = Arc::new(snapshot);
        let old = std::mem::replace(&mut state.snapshot, snapshot.clone());
        let old_watch = self.updates.send_replace(snapshot);
        drop(state);
        drop((old, old_watch));
        self.wake.notify_waiters();
    }

    pub(crate) fn stop_at(&self) -> Option<Instant> {
        self.state.lock().unwrap().stop_at
    }

    // Only registration bookkeeping runs here, never a user factory or destructor.
    pub(crate) fn admit_start<T>(&self, admit: impl FnOnce() -> T) -> Option<T> {
        let state = self.state.lock().unwrap();
        if state.stop_at.is_some() {
            return None;
        }
        let admitted = admit();
        drop(state);
        Some(admitted)
    }

    pub(crate) fn publish(&self, mut snapshot: AppSnapshot) -> Arc<AppSnapshot> {
        let mut state = self.state.lock().unwrap();
        snapshot.ready &= state.stop_at.is_none();
        snapshot.driver_lost |= state.snapshot.driver_lost;
        let snapshot = Arc::new(snapshot);
        let old = std::mem::replace(&mut state.snapshot, snapshot.clone());
        let old_watch = self.updates.send_replace(snapshot.clone());
        drop(state);
        crate::task::retire((old, old_watch));
        snapshot
    }

    pub(crate) fn finish(&self, report: Arc<ExitReport>) {
        let old = self.state.lock().unwrap().report.replace(report);
        drop(old);
        // Subscribers also wake for the report, even when the final snapshot is unchanged.
        self.updates.send_modify(|_| {});
    }

    pub(crate) fn lost(&self) {
        let mut snapshot = (*self.snapshot()).clone();
        snapshot.driver_lost = true;
        snapshot.ready = false;
        self.publish(snapshot);
    }

    fn snapshot(&self) -> Arc<AppSnapshot> {
        self.state.lock().unwrap().snapshot.clone()
    }
}

/// A cloneable observer, with no application control lease.
#[derive(Clone)]
pub struct AppObserver {
    pub(crate) shared: Arc<Shared>,
}

impl fmt::Debug for AppObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppObserver")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl AppObserver {
    /// Read the latest coherent snapshot.
    pub fn snapshot(&self) -> Arc<AppSnapshot> {
        self.shared.snapshot()
    }

    /// Subscribe to coalesced current state; no event history is implied.
    pub fn subscribe(&self) -> SnapshotReceiver {
        SnapshotReceiver {
            receiver: self.shared.updates.subscribe(),
        }
    }

    /// Wait for actual final release; a business failure remains an Ok report.
    pub async fn wait(&self) -> Result<Arc<ExitReport>, DriverLost> {
        let mut receiver = self.shared.updates.subscribe();
        loop {
            {
                let state = self.shared.state.lock().unwrap();
                if let Some(report) = &state.report {
                    return Ok(report.clone());
                }
                if state.snapshot.driver_lost {
                    return Err(DriverLost {
                        snapshot: state.snapshot.clone(),
                    });
                }
            }
            let _ = receiver.changed().await;
        }
    }

    /// Wait for current readiness, including required health, within an observation budget.
    pub async fn ready(&self, within: Duration) -> Result<Arc<AppSnapshot>, ReadyError> {
        let observe = async {
            let mut receiver = self.subscribe();
            loop {
                let snapshot = receiver.current();
                if snapshot.driver_lost {
                    return Err(ReadyError::DriverLost(DriverLost { snapshot }));
                }
                if snapshot.ready {
                    return Ok(snapshot);
                }
                if snapshot.services.iter().any(|s| {
                    s.criticality == crate::service::Criticality::Required && !s.failures.is_empty()
                }) {
                    return Err(ReadyError::Failed(snapshot));
                }
                if matches!(
                    snapshot.phase,
                    crate::report::Phase::Stopping
                        | crate::report::Phase::Cleaning
                        | crate::report::Phase::Terminated
                ) || self.shared.stop_at().is_some()
                {
                    return Err(ReadyError::StoppedBeforeReady(snapshot));
                }
                let _ = receiver.changed().await;
            }
        };
        tokio::time::timeout(within, observe)
            .await
            .unwrap_or_else(|_| Err(ReadyError::Timeout(self.snapshot())))
    }

    /// Observe actual completion for a bounded interval, without requesting stop.
    pub async fn drain(&self, within: Duration) -> Result<ShutdownProgress, DriverLost> {
        match tokio::time::timeout(within, self.wait()).await {
            Ok(result) => result.map(ShutdownProgress::Complete),
            Err(_) => Ok(ShutdownProgress::Pending(self.snapshot())),
        }
    }
}

/// A watch-style current snapshot and change notification.
#[derive(Debug)]
pub struct SnapshotReceiver {
    receiver: watch::Receiver<Arc<AppSnapshot>>,
}

impl SnapshotReceiver {
    /// Read the current snapshot without waiting.
    pub fn current(&self) -> Arc<AppSnapshot> {
        self.receiver.borrow().clone()
    }

    /// Wait for another publication. Intermediate snapshots may be coalesced.
    pub async fn changed(&mut self) -> Option<Arc<AppSnapshot>> {
        self.receiver.changed().await.ok()?;
        Some(self.receiver.borrow_and_update().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cleanup::{CleanupId, CleanupReport},
        report::{Health, Phase},
    };
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct RetiredError(Arc<AtomicUsize>);

    impl fmt::Display for RetiredError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("retired snapshot error")
        }
    }

    impl std::error::Error for RetiredError {}

    impl Drop for RetiredError {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("retired-snapshot-drop");
        }
    }

    fn snapshot() -> AppSnapshot {
        AppSnapshot {
            phase: Phase::Running,
            ready: true,
            health: Health::Healthy,
            services: Vec::new(),
            current_cleanup: None,
            pending_cleanup: Vec::new(),
            cleanup: Vec::new(),
            budget_exceeded: false,
            driver_lost: false,
        }
    }

    #[test]
    fn retiring_a_snapshot_error_does_not_unwind_publication() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut old = snapshot();
        old.cleanup.push(CleanupReport {
            id: CleanupId(1),
            label: "retired".into(),
            error: Some(Arc::new(RetiredError(drops.clone()))),
            panic: None,
            release_panic: None,
        });
        let shared = Shared::new(old, Arc::new(Notify::new()));
        // Cloning the same snapshot during stop must retain its errors.
        shared.stop();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let published = catch_unwind(AssertUnwindSafe(|| shared.publish(snapshot())));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            published.is_ok(),
            "retired snapshot error escaped snapshot publication"
        );
        assert!(!shared.snapshot().driver_lost);
    }
}
