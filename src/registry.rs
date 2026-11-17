//! Outstanding responsibility by name, and the abort handshake.
//!
//! Admitting asynchronous work takes four steps: admit on the gate, `reserve`
//! here before the task is spawned, spawn, then `install` the abort handle.
//! The abort pass and `install` take the same lock, so whichever runs second
//! sees the other's effect and no running task is missed.
//!
//! No user code and no user destructor runs while the lock is held.
#![forbid(unsafe_code)]

use crate::{
    scope::Label,
    sync::{AbortHandle, Arc, AtomicBool, Mutex, MutexGuard, Ordering},
};

/// What kind of holder an outstanding responsibility is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// The service's own entry point.
    Root,
    /// Asynchronous work; it can be aborted after the grace period.
    Task,
    /// Blocking work; it can be cancelled only before it starts.
    Blocking,
    /// A lease held by something the library does not run.
    Lease,
    /// A cleanup action that is running or waiting to run.
    Cleanup,
}

/// Identifies one reservation. A slot index is reused, its sequence is not.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key {
    index: usize,
    sequence: u64,
}

struct Entry {
    sequence: u64,
    label: Label,
    kind: Kind,
    started: Option<Arc<AtomicBool>>,
    unconfirmed: Option<Arc<str>>,
    handle: Option<AbortHandle>,
}

/// One listed responsibility.
#[derive(Clone, Debug)]
pub(crate) struct Listed {
    pub(crate) label: Label,
    pub(crate) kind: Kind,
    pub(crate) started: bool,
    pub(crate) unconfirmed: Option<Arc<str>>,
}

#[derive(Default)]
struct Inner {
    slots: Vec<Option<Entry>>,
    free: Vec<usize>,
    next_sequence: u64,
    aborting: bool,
}

#[derive(Default)]
pub(crate) struct Registry {
    inner: Mutex<Inner>,
}

impl Registry {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("the registry runs no user code")
    }

    /// Name the responsibility before anything can run under it.
    pub(crate) fn reserve(
        &self,
        label: Label,
        kind: Kind,
        started: Option<Arc<AtomicBool>>,
    ) -> Key {
        let mut inner = self.lock();
        let sequence = inner.next_sequence;
        inner.next_sequence += 1;
        let entry = Some(Entry {
            sequence,
            label,
            kind,
            started,
            unconfirmed: None,
            handle: None,
        });
        let index = match inner.free.pop() {
            Some(index) => {
                inner.slots[index] = entry;
                index
            }
            None => {
                inner.slots.push(entry);
                inner.slots.len() - 1
            }
        };
        Key { index, sequence }
    }

    /// Make the task abortable. If the abort pass already ran, abort now. If
    /// the task already finished, its entry is gone and the handle is dropped.
    pub(crate) fn install(&self, key: Key, handle: AbortHandle) {
        let abort_now = {
            let mut inner = self.lock();
            let aborting = inner.aborting;
            match inner.slots.get_mut(key.index).and_then(Option::as_mut) {
                Some(entry) if entry.sequence == key.sequence => {
                    entry.handle = Some(handle.clone());
                    aborting
                }
                // Finished first, possibly with its slot already reused.
                _ => false,
            }
        };
        if abort_now {
            handle.abort();
        }
    }

    /// The responsibility has ended.
    pub(crate) fn remove(&self, key: Key) {
        let removed = {
            let mut guard = self.lock();
            let inner = &mut *guard;
            let live = matches!(
                inner.slots.get(key.index).and_then(Option::as_ref),
                Some(entry) if entry.sequence == key.sequence
            );
            if live {
                inner.free.push(key.index);
                inner.slots[key.index].take()
            } else {
                None
            }
        };
        drop(removed);
    }

    /// Record that this lease's holder cannot prove release. The entry stays.
    pub(crate) fn mark_unconfirmed(&self, key: Key, reason: Arc<str>) {
        let mut inner = self.lock();
        if let Some(entry) = inner.slots.get_mut(key.index).and_then(Option::as_mut)
            && entry.sequence == key.sequence
        {
            entry.unconfirmed = Some(reason);
        }
    }

    /// The abort pass after the grace period. Later installs abort themselves.
    pub(crate) fn abort_all(&self) {
        let handles: Vec<AbortHandle> = {
            let mut inner = self.lock();
            inner.aborting = true;
            inner
                .slots
                .iter()
                .flatten()
                .filter_map(|entry| entry.handle.clone())
                .collect()
        };
        for handle in handles {
            handle.abort();
        }
    }

    /// At most `limit` of the responsibilities still registered.
    pub(crate) fn list(&self, limit: usize) -> Vec<Listed> {
        let inner = self.lock();
        inner
            .slots
            .iter()
            .flatten()
            .take(limit)
            .map(|entry| Listed {
                label: entry.label.clone(),
                kind: entry.kind,
                started: entry
                    .started
                    .as_ref()
                    .is_none_or(|started| started.load(Ordering::Acquire)),
                unconfirmed: entry.unconfirmed.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_late_install_cannot_attach_to_a_reused_slot() {
        let registry = Registry::default();
        let first = registry.reserve(Label::from("first"), Kind::Task, None);
        registry.remove(first);
        let second = registry.reserve(Label::from("second"), Kind::Task, None);
        assert_eq!(first.index, second.index, "the slot is reused");

        let stale = tokio::spawn(std::future::pending::<()>());
        registry.install(first, stale.abort_handle());
        registry.abort_all();
        tokio::task::yield_now().await;
        assert!(
            !stale.is_finished(),
            "a stale handle was attached to another entry"
        );
        assert_eq!(registry.list(8).len(), 1);
        stale.abort();
    }

    #[tokio::test]
    async fn abort_reaches_handles_installed_before_and_after_the_pass() {
        let registry = Registry::default();
        let early = tokio::spawn(std::future::pending::<()>());
        let late = tokio::spawn(std::future::pending::<()>());
        let first = registry.reserve(Label::from("early"), Kind::Task, None);
        let second = registry.reserve(Label::from("late"), Kind::Task, None);
        registry.install(first, early.abort_handle());
        registry.abort_all();
        registry.install(second, late.abort_handle());
        // Bounded, so a missed abort fails the test instead of hanging it.
        let within = std::time::Duration::from_secs(5);
        let early = tokio::time::timeout(within, early)
            .await
            .expect("the early handle was not aborted");
        let late = tokio::time::timeout(within, late)
            .await
            .expect("a handle installed after the abort pass was not aborted");
        assert!(early.unwrap_err().is_cancelled());
        assert!(late.unwrap_err().is_cancelled());
    }
}
