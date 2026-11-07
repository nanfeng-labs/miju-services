//! Named cleanup in one application-wide reverse registration order.
//!
//! Registration transfers responsibility at successful start or during service
//! initialization. An unstarted [`App`](crate::App) cannot run asynchronous cleanup
//! from Drop; bootstrap must retain independent rollback access until start succeeds.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

/// Identity allocated in cleanup registration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CleanupId(pub(crate) u64);

impl CleanupId {
    /// Application-local registration number.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// A rejected transfer that still owns the caller's cleanup closure.
pub struct CleanupRejected<F> {
    label: String,
    close: F,
}

impl<F> CleanupRejected<F> {
    pub(crate) fn new(label: String, close: F) -> Self {
        Self { label, close }
    }

    /// Label of the resource whose responsibility was not accepted.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Recover the original closure for application-owned cleanup.
    pub fn into_inner(self) -> F {
        self.close
    }
}

impl<F> fmt::Debug for CleanupRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupRejected")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl<F> fmt::Display for CleanupRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cleanup registration closed for {}", self.label)
    }
}

/// Result of one actually completed cleanup action.
#[derive(Clone, Debug)]
pub struct CleanupReport {
    /// Registration identity.
    pub id: CleanupId,
    /// Application-supplied resource name.
    pub label: String,
    /// Original close error, if any.
    pub error: Option<SharedError>,
    /// Panic during invocation or polling, if any.
    pub panic: Option<Arc<str>>,
    /// Panic when releasing the close future, independently of its result.
    pub release_panic: Option<Arc<str>>,
}

impl CleanupReport {
    /// Whether close and release both succeeded.
    pub fn is_success(&self) -> bool {
        self.error.is_none() && self.panic.is_none() && self.release_panic.is_none()
    }
}

pub(crate) type Close =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>> + Send>;

pub(crate) struct Action {
    pub(crate) id: CleanupId,
    pub(crate) label: String,
    pub(crate) close: Close,
}

#[derive(Default)]
pub(crate) struct Registry {
    actions: Mutex<Vec<Action>>,
}

impl Registry {
    pub(crate) fn register<F, Fut, E>(
        &self,
        label: String,
        close: F,
    ) -> Result<CleanupId, CleanupRejected<F>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let mut actions = self.actions.lock().unwrap();
        let Some(id) = u64::try_from(actions.len())
            .ok()
            .and_then(|n| n.checked_add(1))
        else {
            drop(actions);
            return Err(CleanupRejected::new(label, close));
        };
        let id = CleanupId(id);
        actions.push(Action {
            id,
            label,
            close: Box::new(move || crate::task::boxed(close())),
        });
        Ok(id)
    }

    pub(crate) fn take(&self) -> Vec<Action> {
        std::mem::take(&mut *self.actions.lock().unwrap())
    }
}
