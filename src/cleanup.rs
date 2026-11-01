//! Initialization-time transfer of asynchronous cleanup responsibility.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError, util::lock};
use futures_util::future::BoxFuture;
use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
};

/// An application-local cleanup identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CleanupId(pub(crate) u64);

/// A rejected registration, retaining the original cleanup closure.
pub struct CleanupRejected<F> {
    pub(crate) cleanup: F,
}

impl<F> CleanupRejected<F> {
    /// Recover the closure and execute it or transfer responsibility elsewhere.
    pub fn into_cleanup(self) -> F {
        self.cleanup
    }
}

impl<F> fmt::Debug for CleanupRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CleanupRejected")
    }
}

impl<F> fmt::Display for CleanupRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("initialization cleanup registration is closed")
    }
}

impl<F> std::error::Error for CleanupRejected<F> {}

/// Result of one cleanup after its body and captures have been released.
#[derive(Clone, Debug)]
pub struct CleanupReport {
    /// Registration identity.
    pub id: CleanupId,
    /// Application-supplied label.
    pub label: String,
    /// Error or unwind diagnostic; absent on success.
    pub error: Option<SharedError>,
}

pub(crate) type CleanupFn = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BoxError>> + Send>;

pub(crate) struct Entry {
    pub id: CleanupId,
    pub label: String,
    pub run: CleanupFn,
}

#[derive(Default)]
pub(crate) struct Stack {
    entries: Mutex<Vec<Entry>>,
}

impl Stack {
    pub(crate) fn push<F, Fut, E>(&self, label: String, cleanup: F) -> CleanupId
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let run: CleanupFn =
            Box::new(move || Box::pin(async move { cleanup().await.map_err(Into::into) }));
        let mut entries = lock(&self.entries);
        let id = CleanupId(entries.len() as u64);
        entries.push(Entry { id, label, run });
        id
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.entries).len()
    }

    pub(crate) fn pop(&self) -> Option<Entry> {
        lock(&self.entries).pop()
    }
}

impl fmt::Debug for Stack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupStack")
            .field("pending", &self.len())
            .finish()
    }
}

pub(crate) fn diagnostic(message: impl Into<String>) -> SharedError {
    Arc::new(std::io::Error::other(message.into()))
}
