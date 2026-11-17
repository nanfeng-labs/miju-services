//! Deferred cleanup actions.
//!
//! Actions are registered by the code that acquired a resource and run by the
//! driver after every scope has drained: consumers' actions before their
//! providers', and within one rank the most recently registered first. Each
//! runs once, to completion, and a failure does not skip the rest.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError, contain::panic_message, report::CleanupReport, scope::Label};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

pub(crate) type Action = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

pub(crate) struct Deferred {
    rank: usize,
    sequence: u64,
    pub(crate) label: Label,
    pub(crate) service: Arc<str>,
    pub(crate) action: Action,
}

#[derive(Default)]
struct Stack {
    actions: Vec<Deferred>,
    next: u64,
    /// Set when the driver takes the actions; nothing is accepted afterwards.
    closed: bool,
}

#[derive(Default)]
struct Progress {
    current: Option<(Arc<str>, Label)>,
    pending: Vec<(Arc<str>, Label)>,
    done: Vec<CleanupReport>,
}

#[derive(Default)]
pub(crate) struct Cleanup {
    stack: Mutex<Stack>,
    progress: Mutex<Progress>,
}

impl Cleanup {
    /// Register an action unless registration has closed or `rejected` says so.
    /// `make` runs only on acceptance, so a rejected caller keeps its future.
    pub(crate) fn push(
        &self,
        rank: usize,
        label: Label,
        service: Arc<str>,
        rejected: impl FnOnce() -> bool,
        make: impl FnOnce() -> Action,
    ) -> bool {
        let mut stack = self.stack.lock().expect("the stack runs no user code");
        if stack.closed || rejected() {
            return false;
        }
        let sequence = stack.next;
        stack.next += 1;
        stack.actions.push(Deferred {
            rank,
            sequence,
            label,
            service,
            action: make(),
        });
        true
    }

    /// Close registration and return the actions in the order they must run.
    pub(crate) fn take_ordered(&self) -> Vec<Deferred> {
        let mut actions = {
            let mut stack = self.stack.lock().expect("the stack runs no user code");
            stack.closed = true;
            std::mem::take(&mut stack.actions)
        };
        // Consumers before providers, then last registered first.
        actions.sort_by_key(|action| std::cmp::Reverse((action.rank, action.sequence)));
        let mut progress = self.progress.lock().expect("progress runs no user code");
        progress.pending = actions
            .iter()
            .map(|action| (action.service.clone(), action.label.clone()))
            .collect();
        actions
    }

    pub(crate) fn begin(&self) {
        let mut progress = self.progress.lock().expect("progress runs no user code");
        if !progress.pending.is_empty() {
            progress.current = Some(progress.pending.remove(0));
        }
    }

    pub(crate) fn finish(&self, report: CleanupReport) {
        let mut progress = self.progress.lock().expect("progress runs no user code");
        progress.current = None;
        progress.done.push(report);
    }

    /// Finished, running and waiting actions, for a snapshot.
    #[allow(clippy::type_complexity)]
    pub(crate) fn observe(
        &self,
    ) -> (
        Vec<CleanupReport>,
        Option<(Arc<str>, Label)>,
        Vec<(Arc<str>, Label)>,
    ) {
        let registered: Vec<_> = {
            let stack = self.stack.lock().expect("the stack runs no user code");
            let mut actions: Vec<_> = stack
                .actions
                .iter()
                .map(|action| {
                    (
                        action.rank,
                        action.sequence,
                        action.service.clone(),
                        action.label.clone(),
                    )
                })
                .collect();
            actions.sort_by_key(|action| std::cmp::Reverse((action.0, action.1)));
            actions
                .into_iter()
                .map(|(_, _, service, label)| (service, label))
                .collect()
        };
        let progress = self.progress.lock().expect("progress runs no user code");
        let pending = if registered.is_empty() {
            progress.pending.clone()
        } else {
            registered
        };
        (progress.done.clone(), progress.current.clone(), pending)
    }
}

/// Run one action to completion, turning a panic into a result.
pub(crate) async fn run(action: Action) -> (Option<SharedError>, Option<Arc<str>>) {
    match (Contained {
        action: Some(action),
    })
    .await
    {
        Ok(Ok(())) => (None, None),
        Ok(Err(error)) => (Some(Arc::from(error)), None),
        Err(message) => (None, Some(message)),
    }
}

struct Contained {
    action: Option<Action>,
}

impl Future for Contained {
    type Output = Result<Result<(), BoxError>, Arc<str>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(action) = self.action.as_mut() else {
            return Poll::Pending;
        };
        let polled = catch_unwind(AssertUnwindSafe(|| action.as_mut().poll(cx)));
        let output = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => Ok(result),
            Err(payload) => Err(panic_message(payload)),
        };
        // The action's captures are released before its result is published.
        let action = self.action.take();
        match catch_unwind(AssertUnwindSafe(|| drop(action))) {
            Ok(()) => Poll::Ready(output),
            Err(payload) => Poll::Ready(Err(panic_message(payload))),
        }
    }
}
