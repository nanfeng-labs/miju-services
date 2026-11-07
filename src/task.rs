//! Release user captures before publishing execution results.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError};
use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{runtime::Handle, task::AbortHandle};
use tokio_util::sync::CancellationToken;

type UserFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;
type Factory = Box<dyn FnOnce() -> UserFuture + Send>;
type Completion = Box<dyn FnOnce(TaskResult) + Send>;

struct MappedFuture<Fut> {
    future: Pin<Box<Fut>>,
}

impl<Fut, E> Future for MappedFuture<Fut>
where
    Fut: Future<Output = Result<(), E>>,
    E: Into<BoxError>,
{
    type Output = Result<(), BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Keep the completed future until Execution releases it separately.
        self.future
            .as_mut()
            .poll(cx)
            .map(|result| result.map_err(Into::into))
    }
}

pub(crate) fn boxed<Fut, E>(future: Fut) -> UserFuture
where
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + 'static,
{
    Box::pin(MappedFuture {
        future: Box::pin(future),
    })
}

#[derive(Clone, Debug)]
pub(crate) enum TaskOutcome {
    Succeeded,
    Failed(SharedError),
    Panicked(Arc<str>),
    CancelledBeforeStart,
    Aborted,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskResult {
    pub outcome: TaskOutcome,
    pub release_panic: Option<Arc<str>>,
    pub was_aborted: bool,
}

pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> Arc<str> {
    let message = if let Some(message) = payload.downcast_ref::<String>() {
        Arc::from(message.as_str())
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        Arc::from(*message)
    } else {
        Arc::from("non-string panic payload")
    };
    // A custom panic payload can itself panic on drop.
    if let Err(nested) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(nested);
    }
    message
}

fn release<T>(value: T) -> Option<Arc<str>> {
    catch_unwind(AssertUnwindSafe(|| drop(value)))
        .err()
        .map(panic_message)
}

struct Execution {
    factory: Option<Factory>,
    future: Option<UserFuture>,
    complete: Option<Completion>,
    stop: CancellationToken,
    started: bool,
}

impl Execution {
    fn finish(&mut self, outcome: TaskOutcome, was_aborted: bool) {
        // The callback may unlock a dependency or cleanup; user state goes first.
        let release_panic = release((self.factory.take(), self.future.take()));
        if let Some(complete) = self.complete.take() {
            complete(TaskResult {
                outcome,
                release_panic,
                was_aborted,
            });
        }
    }
}

impl Future for Execution {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let execution = self.get_mut();
        if execution.future.is_none() {
            if execution.stop.is_cancelled() {
                execution.finish(TaskOutcome::CancelledBeforeStart, false);
                return Poll::Ready(());
            }
            execution.started = true;
            let factory = execution
                .factory
                .take()
                .expect("execution is polled until completion");
            match catch_unwind(AssertUnwindSafe(factory)) {
                Ok(future) => execution.future = Some(future),
                Err(payload) => {
                    execution.finish(TaskOutcome::Panicked(panic_message(payload)), false);
                    return Poll::Ready(());
                }
            }
        }
        let future = execution
            .future
            .as_mut()
            .expect("factory supplied the future");
        let outcome = match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(Ok(()))) => TaskOutcome::Succeeded,
            Ok(Poll::Ready(Err(error))) => TaskOutcome::Failed(Arc::from(error)),
            Err(payload) => TaskOutcome::Panicked(panic_message(payload)),
        };
        execution.finish(outcome, false);
        Poll::Ready(())
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        if self.complete.is_some() {
            let outcome = if self.started {
                TaskOutcome::Aborted
            } else {
                TaskOutcome::CancelledBeforeStart
            };
            self.finish(outcome, self.started);
        }
    }
}

pub(crate) fn spawn<F, Fut, E, C>(
    runtime: &Handle,
    stop: CancellationToken,
    run: F,
    complete: C,
) -> AbortHandle
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
    C: FnOnce(TaskResult) + Send + 'static,
{
    let execution = Execution {
        factory: Some(Box::new(move || {
            let future = run();
            boxed(future)
        })),
        future: None,
        complete: Some(Box::new(complete)),
        stop,
        started: false,
    };
    runtime.spawn(execution).abort_handle()
}

pub(crate) fn spawn_blocking<F, E, C>(
    runtime: &Handle,
    stop: CancellationToken,
    run: F,
    complete: C,
) -> AbortHandle
where
    F: FnOnce() -> Result<(), E> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
    C: FnOnce(TaskResult) + Send + 'static,
{
    struct BlockingExecution {
        run: Option<Box<dyn FnOnce() -> Result<(), BoxError> + Send>>,
        complete: Option<Completion>,
        stop: CancellationToken,
    }
    impl BlockingExecution {
        fn finish(&mut self, outcome: TaskOutcome) {
            let release_panic = release(self.run.take());
            if let Some(complete) = self.complete.take() {
                complete(TaskResult {
                    outcome,
                    release_panic,
                    was_aborted: false,
                });
            }
        }

        fn run(mut self) {
            if self.stop.is_cancelled() {
                self.finish(TaskOutcome::CancelledBeforeStart);
                return;
            }
            let run = self.run.take().expect("blocking entry runs once");
            let outcome = match catch_unwind(AssertUnwindSafe(run)) {
                Ok(Ok(())) => TaskOutcome::Succeeded,
                Ok(Err(error)) => TaskOutcome::Failed(Arc::from(error)),
                Err(payload) => TaskOutcome::Panicked(panic_message(payload)),
            };
            self.finish(outcome);
        }
    }
    impl Drop for BlockingExecution {
        fn drop(&mut self) {
            if self.complete.is_some() {
                self.finish(TaskOutcome::CancelledBeforeStart);
            }
        }
    }
    // One capture owns both the entry and its certificate, including queued abort.
    let execution = BlockingExecution {
        run: Some(Box::new(move || run().map_err(Into::into))),
        complete: Some(Box::new(complete)),
        stop,
    };
    runtime
        .spawn_blocking(move || execution.run())
        .abort_handle()
}
