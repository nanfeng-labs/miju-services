//! Application-private bounded in-memory submission and final execution receipts.

use futures_util::{Sink, SinkExt, StreamExt};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorage, MemoryStorageError},
    task::Task,
};
use miju_services::{BoxError, ServiceContext};
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tower::{Layer, Service};

#[derive(Clone, Debug, PartialEq, Eq)]
/// Final outcome after the complete configured execution/retry chain.
pub(crate) enum Receipt {
    /// Business execution succeeded.
    Success(u32),
    /// Retry policy exhausted with a business failure.
    Failed(String),
    /// Queued or executing work was released before a final business result.
    Cancelled,
}

#[derive(Debug)]
/// Admission outcome; accepted is not an execution-completion certificate.
pub(crate) enum Submission {
    /// Accepted; await this receiver for the final result.
    Accepted(oneshot::Receiver<Receipt>),
    /// This target already has a pending, queued, or executing submission.
    Skipped,
    /// Stop won while awaiting the shared capacity.
    Stopped,
}

#[derive(Debug, thiserror::Error)]
/// Failure to acquire capacity or submit to the selected memory backend.
pub(crate) enum SubmitError {
    /// Capacity is permanently closed.
    #[error("queue capacity is closed")]
    Closed,
    /// The backend did not accept the task.
    #[error("backend rejected submission: {0}")]
    Push(#[source] MemoryStorageError),
}

struct Target {
    name: String,
    active: Arc<Mutex<HashSet<String>>>,
}

impl Drop for Target {
    fn drop(&mut self) {
        self.active.lock().unwrap().remove(&self.name);
    }
}

struct Ticket {
    sender: Mutex<Option<oneshot::Sender<Receipt>>>,
    _permit: OwnedSemaphorePermit,
    _target: Target,
}

impl Ticket {
    fn finish(&self, value: Receipt) {
        let sender = self.sender.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(value);
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.finish(Receipt::Cancelled);
    }
}

#[derive(Clone)]
/// Private application envelope; its ticket cannot be cloned independently.
pub(crate) struct Job {
    /// Application payload.
    pub(crate) value: u32,
    ticket: Arc<Ticket>,
}

#[derive(Clone)]
/// Only supported producer entrance; holds no exposed raw sender.
pub(crate) struct Queue {
    sender: MemorySink<Job>,
    budget: Arc<Semaphore>,
    targets: Arc<Mutex<HashSet<String>>>,
}

impl Queue {
    /// Split an in-memory backend from its bounded private producer.
    pub(crate) fn new(capacity: usize) -> (Self, MemoryStorage<Job>) {
        assert!(capacity > 0, "example queue needs positive capacity");
        let (tx, rx) = futures_channel::mpsc::unbounded::<Task<Job>>();
        let sink: Box<dyn Sink<Task<Job>, Error = MemoryStorageError> + Send + Sync + Unpin> =
            Box::new(tx.sink_map_err(MemoryStorageError::from));
        let sender = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(sink)));
        let backend = MemoryStorage::new_with(sender.clone(), rx.boxed());
        (
            Self {
                sender,
                budget: Arc::new(Semaphore::new(capacity)),
                targets: Arc::new(Mutex::new(HashSet::new())),
            },
            backend,
        )
    }

    /// Capacity not held by queued, executing, or retrying tickets.
    pub(crate) fn available(&self) -> usize {
        self.budget.available_permits()
    }

    /// Reserve the target while waiting for capacity, then submit one ticketed task.
    pub(crate) async fn submit<F: Future>(
        &self,
        target: impl Into<String>,
        value: u32,
        stop: F,
    ) -> Result<Submission, SubmitError> {
        let target = target.into();
        if !self.targets.lock().unwrap().insert(target.clone()) {
            return Ok(Submission::Skipped);
        }
        let target = Target {
            name: target,
            active: self.targets.clone(),
        };
        tokio::pin!(stop);
        let permit = tokio::select! {
            biased;
            _ = &mut stop => return Ok(Submission::Stopped),
            permit = self.budget.clone().acquire_owned() => permit.map_err(|_| SubmitError::Closed)?
        };
        let (sender, receipt) = oneshot::channel();
        let job = Job {
            value,
            ticket: Arc::new(Ticket {
                sender: Mutex::new(Some(sender)),
                _permit: permit,
                _target: target,
            }),
        };
        // Only this entry owns a sender; successful send is acceptance, not completion.
        self.sender
            .clone()
            .send(Task::new(job))
            .await
            .map_err(SubmitError::Push)?;
        Ok(Submission::Accepted(receipt))
    }
}

#[derive(Clone)]
/// Outermost user layer; wrap all retry layers inside this receipt boundary.
pub(crate) struct ReceiptLayer;

#[derive(Clone)]
/// Receipt wrapper for the configured execution service.
pub(crate) struct ReceiptService<S>(S);

/// Drops the entire execution future before publishing its final receipt.
type ReceiptResultFuture = Pin<Box<dyn Future<Output = Result<u32, BoxError>> + Send>>;

pub(crate) struct ReceiptFuture {
    inner: Option<ReceiptResultFuture>,
    ticket: Arc<Ticket>,
}

impl Future for ReceiptFuture {
    type Output = Result<u32, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match self.inner.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        drop(self.inner.take());
        self.ticket.finish(match &result {
            Ok(value) => Receipt::Success(*value),
            Err(error) => Receipt::Failed(error.to_string()),
        });
        Poll::Ready(result)
    }
}

impl Drop for ReceiptFuture {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.ticket.finish(Receipt::Cancelled);
    }
}

impl<S> Layer<S> for ReceiptLayer {
    type Service = ReceiptService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ReceiptService(inner)
    }
}

impl<S> Service<Task<Job>> for ReceiptService<S>
where
    S: Service<Task<Job>, Response = u32> + Send,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
{
    type Response = u32;
    type Error = BoxError;
    type Future = ReceiptFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Task<Job>) -> Self::Future {
        let ticket = request.args.ticket.clone();
        let future = self.0.call(request);
        ReceiptFuture {
            inner: Some(Box::pin(async move { future.await.map_err(Into::into) })),
            ticket,
        }
    }
}

/// Producer dependency must name the worker; an accepted round is awaited even on stop.
pub(crate) async fn produce(
    context: ServiceContext,
    queue: Queue,
    target: String,
    rounds: u32,
    delay: Duration,
) -> Result<Vec<Receipt>, BoxError> {
    match context
        .set_health(miju_services::Health::Healthy)
        .and_then(|()| context.ready())
    {
        Ok(()) => {}
        Err(_) if context.is_stopping() => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    }
    let mut results = Vec::new();
    for value in 1..=rounds {
        if context.is_stopping() {
            break;
        }
        let receipt = match queue
            .submit(target.clone(), value, context.stopping())
            .await?
        {
            Submission::Accepted(receipt) => receipt,
            Submission::Stopped => break,
            Submission::Skipped => {
                return Err(std::io::Error::other(
                    "serial producer unexpectedly overlaps its target",
                )
                .into());
            }
        };
        results.push(receipt.await?);
        if value == rounds {
            break;
        }
        tokio::select! {
            biased;
            _ = context.stopping() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
    Ok(results)
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReceiptLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReceiptLayer")
    }
}

impl<S> std::fmt::Debug for ReceiptService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReceiptService")
    }
}

impl std::fmt::Debug for ReceiptFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiptFuture")
            .field("active", &self.inner.is_some())
            .finish_non_exhaustive()
    }
}
