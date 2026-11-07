//! Application-private admission and final receipts around miju execution.
use miju::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    prelude::{Task, WorkerContext},
    task::task_id::RandomId,
};
use std::{
    collections::HashSet,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tower::{Layer, Service};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Receipt {
    Success,
    Failed(String),
    Cancelled,
}

struct Ticket {
    sender: Mutex<Option<oneshot::Sender<Receipt>>>,
    target: String,
    targets: Arc<Mutex<HashSet<String>>>,
    _permit: OwnedSemaphorePermit,
}

impl Ticket {
    fn finish(&self, result: Receipt) {
        let sender = self.sender.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.targets.lock().unwrap().remove(&self.target);
        self.finish(Receipt::Cancelled);
    }
}

#[derive(Clone)]
pub(crate) struct Job {
    pub(crate) target: String,
    ticket: Arc<Ticket>,
}

#[derive(Clone)]
pub(crate) struct Queue {
    sender: mpsc::UnboundedSender<Task<Job>>,
    budget: Arc<Semaphore>,
    targets: Arc<Mutex<HashSet<String>>>,
}

pub(crate) enum Submission {
    Accepted(oneshot::Receiver<Receipt>),
    Busy,
    Closed,
}

impl Queue {
    pub(crate) fn new(capacity: usize) -> (Self, QueueBackend) {
        assert!(capacity > 0);
        let (sender, receiver) = mpsc::unbounded_channel();
        let budget = Arc::new(Semaphore::new(capacity));
        (
            Self {
                sender,
                budget: budget.clone(),
                targets: Arc::default(),
            },
            QueueBackend { receiver, budget },
        )
    }

    /// The permit covers queued plus executing work, not just worker concurrency.
    pub(crate) fn submit(&self, target: impl Into<String>) -> Submission {
        let target = target.into();
        if self.sender.is_closed() {
            return Submission::Closed;
        }
        let permit = match self.budget.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) if self.budget.is_closed() => return Submission::Closed,
            Err(_) => return Submission::Busy,
        };
        if !self.targets.lock().unwrap().insert(target.clone()) {
            return Submission::Busy;
        }
        let (sender, receipt) = oneshot::channel();
        let job = Job {
            target: target.clone(),
            ticket: Arc::new(Ticket {
                sender: Mutex::new(Some(sender)),
                target,
                targets: self.targets.clone(),
                _permit: permit,
            }),
        };
        match self.sender.send(Task::new(job)) {
            Ok(()) => Submission::Accepted(receipt),
            Err(error) => {
                drop(error);
                Submission::Closed
            }
        }
    }
}

/// Explicit stop policy: finish taken work and cancel every job still queued.
pub(crate) struct QueueBackend {
    receiver: mpsc::UnboundedReceiver<Task<Job>>,
    budget: Arc<Semaphore>,
}

impl Backend for QueueBackend {
    type Task = Task<Job>;
    type Error = Infallible;
    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Task<Job>, Infallible>>> {
        self.receiver.poll_recv(cx).map(|value| value.map(Ok))
    }
    fn poll_close(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Infallible>> {
        self.receiver.close();
        self.budget.close();
        // Stream release drops the receiver and its queued tickets before EOF is certified.
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for QueueBackend {
    type Args = Job;
    type Config = ();
    type Kind = Ephemeral;
    type Layer = tower::layer::util::Identity;
    type Id = RandomId;
    fn config(&self) -> &() {
        &()
    }
    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}

/// Place outside retry layers to publish only the final execution-chain result.
#[derive(Clone)]
pub(crate) struct Receipts;

#[derive(Clone)]
pub(crate) struct ReceiptService<S>(S);

impl<S> Layer<S> for Receipts {
    type Service = ReceiptService<S>;
    fn layer(&self, service: S) -> Self::Service {
        ReceiptService(service)
    }
}

pub(crate) struct ReceiptFuture<F> {
    future: Option<Pin<Box<F>>>,
    ticket: Arc<Ticket>,
}

impl<F, R, E> Future for ReceiptFuture<F>
where
    F: Future<Output = Result<R, E>>,
    E: std::fmt::Display,
{
    type Output = Result<R, E>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(self.future.as_mut().unwrap().as_mut().poll(cx));
        drop(self.future.take());
        self.ticket.finish(match &result {
            Ok(_) => Receipt::Success,
            Err(error) => Receipt::Failed(error.to_string()),
        });
        Poll::Ready(result)
    }
}

impl<F> Drop for ReceiptFuture<F> {
    fn drop(&mut self) {
        drop(self.future.take());
        self.ticket.finish(Receipt::Cancelled);
    }
}

impl<S> Service<Task<Job>> for ReceiptService<S>
where
    S: Service<Task<Job>>,
    S::Error: std::fmt::Display,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ReceiptFuture<S::Future>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }
    fn call(&mut self, task: Task<Job>) -> Self::Future {
        let ticket = task.args.ticket.clone();
        ReceiptFuture {
            future: Some(Box::pin(self.0.call(task))),
            ticket,
        }
    }
}
