//! Lifecycle bridges for existing miju execution bodies.
//!
//! Workers retain their own backend, middleware, concurrency, and retry behavior.
//! Construct the worker with the **same** context passed to [`worker`]; the upstream
//! stream cannot expose its context for identity verification. A Start event only
//! establishes the driver, not remote backend or business readiness.
//!
//! [`monitor`] observes a monitor as one service. Internal worker failures can be
//! delayed by resident siblings; register workers separately for immediate service
//! failure propagation. Neither bridge equates stream EOF with all jobs succeeding.
#![forbid(unsafe_code)]

use crate::{Health, Service, ServiceContext};
use futures_util::{Stream, StreamExt};
use miju_core::{
    error::WorkerError,
    monitor::{Monitor, MonitorError},
    worker::{context::WorkerContext, event::Event},
};
use std::{fmt, sync::Arc};

/// Attach a worker's event stream and the exact context used to construct it.
///
/// The bridge consumes Start before requesting stop and keeps polling until EOF or
/// a driver error. Do not separately start/restart the context after attachment.
/// Normal EOF confirms the stream's close path; errors and forced cancellation do
/// not certify backend asynchronous cleanup. Natural EOF never waits for a signal.
pub fn worker<S>(control: WorkerContext, events: S) -> impl Service<Error = WorkerError>
where
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    WorkerService { control, events }
}

struct WorkerService<S> {
    control: WorkerContext,
    events: S,
}

impl<S> Service for WorkerService<S>
where
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    type Error = WorkerError;

    async fn run(self, context: ServiceContext) -> Result<(), WorkerError> {
        context.backend_unconfirmed(true);
        let mut events = Box::pin(self.events);
        let mut started = false;
        let mut stopping = false;
        loop {
            tokio::select! {
                biased;
                _ = context.stopping(), if started && !stopping => {
                    stopping = true;
                    if !self.control.is_terminated() {
                        self.control.stop()?;
                    }
                }
                event = events.next() => match event {
                    Some(Ok(Event::Start)) => {
                        started = true;
                        report_ready(&context)
                            .map_err(|e| WorkerError::IoError(std::io::Error::other(e)))?;
                    }
                    Some(Ok(Event::Error(error))) =>
                        context.record_execution_error(Arc::new(ExecutionError(error))),
                    Some(Ok(_)) => {},
                    Some(Err(error)) => return Err(error),
                    None => {
                        context.backend_unconfirmed(false);
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn report_ready(context: &ServiceContext) -> Result<(), crate::ContextClosed> {
    match context
        .set_health(Health::Healthy)
        .and_then(|()| context.ready())
    {
        Ok(()) => Ok(()),
        Err(_) if context.is_stopping() => Ok(()),
        Err(error) => Err(error),
    }
}

/// Attach one whole monitor, retaining its existing internal restart policy.
///
/// Initialization means the monitor driver is established. The monitor's aggregate
/// failure can be delayed until all its workers finish. On stop, request shutdown
/// and continue waiting on the original run future; do not recreate it.
/// Successful completion after any restart retains unconfirmed backend cleanup:
/// the replacement finishing cannot certify the failed generation's close path.
pub fn monitor(monitor: Monitor) -> impl Service<Error = MonitorError> {
    MonitorService(monitor)
}

struct MonitorService(Monitor);

impl Service for MonitorService {
    type Error = MonitorError;

    async fn run(self, context: ServiceContext) -> Result<(), MonitorError> {
        context.backend_unconfirmed(true);
        let control = self.0.context();
        let run = self.0.run();
        tokio::pin!(run);
        report_ready(&context)
            .map_err(|e| MonitorError::ShutdownSignal(std::io::Error::other(e)))?;
        let result = tokio::select! {
            biased;
            _ = context.stopping() => {
                control.shutdown()?;
                run.await
            }
            result = &mut run => result,
        };
        if result.is_ok()
            && control
                .workers()
                .iter()
                .all(|worker| worker.restarts() == 0)
        {
            context.backend_unconfirmed(false);
        }
        result
    }
}

#[derive(Debug)]
struct ExecutionError(Arc<Box<dyn std::error::Error + Send + Sync>>);

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
