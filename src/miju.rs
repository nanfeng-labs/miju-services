//! Attach miju execution to an application's lifecycle.
//!
//! miju keeps ownership of its backend, middleware, concurrency and retries.
//! [`worker_from`] supplies the context for constructing a worker; [`monitor`]
//! attaches an existing group with its upstream restart policy. Both keep the
//! original execution alive through cooperative stop. Driver errors or restart
//! history leave aggregate backend responsibility unconfirmed, blocking cleanup.
#![forbid(unsafe_code)]

use crate::{BoxError, Service, ServiceContext, work::WorkSpec};
use ::miju::prelude::{Event, Monitor, WorkerContext, WorkerError};
use futures_util::{Stream, StreamExt};
use std::{fmt, sync::Arc};

/// Construct a worker with the supplied context when its service is admitted.
///
/// The factory is called once, inside accepted aggregate work. It must construct
/// the returned stream with this exact context, for example with
/// `WorkerBuilder::new(context)`. A closure can ignore its argument, so identity
/// is a caller contract rather than a type-system guarantee. Start establishes
/// the driver, not remote backend readiness. Job errors remain observable without
/// failing the lifecycle. Use a Finite ServiceSpec for naturally finite sources.
pub fn worker_from<F, S>(factory: F) -> impl Service<Error = BoxError>
where
    F: FnOnce(WorkerContext) -> S + Send + 'static,
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    attach_worker(move |owner| {
        let control = WorkerContext::new(owner.id().name());
        let events = factory(control.clone());
        (control, events)
    })
}

/// Attach a prebuilt stream and the exact context used to construct it.
///
/// Prefer [`worker_from`] to avoid pairing two separately constructed objects.
/// Do not start or restart this context independently after attachment.
pub fn worker<S>(control: WorkerContext, events: S) -> impl Service<Error = BoxError>
where
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    attach_worker(move |_| (control, events))
}

fn attach_worker<F, S>(factory: F) -> impl Service<Error = BoxError>
where
    F: FnOnce(&ServiceContext) -> (WorkerContext, S) + Send + 'static,
    S: Stream<Item = Result<Event, WorkerError>> + Send + 'static,
{
    crate::service::service_fn(move |context: ServiceContext| async move {
        let owner = context.clone();
        let work = context
            .work()
            .spawn(WorkSpec::aggregate("miju-worker"), move |work| {
                let (control, events) = factory(&owner);
                drive_worker(owner, work, control, events)
            })?;
        // The scope retains aggregate failures even after the root has returned.
        work.wait().await;
        Ok(())
    })
}

async fn drive_worker<S>(
    owner: ServiceContext,
    work: crate::work::WorkContext,
    control: WorkerContext,
    events: S,
) -> Result<(), WorkerError>
where
    S: Stream<Item = Result<Event, WorkerError>>,
{
    let mut events = Box::pin(events);
    let mut started = false;
    let mut stopping = false;
    loop {
        tokio::select! {
            biased;
            _ = work.stopping(), if started && !stopping => {
                stopping = true;
                if !control.is_terminated() {
                    control.stop()?;
                }
            }
            event = events.next() => match event {
                Some(Ok(Event::Start)) => {
                    started = true;
                    owner.ready();
                }
                Some(Ok(Event::Error(error))) => {
                    owner.record_execution_error(Arc::new(JobError(error)));
                }
                Some(Ok(_)) => {},
                Some(Err(error)) => return Err(error),
                None => return Ok(()),
            },
        }
    }
}

/// Attach an entire Monitor and preserve its aggregate failure/restart semantics.
///
/// Readiness means its driver is established, not that every worker/backend is
/// ready. Worker faults can be delayed by resident siblings; register workers
/// separately when faults must stop the application immediately. An Ok result
/// after a restart cannot certify the previous generation's backend cleanup.
pub fn monitor(monitor: Monitor) -> impl Service<Error = BoxError> {
    crate::service::service_fn(move |context: ServiceContext| async move {
        let owner = context.clone();
        let work = context.work().spawn(
            WorkSpec::aggregate("miju-monitor"),
            move |work| async move {
                let control = monitor.context();
                let run = monitor.run();
                tokio::pin!(run);
                owner.ready();
                tokio::select! {
                    biased;
                    _ = work.stopping() => { control.shutdown()?; run.await?; }
                    result = &mut run => result?,
                }
                if control
                    .workers()
                    .iter()
                    .any(|worker| worker.restarts() != 0)
                {
                    return Err::<(), BoxError>(
                        std::io::Error::other(
                            "monitor restarted; previous backend cleanup is unconfirmed",
                        )
                        .into(),
                    );
                }
                Ok(())
            },
        )?;
        work.wait().await;
        Ok(())
    })
}

#[derive(Debug)]
struct JobError(Arc<BoxError>);

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for JobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
