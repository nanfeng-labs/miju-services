//! Serial periodic work owned by a service scope, without a second retry system.
//!
//! Fixed delay waits after actual completion. Fixed rate keeps an origin grid and
//! skips elapsed opportunities. Each round is admitted as ordinary managed work;
//! stopping the service does not discard an active round's asynchronous cleanup.
#![forbid(unsafe_code)]

use crate::{
    BoxError, ContextClosed, Health, Service, ServiceContext, SharedError, SpawnError, WorkContext,
    WorkOutcome, WorkReport, WorkSpec,
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::Instant};

/// Relationship between a completed round and its next planned start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeriodicMode {
    /// Wait one full period after the business future actually returns.
    FixedDelay,
    /// Attempt on the origin grid, skipping busy and missed opportunities.
    FixedRateSkip,
}

/// When the first round is due relative to the service loop origin.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FirstTick {
    /// Start as soon as the loop is ready.
    #[default]
    Immediate,
    /// Wait one period before the first round.
    AfterPeriod,
}

/// Treatment of a returned business error; panics and scheduler faults are fatal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OnRoundError {
    /// Record the failure and schedule the next round without retrying this one.
    #[default]
    Continue,
    /// Return the business failure from the service root.
    StopService,
}

/// Validated timing and business-error policy for a serial periodic service.
#[derive(Clone, Copy, Debug)]
pub struct PeriodicRule {
    period: Duration,
    mode: PeriodicMode,
    first: FirstTick,
    on_error: OnRoundError,
}

impl PeriodicRule {
    /// Validate a positive representable period; defaults to Immediate and Continue.
    pub fn new(period: Duration, mode: PeriodicMode) -> Result<Self, PeriodicError> {
        if period.is_zero() {
            return Err(PeriodicError::ZeroPeriod);
        }
        Instant::now()
            .checked_add(period)
            .ok_or(PeriodicError::Overflow)?;
        Ok(Self {
            period,
            mode,
            first: FirstTick::Immediate,
            on_error: OnRoundError::Continue,
        })
    }

    /// Select the initial waiting rule.
    #[must_use]
    pub fn first_tick(mut self, first: FirstTick) -> Self {
        self.first = first;
        self
    }

    /// Select treatment of returned business failures.
    #[must_use]
    pub fn on_error(mut self, policy: OnRoundError) -> Self {
        self.on_error = policy;
        self
    }
}

/// A scheduling, ownership, or explicitly escalated business failure.
#[derive(Debug, thiserror::Error)]
pub enum PeriodicError {
    /// Zero periods cannot make progress.
    #[error("period must be positive")]
    ZeroPeriod,
    /// A deadline, identity, grid index, or counter cannot be represented.
    #[error("periodic time or counter overflow")]
    Overflow,
    /// The owning scope rejected a round for a reason other than normal stop.
    #[error("periodic admission failed: {0}")]
    Admission(#[source] SpawnError),
    /// A status notification was rejected outside normal stopping.
    #[error("periodic context closed outside stopping")]
    Context(#[source] ContextClosed),
    /// StopService escalated a returned business error, preserving its source.
    #[error("periodic business round failed: {0}")]
    RoundFailed(#[source] SharedError),
    /// Managed execution panicked or was unexpectedly cancelled.
    #[error("periodic managed execution failed: {0:?}")]
    Execution(Arc<WorkReport>),
    /// The accepted round did not return the owned factory after executing.
    #[error("periodic factory ownership was not returned")]
    FactoryLost,
    /// Internal round scheduling failed before returning the factory.
    #[error("periodic scheduling failed: {0}")]
    Scheduling(#[source] SharedError),
}

/// Last actual business invocation; managed abort/panic details live in WorkReport.
#[derive(Clone, Debug)]
pub struct RoundSnapshot {
    /// Monotonic admitted round identity, beginning at one.
    pub round_id: u64,
    /// Selected grid point or completion-based deadline.
    pub scheduled_at: Instant,
    /// Actual business-factory invocation instant.
    pub actual_started_at: Instant,
    /// Business return instant, absent for active, aborted, or panicking rounds.
    pub finished_at: Option<Instant>,
    /// Whether the business future returned an error.
    pub failed: bool,
}

/// Per-service periodic counters; not business result storage or durable history.
#[derive(Clone, Debug, Default)]
pub struct PeriodicSnapshot {
    /// Actual business-factory invocations, excluding cancellation before start.
    pub started: u64,
    /// Business futures that returned, including returned errors.
    pub finished: u64,
    /// Returned business errors; panics and aborts remain in the work reports.
    pub failed: u64,
    /// Grid opportunities skipped by a busy round or late scheduling.
    pub skipped: u64,
    /// Most recent actual invocation.
    pub last_round: Option<RoundSnapshot>,
}

/// One round's identity, timing, stop observation, and business health reporting.
#[derive(Clone, Debug)]
pub struct RoundContext {
    round_id: u64,
    scheduled_at: Instant,
    actual_started_at: Instant,
    work: WorkContext,
    owner: ServiceContext,
}

impl RoundContext {
    /// Admitted round identity, beginning at one; skipped grid points get no identity.
    pub fn round_id(&self) -> u64 {
        self.round_id
    }

    /// Selected scheduled deadline, independent of actual executor wakeup.
    pub fn scheduled_at(&self) -> Instant {
        self.scheduled_at
    }

    /// Instant at which the business factory was invoked.
    pub fn actual_started_at(&self) -> Instant {
        self.actual_started_at
    }

    /// Whether this managed round has been requested to stop.
    pub fn is_stopping(&self) -> bool {
        self.work.is_stopping()
    }

    /// Observe stop while retaining ownership of any asynchronous cleanup.
    pub async fn stopping(&self) {
        self.work.stopping().await;
    }

    /// Report business health; a stopping owner's ContextClosed is a normal rejection.
    ///
    /// Handle that specific rejection while continuing cleanup; do not turn a
    /// stopping health notification into a business failure with an unconditional `?`.
    pub fn set_health(&self, health: Health) -> Result<(), ContextClosed> {
        self.owner.set_health(health)
    }
}

/// Turn a rule and one asynchronous business operation into a serial service.
///
/// The mutable factory is moved into each accepted round and returned afterwards;
/// no user function runs under an admission lock. Continue retries no round. A root
/// cancellation never removes the scope's ownership of an already accepted round.
pub fn periodic<F, Fut, E>(rule: PeriodicRule, run: F) -> impl Service<Error = PeriodicError>
where
    F: FnMut(RoundContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    Periodic { rule, run }
}

struct Periodic<F> {
    rule: PeriodicRule,
    run: F,
}

impl<F, Fut, E> Service for Periodic<F>
where
    F: FnMut(RoundContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    type Error = PeriodicError;

    async fn run(self, context: ServiceContext) -> Result<(), PeriodicError> {
        let Self { rule, mut run } = self;
        let origin = Instant::now();
        let mut index = u64::from(rule.first == FirstTick::AfterPeriod);
        let mut planned = grid(origin, rule.period, index)?;
        let mut round_id = 0_u64;
        let mut stats = PeriodicSnapshot::default();
        context.update_periodic(stats.clone());
        match context
            .set_health(Health::Healthy)
            .and_then(|()| context.ready())
        {
            Ok(()) => {}
            Err(_) if context.is_stopping() => return Ok(()),
            Err(e) => return Err(PeriodicError::Context(e)),
        }
        loop {
            tokio::select! {
                biased;
                _ = context.stopping() => return Ok(()),
                _ = tokio::time::sleep_until(planned) => {}
            }
            round_id = round_id.checked_add(1).ok_or(PeriodicError::Overflow)?;
            let (send, receive) = oneshot::channel();
            let owner = context.clone();
            let mut round_stats = stats.clone();
            let work = context.work().spawn(
                WorkSpec::new(format!("round-{round_id}")),
                move |work| async move {
                    let actual = Instant::now();
                    let actual_index = if rule.mode == PeriodicMode::FixedRateSkip {
                        grid_floor(origin, actual, rule.period)?.max(index)
                    } else {
                        index
                    };
                    if rule.mode == PeriodicMode::FixedRateSkip {
                        round_stats.skipped = round_stats
                            .skipped
                            .checked_add(actual_index - index)
                            .ok_or(PeriodicError::Overflow)?;
                    }
                    let scheduled = if rule.mode == PeriodicMode::FixedRateSkip {
                        grid(origin, rule.period, actual_index)?
                    } else {
                        planned
                    };
                    round_stats.started = round_stats
                        .started
                        .checked_add(1)
                        .ok_or(PeriodicError::Overflow)?;
                    round_stats.last_round = Some(RoundSnapshot {
                        round_id,
                        scheduled_at: scheduled,
                        actual_started_at: actual,
                        finished_at: None,
                        failed: false,
                    });
                    owner.update_periodic(round_stats.clone());
                    let round = RoundContext {
                        round_id,
                        scheduled_at: scheduled,
                        actual_started_at: actual,
                        work,
                        owner: owner.clone(),
                    };
                    let result: Result<(), BoxError> = run(round).await.map_err(Into::into);
                    let finished = Instant::now();
                    round_stats.finished = round_stats
                        .finished
                        .checked_add(1)
                        .ok_or(PeriodicError::Overflow)?;
                    if result.is_err() {
                        round_stats.failed = round_stats
                            .failed
                            .checked_add(1)
                            .ok_or(PeriodicError::Overflow)?;
                    }
                    if let Some(last) = &mut round_stats.last_round {
                        last.finished_at = Some(finished);
                        last.failed = result.is_err();
                    }
                    // Account for elapsed busy opportunities even if the root is stopping.
                    if rule.mode == PeriodicMode::FixedRateSkip {
                        let next = grid_ceil(origin, finished, rule.period)?
                            .max(actual_index.checked_add(1).ok_or(PeriodicError::Overflow)?);
                        round_stats.skipped = round_stats
                            .skipped
                            .checked_add(next - actual_index - 1)
                            .ok_or(PeriodicError::Overflow)?;
                    }
                    owner.update_periodic(round_stats.clone());
                    // Sending only moves ownership; a cancelled root does not cancel this round.
                    let _ = send.send((run, round_stats, actual_index));
                    result
                },
            );
            let work = match work {
                Ok(work) => work,
                Err(SpawnError::Closed) if context.is_stopping() => return Ok(()),
                Err(error) => return Err(PeriodicError::Admission(error)),
            };
            let report = work.wait().await;
            match report.outcome() {
                WorkOutcome::CancelledBeforeStart if context.is_stopping() => return Ok(()),
                WorkOutcome::Panicked(_)
                | WorkOutcome::Aborted
                | WorkOutcome::CancelledBeforeStart => {
                    return Err(PeriodicError::Execution(report));
                }
                WorkOutcome::Succeeded | WorkOutcome::Failed(_) => {}
            }
            let (returned, updated, actual_index) =
                receive.await.map_err(|_| match report.outcome() {
                    WorkOutcome::Failed(error) => PeriodicError::Scheduling(error.clone()),
                    _ => PeriodicError::FactoryLost,
                })?;
            run = returned;
            stats = updated;
            index = actual_index;
            if let WorkOutcome::Failed(error) = report.outcome()
                && rule.on_error == OnRoundError::StopService
            {
                return Err(PeriodicError::RoundFailed(error.clone()));
            }
            if context.is_stopping() {
                return Ok(());
            }
            let finished = stats
                .last_round
                .as_ref()
                .and_then(|round| round.finished_at)
                .ok_or(PeriodicError::FactoryLost)?;
            planned = match rule.mode {
                PeriodicMode::FixedDelay => finished
                    .checked_add(rule.period)
                    .ok_or(PeriodicError::Overflow)?,
                PeriodicMode::FixedRateSkip => {
                    let next = grid_ceil(origin, finished, rule.period)?
                        .max(index.checked_add(1).ok_or(PeriodicError::Overflow)?);
                    index = next;
                    context.update_periodic(stats.clone());
                    grid(origin, rule.period, index)?
                }
            };
        }
    }
}

fn grid_floor(origin: Instant, at: Instant, period: Duration) -> Result<u64, PeriodicError> {
    u64::try_from(at.saturating_duration_since(origin).as_nanos() / period.as_nanos())
        .map_err(|_| PeriodicError::Overflow)
}

fn grid_ceil(origin: Instant, at: Instant, period: Duration) -> Result<u64, PeriodicError> {
    let elapsed = at.saturating_duration_since(origin).as_nanos();
    let p = period.as_nanos();
    u64::try_from(elapsed.div_ceil(p)).map_err(|_| PeriodicError::Overflow)
}

fn grid(origin: Instant, period: Duration, index: u64) -> Result<Instant, PeriodicError> {
    let nanos = period
        .as_nanos()
        .checked_mul(u128::from(index))
        .ok_or(PeriodicError::Overflow)?;
    let seconds = u64::try_from(nanos / 1_000_000_000).map_err(|_| PeriodicError::Overflow)?;
    let duration = Duration::new(seconds, (nanos % 1_000_000_000) as u32);
    origin.checked_add(duration).ok_or(PeriodicError::Overflow)
}
