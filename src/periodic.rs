//! Serial periodic operations without a queue or a second retry system.
//!
//! Fixed delay waits after actual completion. Fixed rate skips busy or missed
//! opportunities on an origin grid. An accepted round belongs to the service's
//! work scope even when its root or waiter is cancelled.
#![forbid(unsafe_code)]

use crate::{
    BoxError, Service, ServiceContext, SharedError,
    work::{SpawnError, WorkContext, WorkOutcome, WorkReport, WorkSpec},
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::Instant};

/// Relationship between completion and the next opportunity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeriodicMode {
    /// Wait one period after the previous round finished.
    FixedDelay,
    /// Use fixed slots, skipping those elapsed while busy or late.
    FixedRateSkip,
}

/// When to begin the first round.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FirstTick {
    /// Attempt as soon as the service starts.
    #[default]
    Immediate,
    /// Wait a full period first.
    AfterPeriod,
}

/// Business-error policy; panics and ownership failures always fail the service.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OnRoundError {
    /// Record this failure and continue with the next period, without retrying.
    #[default]
    Continue,
    /// Promote the original error to a service failure.
    StopService,
}

/// A validated timing rule; Immediate and Continue are the defaults.
#[derive(Clone, Copy, Debug)]
pub struct PeriodicRule {
    period: Duration,
    mode: PeriodicMode,
    first: FirstTick,
    on_error: OnRoundError,
}

impl PeriodicRule {
    /// Reject zero or unrepresentable periods.
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

    /// Choose the first opportunity.
    #[must_use]
    pub fn first_tick(mut self, first: FirstTick) -> Self {
        self.first = first;
        self
    }

    /// Choose whether a returned business error stops this service.
    #[must_use]
    pub fn on_error(mut self, policy: OnRoundError) -> Self {
        self.on_error = policy;
        self
    }
}

/// Invalid timing, failed execution, or an escalated business error.
#[derive(Debug, thiserror::Error)]
pub enum PeriodicError {
    /// A zero period cannot make progress.
    #[error("period must be positive")]
    ZeroPeriod,
    /// The next time or counter cannot be represented.
    #[error("periodic time or counter overflow")]
    Overflow,
    /// The scope could not admit a round.
    #[error("periodic admission failed: {0}")]
    Admission(#[source] SpawnError),
    /// The selected business error policy escalated this original error.
    #[error("periodic round failed: {0}")]
    RoundFailed(#[source] SharedError),
    /// A round panicked, was aborted, or did not return its factory.
    #[error("periodic execution failed: {0:?}")]
    Execution(Arc<WorkReport>),
}

/// Instance-local counters; actual panic/abort details remain in WorkReport.
#[derive(Clone, Debug, Default)]
pub struct PeriodicSnapshot {
    /// Business invocations, excluding cancellation before factory execution.
    pub started: u64,
    /// Business futures that returned, including errors.
    pub finished: u64,
    /// Returned business errors.
    pub failed: u64,
    /// Busy or missed grid slots; always zero in FixedDelay.
    pub skipped: u64,
    /// Most recent round's selected slot.
    pub scheduled_at: Option<Instant>,
    /// Most recent actual factory invocation.
    pub started_at: Option<Instant>,
    /// Most recent business return; absent while the latest round is running.
    pub finished_at: Option<Instant>,
}

/// Timing, cooperative stopping, and health reporting for one owned round.
#[derive(Clone, Debug)]
pub struct RoundContext {
    id: u64,
    scheduled_at: Instant,
    actual_started_at: Instant,
    work: WorkContext,
    owner: ServiceContext,
}

impl RoundContext {
    /// Invocation number beginning at one; skipped slots have no round number.
    pub fn round_id(&self) -> u64 {
        self.id
    }
    /// Selected slot, independent of actual executor wakeup.
    pub fn scheduled_at(&self) -> Instant {
        self.scheduled_at
    }
    /// Actual invocation time.
    pub fn actual_started_at(&self) -> Instant {
        self.actual_started_at
    }
    /// Whether this round has been asked to stop.
    pub fn is_stopping(&self) -> bool {
        self.work.is_stopping()
    }
    /// Observe stop without relinquishing responsibility.
    pub async fn stopping(&self) {
        self.work.stopping().await;
    }
    /// Update current service health; late notifications are harmless.
    pub fn set_health(&self, health: crate::Health) {
        self.owner.set_health(health);
    }
}

/// Wait a full period after each round; errors are recorded and the loop continues.
pub fn fixed_delay<F, Fut>(
    period: Duration,
    mut run: F,
) -> Result<impl Service<Error = PeriodicError>, PeriodicError>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    Ok(periodic(
        PeriodicRule::new(period, PeriodicMode::FixedDelay)?,
        move |_| run(),
    ))
}

/// Attempt on fixed slots, skipping busy/missed slots; no backlog or retry is added.
pub fn fixed_rate_skip<F, Fut>(
    period: Duration,
    mut run: F,
) -> Result<impl Service<Error = PeriodicError>, PeriodicError>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    Ok(periodic(
        PeriodicRule::new(period, PeriodicMode::FixedRateSkip)?,
        move |_| run(),
    ))
}

/// Advanced rule and concrete-error adapter. The factory moves through each
/// accepted round and returns over a private channel; it runs under no scope lock.
pub fn periodic<F, Fut, E>(rule: PeriodicRule, mut run: F) -> impl Service<Error = PeriodicError>
where
    F: FnMut(RoundContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    crate::service::service_fn(move |context: ServiceContext| async move {
        let origin = Instant::now();
        let mut slot = u64::from(rule.first == FirstTick::AfterPeriod);
        let mut planned = grid(origin, rule.period, slot)?;
        let mut stats = PeriodicSnapshot::default();
        context.update_periodic(stats.clone());
        context.ready();
        loop {
            tokio::select! {
                biased;
                _ = context.stopping() => return Ok(()),
                _ = tokio::time::sleep_until(planned) => {},
            }
            let owner = context.clone();
            let (send, receive) = oneshot::channel();
            let admitted =
                context
                    .work()
                    .spawn(WorkSpec::new("periodic-round"), move |work| async move {
                        let started = Instant::now();
                        let actual_slot = if rule.mode == PeriodicMode::FixedRateSkip {
                            floor(origin, started, rule.period)?.max(slot)
                        } else {
                            slot
                        };
                        stats.skipped = stats
                            .skipped
                            .checked_add(actual_slot - slot)
                            .ok_or(PeriodicError::Overflow)?;
                        stats.started = stats
                            .started
                            .checked_add(1)
                            .ok_or(PeriodicError::Overflow)?;
                        stats.scheduled_at = Some(if rule.mode == PeriodicMode::FixedRateSkip {
                            grid(origin, rule.period, actual_slot)?
                        } else {
                            planned
                        });
                        stats.started_at = Some(started);
                        stats.finished_at = None;
                        owner.update_periodic(stats.clone());
                        let result: Result<(), BoxError> = run(RoundContext {
                            id: stats.started,
                            scheduled_at: stats.scheduled_at.unwrap(),
                            actual_started_at: started,
                            work,
                            owner: owner.clone(),
                        })
                        .await
                        .map_err(Into::into);
                        let finished = Instant::now();
                        stats.finished = stats
                            .finished
                            .checked_add(1)
                            .ok_or(PeriodicError::Overflow)?;
                        stats.failed = stats
                            .failed
                            .checked_add(u64::from(result.is_err()))
                            .ok_or(PeriodicError::Overflow)?;
                        stats.finished_at = Some(finished);
                        let next_slot = if rule.mode == PeriodicMode::FixedRateSkip {
                            let elapsed = finished.saturating_duration_since(origin).as_nanos();
                            let next = u64::try_from(elapsed.div_ceil(rule.period.as_nanos()))
                                .map_err(|_| PeriodicError::Overflow)?;
                            next.max(actual_slot.checked_add(1).ok_or(PeriodicError::Overflow)?)
                        } else {
                            slot
                        };
                        if rule.mode == PeriodicMode::FixedRateSkip {
                            stats.skipped = stats
                                .skipped
                                .checked_add(next_slot - actual_slot - 1)
                                .ok_or(PeriodicError::Overflow)?;
                        }
                        owner.update_periodic(stats.clone());
                        let _ = send.send((run, stats, next_slot, finished));
                        result
                    });
            let work = match admitted {
                Ok(work) => work,
                Err(SpawnError::Closed) if context.is_stopping() => return Ok(()),
                Err(error) => return Err(PeriodicError::Admission(error)),
            };
            let report = work.wait().await;
            if matches!(report.outcome(), WorkOutcome::CancelledBeforeStart)
                && context.is_stopping()
            {
                return Ok(());
            }
            if !matches!(
                report.outcome(),
                WorkOutcome::Succeeded | WorkOutcome::Failed(_)
            ) || report.release_panic().is_some()
            {
                return Err(PeriodicError::Execution(report));
            }
            let (factory, returned, next, finished) = receive
                .await
                .map_err(|_| PeriodicError::Execution(report.clone()))?;
            run = factory;
            stats = returned;
            slot = next;
            if let WorkOutcome::Failed(error) = report.outcome()
                && rule.on_error == OnRoundError::StopService
            {
                return Err(PeriodicError::RoundFailed(error.clone()));
            }
            planned = match rule.mode {
                PeriodicMode::FixedDelay => finished
                    .checked_add(rule.period)
                    .ok_or(PeriodicError::Overflow)?,
                PeriodicMode::FixedRateSkip => grid(origin, rule.period, slot)?,
            };
        }
    })
}

fn floor(origin: Instant, now: Instant, period: Duration) -> Result<u64, PeriodicError> {
    u64::try_from(now.saturating_duration_since(origin).as_nanos() / period.as_nanos())
        .map_err(|_| PeriodicError::Overflow)
}

fn grid(origin: Instant, period: Duration, slot: u64) -> Result<Instant, PeriodicError> {
    let nanos = period
        .as_nanos()
        .checked_mul(u128::from(slot))
        .ok_or(PeriodicError::Overflow)?;
    let seconds = u64::try_from(nanos / 1_000_000_000).map_err(|_| PeriodicError::Overflow)?;
    origin
        .checked_add(Duration::new(seconds, (nanos % 1_000_000_000) as u32))
        .ok_or(PeriodicError::Overflow)
}
