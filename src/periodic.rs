//! Direct periodic operations: a period and an operation, no queue.
//!
//! [`every`] waits one period after each round finishes. [`Periodic::on_grid`]
//! uses fixed slots from the start instead, and skips slots that pass while a
//! round is busy or the service wakes late; it never runs a burst to catch up.
//! A round's business error is recorded and the next period follows. There is
//! no retry.
//!
//! Rounds run one at a time inside the service's own root. A stop request is
//! seen between rounds; during a round, [`Round::stopping`] tells the operation,
//! and the service is not drained until the round has returned. A round that
//! ignores stop is aborted with the root when the service's grace expires.
#![forbid(unsafe_code)]

use crate::{BoxError, SharedError, service::Context, service::Service};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::time::Instant;

/// Why a periodic service could not be assembled, or failed.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PeriodicError {
    /// A zero period cannot make progress.
    #[error("period must be positive")]
    ZeroPeriod,
    /// A time or counter could not be represented.
    #[error("periodic time or counter overflow")]
    Overflow,
    /// A round failed and the service was set to stop on errors.
    #[error("periodic round failed: {0}")]
    RoundFailed(#[source] SharedError),
}

/// What a periodic service has done so far. Published with the service's
/// snapshots; read it with `ServiceSnapshot::detail::<PeriodicStats>()`.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct PeriodicStats {
    /// Rounds whose operation was called.
    pub started: u64,
    /// Rounds that returned, with or without an error.
    pub finished: u64,
    /// Rounds that returned an error.
    pub failed: u64,
    /// Grid slots skipped because a round was busy or the service woke late.
    /// Always zero with a fixed delay.
    pub skipped: u64,
    /// The slot the most recent round was scheduled for.
    pub scheduled_at: Option<Instant>,
    /// When the most recent round actually started.
    pub started_at: Option<Instant>,
    /// When the most recent round returned; `None` while it is running.
    pub finished_at: Option<Instant>,
    /// The most recent round error.
    pub last_error: Option<SharedError>,
}

/// What one round's operation is given.
#[derive(Clone, Debug)]
pub struct Round {
    id: u64,
    scheduled_at: Instant,
    started_at: Instant,
    ctx: Context,
}

impl Round {
    /// The round number, starting at one. Skipped slots get no number.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The slot this round was scheduled for.
    pub fn scheduled_at(&self) -> Instant {
        self.scheduled_at
    }

    /// When this round actually started.
    pub fn started_at(&self) -> Instant {
        self.started_at
    }

    /// Whether the service has been asked to stop.
    pub fn is_stopping(&self) -> bool {
        self.ctx.is_stopping()
    }

    /// Resolves when the service is asked to stop. The round still owns the
    /// service until it returns.
    pub async fn stopping(&self) {
        self.ctx.stopping().await;
    }

    /// The service's context, for health, work and cleanup.
    pub fn context(&self) -> &Context {
        &self.ctx
    }
}

/// A periodic service. Built by [`every`] or [`every_with`]; registered with
/// `App::service`.
#[derive(Debug)]
#[must_use = "a periodic service does nothing until it is registered"]
pub struct Periodic<F> {
    period: Duration,
    grid: bool,
    first_after_period: bool,
    stop_on_error: bool,
    operation: F,
}

/// Call `operation` once now, then again one period after each round finishes.
///
/// A returned error is recorded and the next period follows.
pub fn every<F, Fut>(
    period: Duration,
    mut operation: F,
) -> Periodic<impl FnMut(Round) -> Fut + Send + 'static>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    every_with(period, move |_round: Round| operation())
}

/// Like [`every`], with each round's [`Round`] and a concrete error type.
pub fn every_with<F, Fut, E>(period: Duration, operation: F) -> Periodic<F>
where
    F: FnMut(Round) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    Periodic {
        period,
        grid: false,
        first_after_period: false,
        stop_on_error: false,
        operation,
    }
}

impl<F> Periodic<F> {
    /// Use fixed slots from the start: a round finishing late skips the
    /// slots that passed, and a late wake runs only the latest slot.
    pub fn on_grid(mut self) -> Self {
        self.grid = true;
        self
    }

    /// Wait one period before the first round.
    pub fn first_after_period(mut self) -> Self {
        self.first_after_period = true;
        self
    }

    /// Fail the service on the first round error instead of recording it.
    pub fn stop_on_error(mut self) -> Self {
        self.stop_on_error = true;
        self
    }
}

impl<F, Fut, E> Service for Periodic<F>
where
    F: FnMut(Round) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    type Error = PeriodicError;

    fn check(&self) -> Result<(), BoxError> {
        if self.period.is_zero() {
            return Err(PeriodicError::ZeroPeriod.into());
        }
        if Instant::now().checked_add(self.period).is_none() {
            return Err(PeriodicError::Overflow.into());
        }
        Ok(())
    }

    async fn run(mut self, ctx: Context) -> Result<(), PeriodicError> {
        let period = self.period;
        let origin = Instant::now();
        let mut slot = u64::from(self.first_after_period);
        let mut planned = grid(origin, period, slot)?;
        let mut stats = PeriodicStats::default();
        ctx.publish(Arc::new(stats.clone()));
        ctx.ready();
        loop {
            tokio::select! {
                biased;
                _ = ctx.stopping() => return Ok(()),
                _ = tokio::time::sleep_until(planned) => {}
            }

            let started = Instant::now();
            let actual_slot = if self.grid {
                // A late wake runs the latest slot once, not every missed one.
                floor(origin, started, period)?.max(slot)
            } else {
                slot
            };
            stats.skipped = add(stats.skipped, actual_slot - slot)?;
            stats.started = add(stats.started, 1)?;
            let scheduled_at = if self.grid {
                grid(origin, period, actual_slot)?
            } else {
                planned
            };
            stats.scheduled_at = Some(scheduled_at);
            stats.started_at = Some(started);
            stats.finished_at = None;
            ctx.publish(Arc::new(stats.clone()));

            let round = Round {
                id: stats.started,
                scheduled_at,
                started_at: started,
                ctx: ctx.clone(),
            };
            let result: Result<(), SharedError> = (self.operation)(round)
                .await
                .map_err(|error| Arc::from(error.into()));

            let finished = Instant::now();
            stats.finished = add(stats.finished, 1)?;
            stats.finished_at = Some(finished);
            if let Err(error) = &result {
                stats.failed = add(stats.failed, 1)?;
                stats.last_error = Some(error.clone());
            }
            if self.grid {
                let elapsed = finished.saturating_duration_since(origin).as_nanos();
                let next = u64::try_from(elapsed.div_ceil(period.as_nanos()))
                    .map_err(|_| PeriodicError::Overflow)?
                    .max(add(actual_slot, 1)?);
                stats.skipped = add(stats.skipped, next - actual_slot - 1)?;
                slot = next;
                planned = grid(origin, period, slot)?;
            } else {
                planned = finished
                    .checked_add(period)
                    .ok_or(PeriodicError::Overflow)?;
            }
            ctx.publish(Arc::new(stats.clone()));

            if let Err(error) = result
                && self.stop_on_error
            {
                return Err(PeriodicError::RoundFailed(error));
            }
        }
    }
}

fn add(value: u64, more: u64) -> Result<u64, PeriodicError> {
    value.checked_add(more).ok_or(PeriodicError::Overflow)
}

/// The slot `now` falls in.
fn floor(origin: Instant, now: Instant, period: Duration) -> Result<u64, PeriodicError> {
    u64::try_from(now.saturating_duration_since(origin).as_nanos() / period.as_nanos())
        .map_err(|_| PeriodicError::Overflow)
}

/// The start of `slot`.
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
