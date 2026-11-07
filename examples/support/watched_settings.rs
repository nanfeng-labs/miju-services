use miju_services::{BoxError, ServiceContext};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub(crate) interval: Duration,
    pub(crate) message: String,
}

/// Read one immutable snapshot per round; writers validate settings before publishing.
///
/// Changes can wake the next round early, but never replace an in-progress round's
/// snapshot. Watch coalesces intermediate values. Closing the sender keeps the last
/// valid settings until normal service stop; it does not certify consumer completion.
pub(crate) async fn run<F, Fut>(
    context: ServiceContext,
    mut settings: watch::Receiver<Arc<Settings>>,
    mut round: F,
) -> Result<(), BoxError>
where
    F: FnMut(Arc<Settings>) -> Fut + Send,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    let mut updates_open = true;
    context.ready();
    while !context.is_stopping() {
        let snapshot = settings.borrow_and_update().clone();
        round(snapshot.clone()).await?;

        let pause = tokio::time::sleep(snapshot.interval);
        tokio::pin!(pause);
        loop {
            tokio::select! {
                biased;
                _ = context.stopping() => return Ok(()),

                _ = &mut pause => break,

                changed = settings.changed(), if updates_open => {
                    if changed.is_ok() {
                        break;
                    }
                    // A closed channel is permanently ready; stop polling this branch.
                    updates_open = false;
                }
            }
        }
    }
    Ok(())
}
