use miju_services::{App, ExitReport, RunError};
use std::{future::Future, io, sync::Arc};

#[derive(Debug)]
pub(crate) enum HostOutcome {
    Exited(Arc<ExitReport>),
    ForceExit,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HostError {
    #[error(transparent)]
    Run(#[from] RunError),
    #[error("second signal failed: {source}; final result: {result:?}")]
    SecondSignal {
        source: io::Error,
        result: Result<Arc<ExitReport>, RunError>,
    },
}

/// Observe termination from startup; a service fault does not wait for the first signal.
pub(crate) async fn run<F, G>(app: App, first: F, second: G) -> Result<HostOutcome, HostError>
where
    F: Future<Output = io::Result<()>> + Send,
    G: Future<Output = io::Result<()>> + Send,
{
    let (signal_seen, first_signal) = tokio::sync::oneshot::channel();
    let running = app.run_until(async move {
        let result = first.await;
        let _ = signal_seen.send(());
        result
    });
    tokio::pin!(running);

    tokio::select! {
        biased;
        result = &mut running => return result.map(HostOutcome::Exited).map_err(HostError::Run),
        _ = first_signal => {}
    }

    // The second signal future is first polled here. Dropping a Unix ctrl_c future
    // does not restore the default SIGINT handler; this force-exit policy is explicit.
    tokio::select! {
        biased;
        result = &mut running => result.map(HostOutcome::Exited).map_err(HostError::Run),
        signal = second => match signal {
            Ok(()) => Ok(HostOutcome::ForceExit),
            Err(source) => Err(HostError::SecondSignal { source, result: running.await }),
        }
    }
}
