//! Preconstructed-resource rollback fixtures, separate from the normal initialization example.

use crate::{
    services::{SampleFile, Sampler},
    settings::Settings,
};
use miju_services::{App, BoxError, BuildError, ExitReport, RunError};
use std::{future::Future, io, path::Path, sync::Arc};

/// The application composition root: the output goes to the service constructor
/// and its close action goes to the same App.
pub(crate) fn assemble(settings: Settings, output: Arc<SampleFile>) -> Result<App, BuildError> {
    let cleanup = output.clone();
    App::builder()
        .cleanup("sample-file", move || async move {
            cleanup.close().await?;
            Ok(())
        })
        .service("sampler", Sampler::new(settings, output))
        .build()
}

pub(crate) async fn run<F, E>(
    settings: Settings,
    stop: F,
) -> Result<Arc<ExitReport>, BootstrapError>
where
    F: Future<Output = Result<(), E>> + Send,
    E: Into<BoxError> + Send + 'static,
{
    let output = Arc::new(
        open_output(settings.output_file())
            .await
            .map_err(|source| BootstrapError {
                source: source.into(),
                rollback: None,
            })?,
    );
    finish(assemble(settings, output.clone()), output, stop).await
}

/// Until start accepts, retain independent rollback access to the output.
/// Choose recovery through StartError::into_app instead if you intend to retry;
/// this bootstrap chooses rollback, so its returned App must not be retried.
pub(crate) async fn finish<F, E>(
    app: Result<App, BuildError>,
    output: Arc<SampleFile>,
    stop: F,
) -> Result<Arc<ExitReport>, BootstrapError>
where
    F: Future<Output = Result<(), E>> + Send,
    E: Into<BoxError> + Send + 'static,
{
    let app = match app {
        Ok(app) => app,
        Err(source) => {
            return Err(BootstrapError {
                source: source.into(),
                rollback: output.close().await.err(),
            });
        }
    };
    match app.run_until(stop).await {
        Ok(report) => Ok(report),
        Err(source) => {
            let rollback = if matches!(source, RunError::Start(_)) {
                output.close().await.err()
            } else {
                None
            };
            Err(BootstrapError {
                source: source.into(),
                rollback,
            })
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("application bootstrap failed: {source}; rollback error: {rollback:?}")]
pub(crate) struct BootstrapError {
    #[source]
    pub(crate) source: BoxError,
    pub(crate) rollback: Option<io::Error>,
}

/// Pre-start acquisition belongs to this test bootstrap, without an App certificate.
pub(crate) async fn open_output(path: &Path) -> io::Result<SampleFile> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    Ok(SampleFile::from_writer(file))
}
