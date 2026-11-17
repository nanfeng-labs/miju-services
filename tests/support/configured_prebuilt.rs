//! A preconstructed output handed to the application, separate from the normal
//! initialization example.
//!
//! The output is opened before the App exists. The application becomes
//! responsible for closing it only when the `sample-file` resource runs and
//! registers the close; until then the bootstrap is. So the bootstrap closes it
//! itself when start is refused, and when the final report shows that the
//! application never took the close over (stop arrived before the resource ran).

use crate::{
    services::{SampleFile, Sampler},
    settings::Settings,
};
use miju_services::{App, BoxError, Report, RunError, Service, Spec};
use std::{future::Future, io, path::Path, sync::Arc};

/// The cleanup label under which the application takes the output over.
pub(crate) const OUTPUT: &str = "sample-file";

/// The composition root: the output becomes a resource whose close is
/// registered where the application receives it.
pub(crate) fn assemble(settings: Settings, output: Arc<SampleFile>) -> App {
    let mut app = App::new();
    let file = app.resource(OUTPUT, move |ctx| async move {
        let closing = output.clone();
        ctx.defer(OUTPUT, async move { closing.close().await })?;
        Ok(output)
    });
    app.service_fn(Spec::new("sampler").after(&file), move |ctx| async move {
        Sampler::new(settings, file.get(&ctx)?).run(ctx).await?;
        Ok(())
    });
    app
}

pub(crate) async fn run<F, E>(settings: Settings, stop: F) -> Result<Arc<Report>, BootstrapError>
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

/// Run `app`, closing `output` here whenever the application did not take it over.
pub(crate) async fn finish<F, E>(
    app: App,
    output: Arc<SampleFile>,
    stop: F,
) -> Result<Arc<Report>, BootstrapError>
where
    F: Future<Output = Result<(), E>> + Send,
    E: Into<BoxError> + Send + 'static,
{
    let result = app.run_until(stop).await;
    let taken_over = match &result {
        Ok(report) => took_over(report),
        Err(error) => error.report().is_some_and(|report| took_over(report)),
    };
    // A lost driver gives no answer; the output is left alone rather than
    // closed under a cleanup that may still be running.
    let rollback = if taken_over || matches!(result, Err(RunError::Lost { .. })) {
        None
    } else {
        output.close().await.err()
    };
    match result {
        Ok(report) if rollback.is_none() => Ok(report),
        Ok(report) => Err(BootstrapError {
            source: format!("output close failed after {report}").into(),
            rollback,
        }),
        Err(source) => Err(BootstrapError {
            source: source.into(),
            rollback,
        }),
    }
}

fn took_over(report: &Report) -> bool {
    report
        .cleanup
        .iter()
        .any(|cleanup| cleanup.name.name() == OUTPUT)
}

#[derive(Debug, thiserror::Error)]
#[error("application bootstrap failed: {source}; rollback error: {rollback:?}")]
pub(crate) struct BootstrapError {
    #[source]
    pub(crate) source: BoxError,
    pub(crate) rollback: Option<io::Error>,
}

/// Pre-start acquisition belongs to this test bootstrap.
pub(crate) async fn open_output(path: &Path) -> io::Result<SampleFile> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    Ok(SampleFile::from_writer(file))
}
