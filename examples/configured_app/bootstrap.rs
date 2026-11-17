use crate::{
    services::{SampleFile, Sampler},
    settings::Settings,
};
use miju_services::{App, Service, WorkError};
use std::{convert::Infallible, fs::OpenOptions, sync::Arc};
use tokio::fs::File;

/// Register the service now; acquire its output only after start accepts.
pub(crate) fn assemble(settings: Settings) -> App {
    let mut app = App::new();
    app.service_fn("sampler", move |ctx| async move {
        let path = settings.output_file().to_owned();
        // Owned blocking work: if this root is aborted while it waits, the
        // opened file stays in the work's result and is closed with it. The
        // open's own error is the value, so the root reports it unchanged.
        let opened = ctx.spawn_blocking("open sample-file", move || {
            Ok::<_, Infallible>(OpenOptions::new().create(true).append(true).open(path))
        })?;
        let file = match opened.await {
            Ok(result) => result?,
            // Stop arrived before the open started: nothing was acquired.
            Err(WorkError::Cancelled) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let output = Arc::new(SampleFile::from_writer(File::from_std(file)));
        // Registered by the code that completed the acquisition, before any await.
        let closing = output.clone();
        ctx.defer("sample-file", async move { closing.close().await })?;
        Sampler::new(settings, output).run(ctx).await?;
        Ok(())
    });
    app
}
