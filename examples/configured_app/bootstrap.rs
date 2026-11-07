use crate::{
    services::{SampleFile, Sampler},
    settings::Settings,
};
use miju_services::{
    App, BoxError, BuildError, Service,
    work::{SpawnError, WorkSpec},
};
use std::{fs::OpenOptions, sync::Arc};
use tokio::{fs::File, sync::oneshot};

/// Register the service now; acquire its output only after start accepts.
pub(crate) fn assemble(settings: Settings) -> Result<App, BuildError> {
    App::builder()
        .service_fn("sampler", move |context| async move {
            let path = settings.output_file().to_owned();
            let (opened, output) = oneshot::channel();
            // Retain the actual open even when the initializer is aborted.
            let admission = context.work().spawn_blocking(
                WorkSpec::new("open sample-file").fail_service(),
                move |_| {
                    let result = OpenOptions::new().create(true).append(true).open(path);
                    let _ = opened.send(result);
                    Ok::<(), std::io::Error>(())
                },
            );
            match admission {
                Ok(_) => {}
                Err(SpawnError::Closed) if context.is_stopping() => return Ok(()),
                Err(error) => return Err(error.into()),
            }
            let file = match output.await {
                Ok(result) => result?,
                Err(_) if context.is_stopping() => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let output = Arc::new(SampleFile::from_writer(File::from_std(file)));
            let cleanup = output.clone();
            if let Err(rejected) =
                context.defer_cleanup("sample-file", move || async move { cleanup.close().await })
            {
                rejected.into_inner()().await?;
                return Err(BoxError::from(
                    "sample-file cleanup registration was rejected",
                ));
            }
            Sampler::new(settings, output).run(context).await?;
            Ok(())
        })
        .build()
}
