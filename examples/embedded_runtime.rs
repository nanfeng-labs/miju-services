//! A synchronous host retains its runtime across an incomplete shutdown observation.

use miju_services::{
    AppOptions, Application, BoxError, DrainObservation, Health, ServiceSpec, ShutdownReport,
    service_fn,
};
use std::time::Duration;

fn main() -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (release, cleanup_release) = tokio::sync::oneshot::channel();
    let (first, observer, second, other) = runtime.block_on(async {
        let first = Application::builder()
            .options(AppOptions {
                shutdown_grace: Duration::from_millis(20),
                abort_wait: Duration::ZERO,
                ..AppOptions::default()
            })
            .service(
                ServiceSpec::new("first"),
                service_fn(move |context| async move {
                    let cleanup = move || async move {
                        cleanup_release.await?;
                        Ok::<(), BoxError>(())
                    };
                    if let Err(rejected) = context.defer_cleanup("save", cleanup) {
                        rejected.into_cleanup()().await?;
                        return Err(std::io::Error::other("cleanup rejected").into());
                    }
                    match context
                        .set_health(Health::Healthy)
                        .and_then(|()| context.ready())
                    {
                        Ok(()) => {}
                        Err(_) if context.is_stopping() => {}
                        Err(e) => return Err(e.into()),
                    }
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()?
            .start()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let second = Application::builder()
            .service(
                ServiceSpec::new("second"),
                service_fn(|context| async move {
                    match context
                        .set_health(Health::Healthy)
                        .and_then(|()| context.ready())
                    {
                        Ok(()) => {}
                        Err(_) if context.is_stopping() => {}
                        Err(e) => return Err(e.into()),
                    }
                    context.stopping().await;
                    Ok::<(), BoxError>(())
                }),
            )
            .build()?
            .start()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let observer = first.observer();
        let other = second.observer();
        observer.ready(Duration::from_secs(2)).await?;
        other.ready(Duration::from_secs(2)).await?;
        assert!(matches!(
            first.shutdown().await?.as_ref(),
            ShutdownReport::Incomplete(_)
        ));
        assert!(matches!(
            observer.drain(Duration::from_millis(1)).await,
            DrainObservation::Pending(_)
        ));
        assert!(other.snapshot().ready);
        Ok::<_, BoxError>((first, observer, second, other))
    })?;
    // The same host keeps the runtime and observer while the application is pending.
    let _ = release.send(());
    runtime.block_on(async {
        assert!(observer.wait().await?.is_success());
        drop(first);
        drop(second); // Last control lease requests stop; observer remains usable.
        assert!(other.wait().await?.is_success());
        Ok::<(), BoxError>(())
    })?;
    println!("both applications drained before the host drops its runtime");
    Ok(())
}
