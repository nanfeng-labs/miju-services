//! Explicit resource ownership, readiness, dependent work, and final cleanup.

use miju_services::{Application, BoxError, Health, Lifetime, ServiceSpec, WorkSpec, service_fn};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let cleanup_events = events.clone();
    let app = Application::builder()
        .service(
            ServiceSpec::new("storage").lifetime(Lifetime::Finite),
            service_fn(move |context| async move {
                let close = cleanup_events.clone();
                context.defer_cleanup("close", move || async move {
                    close.lock().unwrap().push("close");
                    Ok::<(), BoxError>(())
                })?;
                context.defer_cleanup("flush", move || async move {
                    cleanup_events.lock().unwrap().push("flush");
                    Ok::<(), BoxError>(())
                })?;
                Ok::<(), BoxError>(())
            }),
        )
        .service(
            ServiceSpec::new("consumer").depends_on("storage"),
            service_fn(|context| async move {
                match context
                    .set_health(Health::Healthy)
                    .and_then(|()| context.ready())
                {
                    Ok(()) => {}
                    Err(_) if context.is_stopping() => {}
                    Err(error) => return Err(error.into()),
                }
                if !context.is_stopping() {
                    match context
                        .work()
                        .spawn(WorkSpec::new("sample"), |work| async move {
                            work.stopping().await;
                            Ok::<(), BoxError>(())
                        }) {
                        Ok(_) => {}
                        Err(_) if context.is_stopping() => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                context.stopping().await;
                context.work().close();
                context.work().wait().await;
                Ok::<(), BoxError>(())
            }),
        )
        .build()?;
    let control = app
        .start()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let observer = control.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    let budget = control.shutdown().await;
    let exit = observer.wait().await;
    println!(
        "budget: {budget:?}\nexit: {exit:?}\ncleanup: {:?}",
        events.lock().unwrap()
    );
    ready?;
    budget?;
    let report = exit?;
    if !report.is_success() {
        return Err(std::io::Error::other("application failed; inspect report").into());
    }
    Ok(())
}
