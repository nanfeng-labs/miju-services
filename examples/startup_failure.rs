//! A failed initialization still owns its registered resource cleanup.

use miju_services::{Application, BoxError, ServiceSpec, service_fn};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let app = Application::builder()
        .service(
            ServiceSpec::new("storage"),
            service_fn(|context| async move {
                context.defer_cleanup("close partially initialized storage", || async {
                    println!("storage cleanup completed");
                    Ok::<(), BoxError>(())
                })?;
                Err::<(), BoxError>(
                    std::io::Error::other("demonstration initialization failure").into(),
                )
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
    println!("initialization: {ready:?}\nshutdown: {budget:?}\nactual exit: {exit:?}");
    let report = exit?;
    assert!(!report.is_success());
    assert_eq!(report.cleanup.len(), 1);
    Ok(())
}
