//! Pass watch snapshots to a service without adding a configuration handle framework.

#[path = "support/watched_settings.rs"]
mod watched_settings;

use miju_services::{App, BoxError};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use watched_settings::Settings;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let (updates, settings) = watch::channel(Arc::new(Settings {
        interval: Duration::from_millis(500),
        message: "initial settings".into(),
    }));
    let app = App::builder()
        .service_fn("printer", move |context| {
            watched_settings::run(context, settings, |snapshot| async move {
                println!("{}", snapshot.message);
                Ok(())
            })
        })
        .build()?;

    let report = app
        .run_until(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            updates.send(Arc::new(Settings {
                interval: Duration::from_millis(250),
                message: "updated settings".into(),
            }))?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(updates); // The service keeps the last settings until the host stops it.
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<(), BoxError>(())
        })
        .await?;
    println!("{report:?}");
    Ok(())
}
