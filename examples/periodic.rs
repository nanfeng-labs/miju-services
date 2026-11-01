//! A direct serial sampler, with no queue or target registry.

use miju_services::{
    Application, BoxError, ServiceSpec,
    periodic::{PeriodicMode, PeriodicRule, periodic},
};
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let (send, mut receive) = mpsc::unbounded_channel();
    let sampler = periodic(
        PeriodicRule::new(Duration::from_millis(20), PeriodicMode::FixedDelay)?,
        move |round| {
            let send = send.clone();
            async move {
                println!("sample {}", round.round_id());
                send.send(round.round_id())?;
                Ok::<(), BoxError>(())
            }
        },
    );
    let app = Application::builder()
        .service(ServiceSpec::new("sampler"), sampler)
        .build()?
        .start()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let observer = app.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    let samples = async {
        if ready.is_ok() {
            for _ in 0..3 {
                receive
                    .recv()
                    .await
                    .ok_or_else(|| std::io::Error::other("sampler stopped"))?;
            }
        }
        Ok::<(), BoxError>(())
    }
    .await;
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    println!("{budget:?}\n{exit:?}");
    ready?;
    samples?;
    budget?;
    if !exit?.is_success() {
        return Err(std::io::Error::other("periodic lifecycle failed").into());
    }
    Ok(())
}
