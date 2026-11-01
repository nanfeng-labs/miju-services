//! Own a real miju worker while preserving its queued execution contract.

use miju_core::{
    backend::{TaskSink, memory::MemoryStorage},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_services::{Application, BoxError, ServiceSpec, miju::worker};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let mut backend = MemoryStorage::new();
    backend.push("collect-device-status".to_owned()).await?;
    let (send, receive) = oneshot::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(Some(send)));
    let context = WorkerContext::new("device-jobs");
    let jobs = WorkerBuilder::new(context.clone())
        .backend(backend)
        .build(move |job: String| {
            let send = send.clone();
            async move {
                println!("completed {job}");
                if let Some(send) = send.lock().unwrap().take() {
                    let _ = send.send(());
                }
                Ok::<(), std::io::Error>(())
            }
        });
    let app = Application::builder()
        .service(
            ServiceSpec::new("device-jobs"),
            worker(context, jobs.stream()),
        )
        .build()?
        .start()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let observer = app.observer();
    let ready = observer.ready(Duration::from_secs(5)).await;
    let work_result = if ready.is_ok() {
        receive.await.map_err(BoxError::from)
    } else {
        Ok(())
    };
    let budget = app.shutdown().await;
    let exit = observer.wait().await;
    println!("shutdown: {budget:?}; exit: {exit:?}");
    ready?;
    work_result?;
    budget?;
    let exit = exit?;
    if !exit.is_success() {
        return Err(std::io::Error::other("worker lifecycle failed; inspect report").into());
    }
    Ok(())
}
