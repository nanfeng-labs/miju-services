//! Pass application settings and a shared resource through a service constructor.

#[path = "support/shared_resource.rs"]
mod shared_resource;

use miju_services::BoxError;
use shared_resource::{CounterStore, Settings};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let store = Arc::new(CounterStore::default());
    let report =
        shared_resource::run(Settings::default(), store.clone(), tokio::signal::ctrl_c()).await?;
    println!("count: {}, closed: {}", store.count(), store.is_closed());
    println!("{report:?}");
    Ok(())
}
