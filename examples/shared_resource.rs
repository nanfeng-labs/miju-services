//! Pass settings and a shared resource to a service, and close the resource
//! after everything that uses it has released.

#[path = "support/shared_resource.rs"]
mod shared_resource;

use miju_services::Result;
use shared_resource::{CounterStore, Settings};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let store = Arc::new(CounterStore::default());
    let report = shared_resource::assemble(Settings::default(), store.clone())
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("count: {}, closed: {}", store.count(), store.is_closed());
    println!("{report}");
    Ok(())
}
