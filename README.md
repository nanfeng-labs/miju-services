<h1 align="center">miju-services</h1>

<p align="center">
  <strong>Assemble services, observe readiness, and wait for real cleanup.</strong>
</p>

<p align="center">
  <a href="README.zh-CN.md">简体中文</a> ·
  <a href="https://github.com/nanfeng-labs/miju">miju</a> ·
  <a href="LICENSE">MIT</a> · Rust 1.89+
</p>

`miju-services` is a Rust service foundation for [miju](https://github.com/nanfeng-labs/miju). It gives applications one place to assemble ordinary services, observe readiness and health, and wait for actual work and resource cleanup. The application owns its Tokio runtime, business configuration, resources, and logging setup.

This unpublished `0.2` checkout implements ordinary lifecycles. The published `0.1.0-beta` has a different API. Other integrations follow in later batches.

## Usage

Register a function, run until the host requests shutdown, and receive the final report:

```rust,no_run
use miju_services::{App, BoxError, ServiceContext};
use std::time::Duration;

async fn heartbeat(context: ServiceContext) -> Result<(), BoxError> {
    context.ready();
    loop {
        tokio::select! {
            _ = context.stopping() => break,
            _ = tokio::time::sleep(Duration::from_secs(5)) => println!("heartbeat"),
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = App::builder()
        .service_fn("heartbeat", heartbeat)
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("{report:?}");
    Ok(())
}
```

Run the same assembly with `cargo run --example basic`. Ctrl-C requests shutdown; the function returns after services, accepted work, and registered cleanup have finished. A required service failure also ends the application without waiting for Ctrl-C.

For this unreleased checkout, use a path dependency:

```toml
[dependencies]
miju-services = { path = "../miju-services" }
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

The library does not install signal handlers. The example application enables Tokio's `signal` feature. A multithreaded `#[tokio::main]` also needs `rt-multi-thread`.

## Core concepts

- **App** collects service registrations, validates dependencies, and runs the lifecycle driver.
- **Service** owns its settings and dependencies. Use `service_fn` for an asynchronous function or `service` for a type implementing `Service`.
- **WorkScope** tracks background work owned by a service. Accepted work remains tracked when its handle or waiter is dropped.
- **Cleanup** runs named asynchronous actions in reverse registration order after service roots and their work have actually released responsibility.

### Assembly and resources

Pass ordinary values, `Arc`s, or existing watch handles to your service constructor. The core does not look up resources by type or load business configuration.

```rust
# use miju_services::{App, BoxError};
# use std::sync::Arc;
# #[derive(Default)]
# struct Store;
# impl Store { async fn close(&self) -> Result<(), BoxError> { Ok(()) } }
# fn main() -> Result<(), BoxError> {
let store = Arc::new(Store::default());
let cleanup = store.clone();
let app = App::builder()
    .cleanup("store", move || async move { cleanup.close().await })
    .service_fn("counter", move |context| async move {
        let _store = store;
        context.ready();
        context.stopping().await;
        Ok(())
    })
    .build()?;
# let _ = app;
# Ok(())
# }
```

Before `start` succeeds, bootstrap owns rollback. Keep independent access to resources needed for rollback: dropping a builder or an unstarted `App` cannot run asynchronous cleanup. `StartError::into_app()` recovers an app rejected because no Tokio runtime is active. After acceptance, the driver owns registered cleanup, including partial initialization failures. The [shared resource example](examples/shared_resource.rs) includes that application boundary and a concrete `Service` implementation.


### Readiness and health

Call `context.ready()` after real initialization, such as a successful listener bind. It latches initialization and defaults undeclared health to `Healthy`; an explicit `Unknown`, `Degraded`, or `Unhealthy` is preserved. Later health updates affect availability. Notifications after local stopping or root release are ignored.

Default services are required and resident. A required failure, panic, Resident readiness timeout, or unexpected resident return requests global shutdown. Finite services are one-shot business steps; they initialize dependencies only after successful completion and actual drain, and are not limited by the Resident startup timeout. Optional failure stops and drains that service locally. There is no automatic restart. Use `ServiceSpec` only when you need dependencies, optional services, finite steps, or a different work capacity.

### Managed work and stopping

Use `context.work().spawn(...)` or `spawn_blocking(...)` for background work. Admission is bounded; a rejected factory is never called. Root return closes admission and the driver drains accepted work. A dependency provider remains available until its consumers' roots and work have released responsibility.

Ordinary asynchronous work can be aborted after the shutdown grace period. Started blocking work must finish cooperatively. `WorkSpec::aggregate` covers external execution through an explicit completion contract: only a normal `Ok(())` confirms completion; an error or panic leaves external responsibility unconfirmed. Do not use it for an ordinary task.

Grace expiry requests eligible aborts. It does not prove completion or allow cleanup to overtake work. A pending blocking operation, unconfirmed external work, or unfinished cleanup keeps the driver alive and prevents a final report.

### Embedded control

Use `build()?.start()?` when the host needs a control handle:

```rust
# use miju_services::{App, BoxError};
# use std::time::Duration;
# fn main() -> Result<(), BoxError> {
# let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
# runtime.block_on(async {
let handle = App::builder()
    .service_fn("service", |context| async move {
        context.ready();
        context.stopping().await;
        Ok(())
    })
    .build()?
    .start()?;
handle.ready(Duration::from_secs(1)).await?;
let report = handle.shutdown().await?;
assert!(report.is_success());
# Ok::<(), BoxError>(())
# })?;
# Ok(())
# }
```

| Operation | Meaning |
| --- | --- |
| `ready(within)` | Wait for current application readiness; timeout only ends observation. |
| `snapshot()` | Read the current phase, health, and outstanding responsibility. |
| `stop()` | Request shutdown once; repeated calls do not renew its budget. |
| `wait()` | Observe the actual final report, including unsuccessful outcomes. |
| `shutdown()` | Request shutdown, wait for actual completion, and classify failures. |
| `shutdown_within(within)` | Return `Complete(report)` or `Pending(snapshot)` within this observation window. |

`Complete` means actual completion; inspect `report.is_success()` for outcome. Canceling an observation does not cancel the driver. Dropping the last control handle requests shutdown; observer and work handles do not keep a control lease. Keep the caller's runtime alive until completion. Losing that runtime can produce `DriverLost`, never a cleanup certificate.

The runtime must also have Tokio's time driver enabled. `start()` checks that a runtime is active, but cannot establish this capability; without it, the lifecycle driver can panic on its first timer poll after services have been accepted. Service roots may then continue without lifecycle supervision, and cleanup or a final report is not guaranteed.

`run_until` retains errors from the stop source alongside the final report. Its terminal branch has priority when both terminal observation and the stop source are ready. A host that needs diagnostics and a second-signal force-exit policy can use the [operational host example](examples/operational_host.rs); force exit is a process decision and supplies no completion certificate.

## Examples

| Example | Focus |
| --- | --- |
| [basic](examples/basic.rs) | Register a function and run until Ctrl-C. |
| [shared_resource](examples/shared_resource.rs) | Typed settings, constructor injection, cleanup, and bootstrap rollback. |
| [startup_failure](examples/startup_failure.rs) | Required initialization failure exits without a host signal. |
| [managed_work](examples/managed_work.rs) | Finite prerequisites and service-owned background work. |
| [operational_host](examples/operational_host.rs) | Observe failure from startup and choose an explicit second-signal policy. |

## Features and development

Default features are empty. The ordinary lifecycle works independently; `tracing` only emits events and `docsrs` generates feature documentation. Applications own signal handling.

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI runs separate check, test, format, Clippy, documentation, and unused-dependency jobs, with Linux/macOS/Windows test and Rust 1.89 coverage. Workflow configuration does not claim that a hosted run has completed.
