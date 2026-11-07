<p align="center">
  <img src="https://raw.githubusercontent.com/nanfeng-labs/miju-services/main/assets/miju-services-header.svg" alt="miju-services" width="560" />
</p>

<h1 align="center">miju-services</h1>

<div align="center">
  <strong>Application assembly, service lifecycles, and owned work for Rust</strong>
</div>

<br />

<div align="center">
  <a href="https://crates.io/crates/miju-services">
    <img src="https://img.shields.io/crates/v/miju-services.svg?label=published" alt="Published crate version" />
  </a>
  <a href="https://docs.rs/miju-services">
    <img src="https://img.shields.io/badge/docs-published-blue.svg" alt="Published API documentation" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml">
    <img src="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/MSRV-1.89-orange.svg?logo=rust" alt="Rust 1.89 or newer" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/nanfeng-labs/miju-services" alt="MIT license" />
  </a>
</div>

<br />

<div align="center">
  English · <a href="README.zh-CN.md">简体中文</a>
</div>

<br />

<div align="center">
  <a href="#features">Features</a> ·
  <a href="#installation">Installation</a> ·
  <a href="#usage">Usage</a> ·
  <a href="#examples">Examples</a> ·
  <a href="#how-miju-services-works">Lifecycle</a> ·
  <a href="#cargo-features">Cargo features</a>
</div>

<br />

A Rust service foundation built on [miju](https://github.com/nanfeng-labs/miju). Assemble application services, observe readiness and health, and wait for background work and resource cleanup. Ordinary Rust applications own their Tokio runtime; the optional JNI host owns one runtime per generation. Business configuration, resources, and logging remain application responsibilities.

## Features

- **Application assembly**: register services in one place and validate their dependencies.
- **Readiness and health**: confirm initialization and observe changing availability.
- **Owned work**: bound and track asynchronous, blocking, and externally aggregated work.
- **Shutdown and cleanup**: drain consumers before providers and run named cleanup actions after work releases.
- **miju integration**: attach Workers and Monitors, with miju-cron demonstrated as a calendar backend.
- **Direct periodic operations**: run one asynchronous round with fixed-delay or fixed-grid-skip timing.
- **Optional JNI hosting**: export five Java methods and retain one runtime per application generation.

## Installation

Requires **Rust 1.89+** and a Tokio runtime with time enabled.

The examples below use the `0.2.0-beta.1` API. For breaking changes from `0.1.0-beta`, see the [changelog](https://github.com/nanfeng-labs/miju-services/blob/v0.2.0-beta.1/CHANGELOG.md).

```toml
[dependencies]
miju-services = "0.2.0-beta.1"
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

The library does not install signal handlers. The example application enables Tokio's `signal` feature. A multithreaded `#[tokio::main]` also needs `rt-multi-thread`.

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

## miju and periodic operations

Keep the worker's backend, middleware, and job function in miju. The factory receives the context used for both construction and lifecycle control:

```rust,no_run
# #[cfg(feature = "miju")]
# async fn worker_example() -> Result<(), miju_services::BoxError> {
use miju::prelude::{MemoryStorage, WorkerBuilder};
use miju_services::{App, miju::worker_from};

let jobs = MemoryStorage::new();
App::builder()
    .service("devices", worker_from(move |context| {
        WorkerBuilder::new(context)
            .backend(jobs)
            .build(|device: String| async move { println!("Read {device}"); })
            .stream()
    }))
    .build()?
    .run_until(tokio::signal::ctrl_c())
    .await?;
# Ok(())
# }
```

Use the supplied context, rather than creating another worker context. Start establishes the worker driver, not remote backend readiness. The bridge keeps polling the original stream after stop; job errors remain observable in snapshots. Backend failure or Monitor restart history leaves aggregate responsibility unconfirmed and prevents cleanup from being certified. A Monitor is one grouped service: its worker failures may be delayed by resident siblings. See [worker](examples/miju_worker.rs) and [monitor](examples/miju_monitor.rs).

For a simple polling operation, no queue is needed:

```rust
# #[cfg(feature = "periodic")]
# fn periodic_example() -> Result<(), miju_services::BoxError> {
use miju_services::{App, periodic};
use std::time::Duration;

let app = App::builder()
    .service("device-status", periodic::fixed_delay(Duration::from_secs(5), || async {
        println!("Collect device status");
        Ok(())
    })?)
    .build()?;
# let _ = app;
# Ok(())
# }
```

`fixed_delay` waits after completion. `fixed_rate_skip` uses fixed slots and skips busy or missed opportunities. If a round starts at second 0 and finishes at second 8 with a five-second period, the next start is second 13 or second 10, respectively. Defaults are immediate first execution, serial rounds, and recorded business errors followed by the next period; there is no hidden retry. Use `PeriodicRule` and `RoundContext` for a delayed first round, concrete errors, cooperative stopping, or failure escalation.

Calendar work uses [miju-cron directly as a backend](examples/cron_worker.rs). The advanced [bounded queue example](examples/bounded_queue_rounds.rs) adds application-owned admission, target overlap policy, and final receipts around miju execution. Its capacity covers queued plus running work; stop finishes taken jobs and cancels the rest. Receipts wrap the full configured retry chain. `MemoryStorage` itself is unbounded; limiting worker concurrency alone does not bound backlog.

## Examples

Protocols use the same App entry point. Supply a Router and register the example-local `HttpService::new(address, router)`. It reports ready after binding and waits for the framework, Router, request futures, and response bodies to release. WebSocket reserves a session before returning 101 and returns 503 at capacity; its upgrade callback also holds the HTTP release barrier until it transfers or drops the socket. Sessions use a bounded outgoing queue and a two-second close budget. UDP drops overload without queuing and drains accepted datagrams. Separately retained resources and detached tasks remain application responsibilities.

Run `cargo run --example http` for `127.0.0.1:3000`, `websocket` for `ws://127.0.0.1:3001/ws`, or `udp` for UDP `127.0.0.1:4000`. Ctrl-C requests shutdown and waits for actual cleanup. Run `embedded_runtime` to observe Pending while retaining the runtime until completion. Protocol adapters live in `examples/support` and are not public library API.

| Example | Focus |
| --- | --- |
| [basic](examples/basic.rs) | Register a function and run until Ctrl-C. |
| [shared_resource](examples/shared_resource.rs) | Typed settings, constructor injection, cleanup, and bootstrap rollback. |
| [configured_app](examples/configured_app/README.md) | Multiple configuration sources, typed settings, centralized assembly, and file cleanup. |
| [watched_settings](examples/watched_settings.rs) | Immutable per-round snapshots and application-owned configuration updates. |
| [startup_failure](examples/startup_failure.rs) | Required initialization failure exits without a host signal. |
| [managed_work](examples/managed_work.rs) | Finite prerequisites and service-owned background work. |
| [operational_host](examples/operational_host.rs) | Observe failure from startup and choose an explicit second-signal policy. |
| [miju_worker](examples/miju_worker.rs) | Construct a real miju worker once and manage its lifecycle. |
| [miju_monitor](examples/miju_monitor.rs) | Attach a group with upstream restart and aggregate-result semantics. |
| [periodic](examples/periodic.rs) | A period and one asynchronous operation, without a queue. |
| [cron_worker](examples/cron_worker.rs) | Calendar scheduling directly through a miju-cron backend. |
| [bounded_queue_rounds](examples/bounded_queue_rounds.rs) | Advanced bounded admission, overlap, final receipts, and queued-job cancellation. |
| [http](examples/http.rs) | Supply business routes, bind before ready, and wait for actual release. |
| [websocket](examples/websocket.rs) | Admit before 101, retain upgrade responsibility, and bound session close. |
| [udp](examples/udp.rs) | Drop overload and drain accepted datagrams. |
| [embedded_runtime](examples/embedded_runtime.rs) | Retain the runtime after Pending until cleanup actually completes. |

## How miju-services works

```mermaid
flowchart LR
    app["App<br/>Register services and cleanup"] --> run["Start and run<br/>Initialize, ready, observe health"]
    run --> drain["Stop and drain<br/>Respect dependencies and owned work"]
    drain --> report["Cleanup and report<br/>Release resources, record outcome"]
    classDef assembly fill:#fff7ed,stroke:#f97316,color:#7c2d12
    classDef operation fill:#ecfdf5,stroke:#10b981,color:#064e3b
    classDef ending fill:#eff6ff,stroke:#3b82f6,color:#172554
    class app assembly
    class run operation
    class drain,report ending
```

On cooperative shutdown, work must actually release before cleanup and the final report. Budget expiry keeps unfinished responsibility owned.

### Core concepts

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

For multiple configuration sources, see [configured_app](examples/configured_app/README.md). It loads Default → explicit TOML → application environment → CLI, explains the effective sources, then assembles a file-backed sampler. Settings are validated before opening resources. The separate [watch example](examples/watched_settings.rs) shows one immutable configuration snapshot per round. Both stay outside the library's normal dependency tree.

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

## Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `miju` | Yes | Worker and Monitor lifecycle integration. |
| `periodic` | Yes | Serial fixed-delay and fixed-grid-skip operations. |
| `jni` | No | Five-method Java bridge and one owned runtime per generation. |
| `tracing` | No | Lifecycle events without installing a subscriber. |
| `docsrs` | No | Generate the feature reference in rustdoc. |

Default features are `miju` and `periodic`; either can be selected independently. `default-features = false` retains the complete ordinary lifecycle without miju. The miju dependency disables its own defaults; applications select their middleware features. `tracing` emits lifecycle events without installing a subscriber. `docsrs` generates feature documentation. Configuration, cron, queue helpers, protocols, and signal handling remain application choices.

## JNI hosting

Ordinary Rust applications keep their own Tokio runtime. With the default-off `jni` feature, implement `jni::AppFactory` to return an unstarted `App` and invoke `jni::export!` once for five Java static methods. The JNI owner retains one runtime per generation until work, cleanup and user references have actually released. A Java stop timeout ends only that observation.

The unstarted App returned by the factory must be safe to drop synchronously. Acquire async-close resources inside service initialization and register cleanup before ready; builder cleanup is not pre-start rollback. See the [JNI example](examples/jni/README.md) for Rust assembly, Java declarations and a real UDP caller.

## Development

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI runs separate check, test, format, Clippy, documentation, and unused-dependency jobs, with Linux/macOS/Windows test and Rust 1.89 coverage.

## License

miju-services is licensed under the [MIT License](https://github.com/nanfeng-labs/miju-services/blob/main/LICENSE).
