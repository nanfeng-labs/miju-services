<!-- Also rendered as crate documentation: keep navigation file links absolute and pinned to this release. -->

<p align="center">
  <img src="https://raw.githubusercontent.com/nanfeng-labs/miju-services/v0.3.0/assets/miju-services-header.svg" alt="miju-services" width="560" />
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
  <a href="https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/LICENSE">
    <img src="https://img.shields.io/github/license/nanfeng-labs/miju-services" alt="MIT license" />
  </a>
</div>

<br />

<div align="center">
  English · <a href="https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/README.zh-CN.md">简体中文</a>
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

A Rust service foundation built on [miju](https://github.com/nanfeng-labs/miju). Assemble application services, observe readiness and health, and wait until background work and resource cleanup have actually finished. Ordinary Rust applications own their Tokio runtime; the optional JNI host owns one runtime per generation. Business configuration, resources, and logging remain application responsibilities.

## Features

- **Application assembly**: register services in one place; names, dependencies and each service's own settings are checked before anything runs.
- **Readiness and health**: confirm initialization and observe changing availability.
- **Owned work**: bounded asynchronous and blocking work that returns a value, and leases for execution the library does not run.
- **Shutdown and cleanup**: drain consumers before providers, give each service its own grace period, and run cleanup only after everything has released.
- **Resources**: acquire a value after start, register its close where it was acquired, and hand it to the services that declared the dependency.
- **miju integration**: attach Workers and Monitors, with miju-cron demonstrated as a calendar backend.
- **Periodic operations**: run one asynchronous round on a fixed delay or a fixed grid.
- **Optional JNI hosting**: export five Java methods and retain one runtime per application generation.

## Installation

Requires **Rust 1.89+** and a Tokio runtime with time enabled.

The examples below use the `0.3.0` API. 0.3 is not source-compatible with 0.2; the [changelog](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/CHANGELOG.md) has a migration table.

```toml
[dependencies]
miju-services = "0.3.0"
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

The library does not install signal handlers. The example application enables Tokio's `signal` feature. A multithreaded `#[tokio::main]` also needs `rt-multi-thread`.

## Usage

Register a function, run until the host requests shutdown, and receive the final report.

```rust,no_run
use miju_services::{App, Context, Result};
use std::time::Duration;

async fn heartbeat(ctx: Context) -> Result {
    ctx.ready();
    loop {
        tokio::select! {
            _ = ctx.stopping() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => println!("heartbeat"),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service("heartbeat", heartbeat);
    // Returns after the service has stopped and everything it owned is released.
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
```

Run the same assembly with `cargo run --example basic`. Ctrl-C requests shutdown; `run_until` returns after services, accepted work, and registered cleanup have finished, with no deadline. A required service failure also ends the application without waiting for Ctrl-C. For a bounded wait or a second-signal exit policy, see [Embedded control](#embedded-control).

## miju and periodic operations

Keep the worker's backend, middleware, and job function in miju. The factory receives a `Seat`, and the worker is built on it:

```rust,no_run
# #[cfg(feature = "miju")]
# async fn worker_example() -> miju_services::Result {
use miju::prelude::{MemoryStorage, WorkerBuilder};
use miju_services::{App, BoxError, miju::worker};

async fn read_device(device: String) -> Result<(), BoxError> {
    println!("read {device}");
    Ok(())
}

let jobs = MemoryStorage::new();
let mut app = App::new();
app.service("devices", worker(move |seat| {
    WorkerBuilder::new(&seat)
        .backend(jobs)
        .build(read_device)
        .stream()
}));
app.run_until(tokio::signal::ctrl_c()).await?;
# Ok(())
# }
```

The service is ready at the worker's real `Start`, which does not mean a remote backend is ready. Jobs run through a managed executor, so the service drains only after every handler has finished. A backend failure fails the service; job errors are counted in `MijuStats` and do not. A Monitor is one grouped service whose restarts are recorded without failing the report by themselves. If a worker or monitor is still running handlers when its grace period expires, they are cut off: a durable backend redelivers the task after its orphan timeout, a memory or cron backend loses it, and the report says the service was forced. See [worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_worker.rs) and [monitor](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_monitor.rs).

For a simple polling operation, no queue is needed:

```rust
# #[cfg(feature = "periodic")]
# fn periodic_example() {
use miju_services::{App, periodic};
use std::time::Duration;

let mut app = App::new();
app.service("device-status", periodic::every(Duration::from_secs(5), || async {
    println!("collect device status");
    Ok(())
}));
# }
```

`every` waits one period after each round finishes; `.on_grid()` uses fixed slots and skips the ones a long round missed. If a round starts at second 0 and finishes at second 8 with a five-second period, the next start is second 13 or second 10, respectively. Rounds run one at a time inside the service, the first runs at once (`.first_after_period()` delays it), and a round's error is recorded before the next period (`.stop_on_error()` fails the service instead). There is no hidden retry. `every_with` passes each round a `Round` and keeps a concrete error type.

Calendar work uses [miju-cron directly as a backend](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/cron_worker.rs). The advanced [bounded queue example](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/bounded_queue_rounds.rs) adds application-owned admission, target overlap policy, and final receipts around miju execution. Its capacity covers queued plus running work; stop finishes taken jobs and cancels the rest. `MemoryStorage` itself is unbounded; limiting worker concurrency alone does not bound backlog.

## Examples

Protocols use the same entry point. The example-local `HttpService::new(address, router)` reports ready after binding, then takes two leases: one per connection, and one per request that moves into the response body. The service drains only when the last handler, router clone and body has been dropped, and a request that cannot get a lease is answered 503 before its handler runs. WebSocket reserves a session before answering 101, with its own session limit (503 when it is reached); sessions use a bounded outgoing queue and a two-second close budget. UDP drops overload without queuing and drains accepted datagrams. Separately retained resources and tasks the application detaches itself remain its responsibility.

Run `cargo run --example http` for `127.0.0.1:3000`, `websocket` for `ws://127.0.0.1:3001/ws`, or `udp` for UDP `127.0.0.1:4000`. Ctrl-C requests shutdown and waits for actual cleanup. Protocol adapters live in `examples/support` and are not public library API.

| Example | Focus |
| --- | --- |
| [basic](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/basic.rs) | Register a function and run until Ctrl-C. |
| [shared_resource](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/shared_resource.rs) | Typed settings, a concrete `Service`, and a pre-built store handed over as a resource. |
| [configured_app](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/configured_app/README.md) | Multiple configuration sources, typed settings, centralized assembly, and file cleanup. |
| [watched_settings](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/watched_settings.rs) | Immutable per-round snapshots and application-owned configuration updates. |
| [startup_failure](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/startup_failure.rs) | Required initialization failure exits without a host signal. |
| [managed_work](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/managed_work.rs) | Finite prerequisites and service-owned background work. |
| [operational_host](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/operational_host.rs) | Observe failure from startup and choose an explicit second-signal policy. |
| [embedded_runtime](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/embedded_runtime.rs) | A bounded wait, what is still owned, and the actual completion afterwards. |
| [miju_worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_worker.rs) | Construct a real miju worker once and manage its lifecycle. |
| [miju_monitor](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_monitor.rs) | Attach a group with upstream restart and aggregate-result semantics. |
| [periodic](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/periodic.rs) | A period and one asynchronous operation, without a queue. |
| [cron_worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/cron_worker.rs) | Calendar scheduling directly through a miju-cron backend. |
| [bounded_queue_rounds](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/bounded_queue_rounds.rs) | Advanced bounded admission, overlap, final receipts, and queued-job cancellation. |
| [http](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/http.rs) | Supply business routes, bind before ready, and wait for actual release. |
| [websocket](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/websocket.rs) | Admit before 101, retain upgrade responsibility, and bound session close. |
| [udp](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/udp.rs) | Drop overload and drain accepted datagrams. |

## How miju-services works

```mermaid
flowchart LR
    app["App<br/>Register services and resources"] --> run["Start and run<br/>Initialize, ready, observe health"]
    run --> drain["Stop and drain<br/>Consumers first, each with its own grace"]
    drain --> report["Cleanup and report<br/>Release resources, record outcome"]
    classDef assembly fill:#fff7ed,stroke:#f97316,color:#7c2d12
    classDef operation fill:#ecfdf5,stroke:#10b981,color:#064e3b
    classDef ending fill:#eff6ff,stroke:#3b82f6,color:#172554
    class app assembly
    class run operation
    class drain,report ending
```

A report exists only after every service has drained and every cleanup action has finished. Whether that went well is a separate question: `report.is_success()`.

### Core concepts

- **App** collects registrations, checks them, and runs one lifecycle driver that owns every start and stop decision.
- **Service** is an `async fn(Context) -> Result`, a closure, or a type implementing `Service` (to keep a concrete error or check its settings at assembly). A `Spec` sets dependencies, optional or finite, and work capacity.
- **Context** is what a service uses to talk to its lifecycle: `ready()`, `set_health()`, `stopping()`, `spawn()`, `lease()`, `defer()`.
- **Scope** is a service's admission gate and release barrier. Work and leases count against it; the service has drained when its root, its work and its leases are all gone.
- **Resource** is a value one service acquires after start and others read, tied to the dependency edge.
- **Cleanup** actions run after every service has drained: consumers' before providers', then most recent first.

### Assembly and resources

Pass ordinary values, `Arc`s, or existing watch handles to your service constructor. A value that needs closing becomes a resource: it is acquired after start is accepted, and its close is registered by the code that acquired it.

```rust
# use miju_services::{App, BoxError, Spec};
# use std::sync::Arc;
# #[derive(Default)]
# struct Store;
# impl Store {
#     async fn open() -> Result<Self, BoxError> { Ok(Self) }
#     async fn close(&self) -> Result<(), BoxError> { Ok(()) }
# }
let mut app = App::new();
let store = app.resource("store", |ctx| async move {
    let store = Arc::new(Store::open().await?);
    let closing = store.clone();
    ctx.defer("store", async move { closing.close().await })?;
    Ok(store)
});
app.service_fn(Spec::new("counter").after(&store), move |ctx| async move {
    let _store = store.get(&ctx)?;
    ctx.ready();
    ctx.stopping().await;
    Ok(())
});
```

Nothing is acquired before start, so a refused start has nothing to roll back, and `StartError::into_app()` returns the unstarted `App`. A store built before the `App` can be handed over the same way: the [shared resource example](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/shared_resource.rs) registers its close when the resource runs, and until then the caller is still responsible for it. A service that forgets `.after(&store)` gets an error from `get` every time, whatever order services happened to start in.

For multiple configuration sources, see [configured_app](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/configured_app/README.md). It loads Default → explicit TOML → application environment → CLI, explains the effective sources, then assembles a file-backed sampler that opens its output after start and registers the close before readiness. The separate [watch example](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/watched_settings.rs) shows one immutable configuration snapshot per round. Both stay outside the library's dependency tree.

### Readiness and health

Call `ctx.ready()` after real initialization, such as a successful listener bind. It latches initialization and defaults undeclared health to `Healthy`; an explicit `Unknown`, `Degraded`, or `Unhealthy` is preserved. The application is ready when every required service is initialized and healthy. Calls after stop, or after the root has returned, are ignored.

Services are required and resident by default. A required failure, panic, readiness timeout (30 seconds by default, `app.startup_timeout(d)`), or an unexpected return of a resident root stops the application. A finite service (`Spec::finite()`) is a one-off step: it counts as initialized once it has succeeded and drained, and has no startup timeout. An optional service's failure stops and drains only that service. There is no automatic restart.

### Managed work and stopping

`ctx.spawn(name, future)` and `ctx.spawn_blocking(name, closure)` admit work against the service's capacity (1024 by default), or reject it without running it. The returned `Work<T>` yields the value; dropping it leaves the work running and owned by the service. Wrap the name in `critical(..)` to make a failure of that work fail the service. For execution the library does not run, such as a framework's connection task, take a `ctx.lease(name)` and move it into that execution; the service cannot drain while the lease exists.

Stop is cooperative. A service is asked to stop only after its consumers have drained, and its grace period (30 seconds by default, `app.shutdown_grace(d)`) starts then, so every provider gets a full window. When it expires, the root and its asynchronous work are aborted and the report marks the service as forced. Blocking work that has started cannot be interrupted; the service, and the report, wait for it to return. Work queued but not yet started when stop arrives is cancelled without running.

Snapshots are observations: per service they are coherent, across services they are not an atomic cut while the application is running. The final report is.

### Embedded control

Use `start()` when the host needs a handle:

```rust
# use miju_services::{App, BoxError};
# use std::time::Duration;
# fn main() -> Result<(), BoxError> {
# let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
# runtime.block_on(async {
let mut app = App::new();
app.service_fn("service", |ctx| async move {
    ctx.ready();
    ctx.stopping().await;
    Ok(())
});
let handle = app.start()?;
tokio::time::timeout(Duration::from_secs(1), handle.ready()).await??;
let report = handle.shutdown().await?;
assert!(report.is_success());
# Ok::<(), BoxError>(())
# })?;
# Ok(())
# }
```

| Operation | Meaning |
| --- | --- |
| `ready()` | Resolves when the application is ready, or with why it will not be. |
| `status()` / `changed()` | Phase, readiness and health now, or at the next change. |
| `snapshot()` | Every service and what it still owns, assembled now. |
| `stop()` | Request dependency-ordered stop. Repeated calls change nothing. |
| `wait()` | The final report at actual completion, or `Lost` if the driver is gone. |
| `shutdown()` | `stop()` then `wait()`. |
| `observer()` | A handle that observes without keeping the application running. |

None of these has a deadline. Bound any of them with `tokio::time::timeout`: an elapsed timeout ends that observation and nothing else. After a timed-out `wait()`, `snapshot().outstanding()` lists what is still held. Dropping the last `AppHandle` requests stop. Keep the runtime alive until the report; if it goes away first, `wait()` returns `Lost`, never a report.

`start()` refuses, and hands the `App` back, when there is no current Tokio runtime or the runtime has no time driver. `run_until` keeps the stop source's error alongside the final report. Returning from `#[tokio::main]` can still wait for blocking work that has started; the application owns its runtime and its process-exit policy, as the [operational host example](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/operational_host.rs) shows with an explicit second-signal exit.

## Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `miju` | Yes | Worker and Monitor lifecycle integration. |
| `periodic` | Yes | Fixed-delay and fixed-grid operations run inside a service. |
| `jni` | No | Five-method Java bridge and one owned runtime per generation. |
| `tracing` | No | Lifecycle events without installing a subscriber. |
| `docsrs` | No | Generate the feature reference in rustdoc. |

`default-features = false` keeps the complete lifecycle without miju. The miju dependency disables its own defaults; applications select their middleware features. With `tracing`, the driver emits a warning for each recorded service failure, an expired grace period and a failed cleanup action, an error when the driver is lost, and info events when stopping begins and the application terminates. Configuration, cron, queue helpers, protocols, and signal handling remain application choices.

## JNI hosting

With the default-off `jni` feature, implement `jni::AppFactory` to return an unstarted `App` and invoke `jni::export!` once for five Java static methods. The Java declarations and every numeric code are the same as in 0.2. The JNI owner retains one runtime per generation until work, cleanup and user references have actually released; a Java stop timeout (`-124`) ends only that observation.

The factory's `App` must be safe to drop synchronously, which it is when resources are acquired after start and closed with `ctx.defer`. See the [JNI example](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/jni/README.md) for Rust assembly, Java declarations and a real UDP caller.

## Development

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI runs separate check, test, format, Clippy, documentation, and unused-dependency jobs, with Linux/macOS/Windows test and Rust 1.89 coverage, and real JVM runs on Linux and macOS. Windows desktop JNI was [accepted for 0.2.0-beta.5](https://github.com/nanfeng-labs/miju-services/releases/download/v0.2.0-beta.5/BETA5-WINDOWS-ACCEPTANCE.md); that evidence covers that source revision only. Android ART and target business-application acceptance remain separate.

## License

miju-services is licensed under the [MIT License](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/LICENSE).
