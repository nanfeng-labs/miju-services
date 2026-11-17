# Changelog

## [Unreleased]

## [0.3.0]

- feat!: rebuild the core around scope ownership for v0.3.0 ([#27](https://github.com/nanfeng-labs/miju-services/pull/27))

- ci: append full changelog links to releases ([#25](https://github.com/nanfeng-labs/miju-services/pull/25))

0.3 rebuilds the core on one ownership model: every service has a scope that
counts its root, its work and its leases, and a single lifecycle driver makes
every start and stop decision. A report exists only after every scope has
drained and every cleanup action has finished. The Rust API is not
source-compatible with 0.2; the JNI Java declarations and codes are unchanged.

### Changed

- Assembly is `App::new()` plus `service`, `service_fn` and `resource` on
  `&mut App`; there is no builder and no `build()`. Settings are checked at
  `start()`, which refuses (returning the `App`) on an invalid assembly, without
  a Tokio runtime, or without its time driver.
- Work returns a typed value: `ctx.spawn(name, future)` and
  `ctx.spawn_blocking(name, closure)` give a `Work<T>`. Wrap the name in
  `critical(..)` for work whose failure fails the service.
- Readiness and shutdown waits have no built-in deadline; compose them with
  `tokio::time::timeout`. A small `Status` is pushed on lifecycle transitions,
  and snapshots are assembled on demand.
- `periodic::every` and `.on_grid()` replace `fixed_delay` and
  `fixed_rate_skip`; rounds run inside the service root.
- The miju adapter builds workers on a `Seat`: `worker(|seat| ..)` and
  `monitor(|seat| ..)`. Jobs run inside the stream, or as managed work with
  `.parallelize(seat.executor())`; either way the service drains only after
  every handler has finished.

### Behaviour changes

- **Backend failure and Monitor restarts no longer block completion.** A
  worker's backend error fails its service and is named in the report. A
  Monitor is judged by the final result of `run()`; its restarts are recorded
  and do not fail the report by themselves.
- **The stop grace is per service**, measured from that service's own stop
  request, so every provider gets a full cooperative window. Worst-case
  cooperative shutdown is dependency depth × grace.
- **Cleanup order** is reverse dependency rank, then most recent first. A
  consumer's cleanup still runs before its provider's; the relative order of
  cleanups that belong to unrelated services can differ from 0.2.
- **Grace expiry cuts off miju handlers.** When a worker or monitor service's
  grace expires, its stream is dropped with the handlers running inside it. A
  durable backend redelivers the task after its orphan timeout; a memory or
  cron backend loses it. The report marks the service as forced. Set a grace
  longer than the longest job if a handler must never be cut off.
- **Per-work stop tokens are gone.** Work observes the service's stop signal
  (`ctx.stopping()`, `ctx.stop_token()`) and can be aborted with `Work::abort`.

### Added

- `App::resource` and `Resource<T>`: a value acquired after start, read by
  services that declared `.after(&resource)`; an undeclared read fails every
  time.
- `Lease` (`ctx.lease`, `Scope::lease`) for execution the library does not run.
- `Observer`, `Status` and `AppHandle::changed`; `Snapshot::outstanding` lists
  what is still held after a timed-out wait.
- With the `tracing` feature: events for recorded service failures, the start
  of stopping, grace expiry, failed cleanup actions, termination and loss of the
  driver.

### Removed

- `App::builder`, `AppBuilder::cleanup` and pre-start rollback; register
  cleanup with `ctx.defer` where a resource is acquired.
- `WorkSpec`, `WorkSpec::aggregate`, `fail_service`, `ShutdownProgress`,
  `shutdown_within`, `ready(within)`, `AppOptions` and `ExitReport` (now
  `Report`).

### Dependencies

- Require miju 0.3.0 with the `miju` feature (miju-cron 0.3.0 for examples).
- `futures-util` becomes `futures-core`; `tokio-util` no longer needs `rt`;
  the `jni` feature no longer enables `tokio/net`. MSRV stays 1.89.

### Migration from 0.2.0-beta.6

| 0.2 | 0.3 |
| --- | --- |
| `App::builder().service_fn(n, f).build()?.run_until(s)` | `let mut app = App::new(); app.service_fn(n, f); app.run_until(s)` (an `async fn` item also works with `app.service(n, f)`) |
| `ServiceSpec::new(n).lifetime(Lifetime::Finite).depends_on(p)` | `Spec::new(n).finite().after(p)` |
| `.criticality(Criticality::Optional)`, `.max_work(n)` | `.optional()`, `.capacity(NonZeroUsize)` |
| `.cleanup(label, f)` on the builder + bootstrap rollback | `app.resource(name, \|ctx\| async move { ..; ctx.defer(label, fut)?; Ok(value) })` |
| `context.defer_cleanup(label, f)` + `CleanupRejected` handling | `ctx.defer(label, fut)?` |
| `context.work().spawn(WorkSpec::new(l), \|w\| async { .. })` + `oneshot` for the value | `ctx.spawn(l, async { ..; Ok(value) })?.await?` |
| `WorkSpec::new(l).fail_service()` | `ctx.spawn(critical(l), ..)`, or await the handle and use `?` |
| `WorkSpec::aggregate(l)` | `ctx.lease(l)?` moved into the external execution |
| `work.stopping()` | `ctx.stopping()` (clone the context into the work) |
| `handle.ready(d)` | `tokio::time::timeout(d, handle.ready())` |
| `handle.shutdown_within(d)` → `ShutdownProgress` | `handle.stop(); tokio::time::timeout(d, handle.wait())` |
| `report.snapshot.services[i]` | `report.services[i]` or `report.service(name)`; adapter data via `.detail::<T>()` |
| `worker_from(\|c\| WorkerBuilder::new(c)…stream())` | `worker(\|seat\| WorkerBuilder::new(&seat)…stream())` |
| `monitor(m)` | `monitor(\|_seat\| m)` |
| `periodic::fixed_delay(p, f)?` | `periodic::every(p, f)` |
| `periodic::fixed_rate_skip(p, f)?` | `periodic::every(p, f).on_grid()` |
| `PeriodicRule`, `FirstTick`, `OnRoundError`, `RoundContext` | `.first_after_period()`, `.stop_on_error()`, `every_with` + `Round` |
| `AppOptions { startup_timeout, shutdown_grace, .. }` | `app.startup_timeout(d).shutdown_grace(d)` |
| JNI Java declarations and codes | Unchanged |

## [0.2.0-beta.6]

- bump: to v0.2.0-beta.6 ([#24](https://github.com/nanfeng-labs/miju-services/pull/24))

- docs: clarify shutdown and strengthen lifecycle evidence ([#23](https://github.com/nanfeng-labs/miju-services/pull/23))

### Validation

- Strengthen queued-blocking runtime-shutdown, Monitor restart-success and JNI
  runtime-release regression evidence.
- Document two completed Windows 10 x64/MSVC desktop JNI All runs, including the
  functional matrix, formal load, resource observations and public replay archive.
  The recorded executions are bound to main commit `af51bc0`.

### Documentation

- Clarify that queued blocking work may begin during Tokio runtime shutdown.
- Separate recorded Windows workstation acceptance from Linux/macOS JVM CI,
  Android ART and target business-application acceptance.

Public APIs, dependencies and execution policy are unchanged from beta.5.

## [0.2.0-beta.5]

- bump: to v0.2.0-beta.5 ([#22](https://github.com/nanfeng-labs/miju-services/pull/22))

### Fixed

- Preserve final drain notifications when retiring an old error report unwinds.
- Contain the same retired-error failure in job history and snapshot publication
  without assigning an earlier error's destructor panic to a newer work result.

### Validation

- Add regression coverage for runtime loss before root and cleanup polling,
  started and unpolled work, queued blocking cancellation, and JNI reuse after
  core drain while runtime destruction is still pending.
- Protect the non-exhaustive snapshot contract with a compile-fail doctest and
  strengthen Monitor restart, pre-poll shutdown and periodic grid assertions.

### Documentation

- Clarify finite readiness, custom worker streams, Monitor restart limits and
  WebSocket capacity. Pin the README artwork and license to this release.

## [0.2.0-beta.4]

- bump: to v0.2.0-beta.4 ([#21](https://github.com/nanfeng-labs/miju-services/pull/21))

### Changed

- Require miju 0.2.1 when the `miju` feature is enabled, including the upstream
  Monitor restart scheduling fix. Service APIs and lifecycle policy are unchanged.

- deps: require miju 0.2.1 ([#20](https://github.com/nanfeng-labs/miju-services/pull/20))

## [0.2.0-beta.3]

- bump: to v0.2.0-beta.3 ([#19](https://github.com/nanfeng-labs/miju-services/pull/19))

### Fixed

- Pin README example and language links to release source URLs so they work
  when the README is rendered as crate documentation on docs.rs.

### Documentation

- Put the unbounded run_until wait and operational-host policy beside Usage.
- Explain shared provider grace at stopping and depends_on, and distinguish
  bounded observation from Tokio runtime destruction waiting for blocking work.

## [0.2.0-beta.2]

- bump: to v0.2.0-beta.2 ([#18](https://github.com/nanfeng-labs/miju-services/pull/18))

- fix: improve snapshot compatibility and app examples ([#17](https://github.com/nanfeng-labs/miju-services/pull/17))

### Breaking changes

- Make `ServiceSnapshot` non-exhaustive so Cargo feature unification cannot
  invalidate supported field patterns. Read fields directly or destructure with
  `..`; external struct literals and exhaustive patterns are no longer supported.

### Changed

- State explicitly that `run_until` and `shutdown` have no deadline, and point
  bounded callers to `start` plus `shutdown_within` and application exit policy.
- Explain that dependency providers receive the remainder of the shared stop
  grace, and required asynchronous teardown belongs in registered cleanup.
- Show basic and managed-work assembly in their executable entry files.
- Acquire the configured sampler's output during admitted initialization and
  transfer cleanup before readiness, avoiding pre-start file rollback in the
  normal example. Track the actual file open as owned blocking work through
  cancellation. Keep preconstructed-resource rollback regression coverage.

## [0.2.0-beta.1]

- bump: to v0.2.0-beta.1 ([#16](https://github.com/nanfeng-labs/miju-services/pull/16))

- docs(readme): restore branding and align bilingual guides with miju ([#15](https://github.com/nanfeng-labs/miju-services/pull/15))

- refactor(jni): assemble App with verified runtime ownership ([#14](https://github.com/nanfeng-labs/miju-services/pull/14))
- refactor(examples): simplify protocol and embedded hosts ([#13](https://github.com/nanfeng-labs/miju-services/pull/13))
- refactor(miju): simplify worker and periodic service integration ([#12](https://github.com/nanfeng-labs/miju-services/pull/12))
- feat(examples): add typed configuration and resource assembly ([#11](https://github.com/nanfeng-labs/miju-services/pull/11))
- refactor(core): simplify application assembly and lifecycle control ([#10](https://github.com/nanfeng-labs/miju-services/pull/10))

This release includes breaking Rust API and lifecycle changes from `0.1.0-beta`.

### Breaking changes

- Rename `Application` and `ApplicationBuilder` to `App` and `AppBuilder`, and
  `StartError::into_application()` to `StartError::into_app()`.
- Make `ServiceContext::ready()` infallible and idempotent. It sets default
  `Healthy` only when health has not been explicitly declared; notifications
  after local shutdown or root release no longer change service state.
- Change `AppHandle::shutdown()` to wait for actual completion and return a
  classified result with an `ExitReport`. Use `shutdown_within()` for bounded
  observation returning `Complete(report)` or `Pending(snapshot)`.
- Rename the JNI factory trait from `jni::App` to `jni::AppFactory`, returning
  the core `App`. The five-method Java ABI remains unchanged. Factory results
  must be safe to abandon by synchronous drop; acquire resources that require
  async cleanup inside admitted service initialization.
- Move advanced service-policy, work, and observation types from root re-exports
  to their respective modules.

### Added

- Add `App::run_until()` to observe service completion from startup, handle the
  host stop source, and wait for cleanup while retaining errors and reports.
- Add name-based service registration, `AppBuilder::service_fn()` and named
  builder cleanup, plus direct snapshot, readiness, and wait methods on `AppHandle`.
- Add `configured_app` with validated typed settings, defaults, explicit TOML,
  application-prefixed environment variables, CLI overrides, per-field source
  reporting, resource assembly, and bootstrap rollback.
- Add `watched_settings` with immutable per-round snapshots and retention of
  the last settings after the update sender closes.
- Add `miju::worker_from()` to construct a real miju worker with its lifecycle
  context, avoiding manual pairing in the common path.

### Changed

- Rewrite introductory, resource, managed-work, operational-host, protocol, and
  embedded-runtime examples around focused assembly functions. Configuration,
  resource construction, protocol adapters, and signal policy remain application-owned.
- Use the published optional `miju` dependency for Worker/Monitor integration.
  Retain independent `miju` and `periodic` features, the `miju + periodic` defaults,
  direct miju-cron backends, and default-off JNI hosting.
- Treat successful finite services as initialized only after their root and
  owned work have actually drained, without an authoritative service failure.
  Child errors configured only for recording remain observable without blocking
  that success gate.
- Package the JNI example with `Cargo.toml.in`; materialize its manifest only in
  the selected output directory, with explicit UTF-8 handling for Unicode source paths.
- ci(dependabot): cover the standalone JNI example ([#9](https://github.com/nanfeng-labs/miju-services/pull/9))

## [0.1.0-beta]

- bump: to v0.1.0-beta ([#8](https://github.com/nanfeng-labs/miju-services/pull/8))

- fix(docs): display the product README on the repository homepage ([#7](https://github.com/nanfeng-labs/miju-services/pull/7))

- feat(jni): add JVM lifecycle host and UDP example ([#6](https://github.com/nanfeng-labs/miju-services/pull/6))
- feat(examples): add HTTP WebSocket and UDP services ([#5](https://github.com/nanfeng-labs/miju-services/pull/5))
- feat(examples): add calendar and bounded queue workflows ([#4](https://github.com/nanfeng-labs/miju-services/pull/4))
- feat(periodic): add serial periodic services ([#3](https://github.com/nanfeng-labs/miju-services/pull/3))
- feat(miju): manage worker and monitor lifecycles ([#2](https://github.com/nanfeng-labs/miju-services/pull/2))
- feat(core): add application lifecycle management ([#1](https://github.com/nanfeng-labs/miju-services/pull/1))

- Add service assembly, lifecycle observation, managed work and asynchronous cleanup.
- Integrate miju workers and monitors with service-owned execution and shutdown.
- Add serial direct periodic work with fixed-delay and fixed-grid timing.
- Add calendar sources and bounded queued rounds with final receipts.
- Add HTTP, WebSocket and UDP lifecycle examples and protocol regression coverage.
- Add an optional five-method JVM host with per-generation runtime ownership,
  explicit shutdown observation and a real Java UDP example.
