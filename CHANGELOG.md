# Changelog

## [Unreleased]

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
