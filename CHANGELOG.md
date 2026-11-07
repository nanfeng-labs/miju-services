# Changelog

## [Unreleased]

- refactor(core): simplify application assembly and lifecycle control ([#10](https://github.com/nanfeng-labs/miju-services/pull/10))

The local 0.2.0-beta.1 refactor is unreleased and includes breaking Rust API changes.

- Rename Application/ApplicationBuilder to App/AppBuilder and recover rejected starts with StartError::into_app().
- Add name-based registration, service_fn, named cleanup and run_until for the ordinary application path.
- Make ready() infallible; preserve explicit health and ignore late notifications after local stop or root release.
- Make shutdown wait for actual completion; use shutdown_within for Complete/Pending observation.
- Drain service-owned work before dependency release and LIFO cleanup; preserve finite-service fault and completion semantics.
- Keep advanced policy, work and observation types in their responsibility modules.

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
