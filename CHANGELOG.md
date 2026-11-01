# Changelog

## [Unreleased]

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
