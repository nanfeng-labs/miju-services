# Configured application

This application loads two settings and registers a sampler. After start accepts, service initialization opens an append-only output file through owned blocking work, registers its close action, and passes the settings and file to the sampler constructor. Configuration loading and resource construction remain application code.

```text
Default → selected TOML → SAMPLER_ environment → CLI
                         ↓
                validated Settings
                         ↓
                    assemble App
                         ↓
           start → open output → ready
                         ↓
           Ctrl-C → drain → flush/close
```

## Run

From the repository root:

```sh
cargo run --example configured_app -- --explain
cargo run --example configured_app -- --config examples/configured_app/settings.toml --interval-ms 250 --output samples.log
```

`--explain` prints the effective values and their sources, then exits before opening the output. Otherwise Ctrl-C requests stop and the final report appears after cleanup. Required service errors also end the run without waiting for Ctrl-C.

| Setting | Default | Environment | CLI |
| --- | --- | --- | --- |
| `sample_interval_ms` | `1000` | `SAMPLER_SAMPLE_INTERVAL_MS` | `--interval-ms` |
| `output_file` | `configured-samples.log` | `SAMPLER_OUTPUT_FILE` | `--output` |

For example, `SAMPLER_SAMPLE_INTERVAL_MS=200 cargo run --example configured_app -- --explain` shows the environment overriding the default. Adding `--interval-ms 50` makes CLI win. These are application-specific names, not library environment variables.

No file is searched for unless `--config` is supplied. A selected file must exist at that exact path, contain UTF-8 TOML, and use known fields. Unknown prefixed environment keys and unknown CLI options are errors. Environment names are case-insensitive; multiple spellings of the same name are rejected, including when their values match or CLI overrides that field. Intervals must be positive, representable integer milliseconds; booleans and floating-point values are rejected. Literal output names such as `123` stay strings. Empty paths are rejected. Only these two nonsensitive settings appear in `--explain`; the environment is not dumped.

Output paths are relative to the process working directory, including paths read from TOML. The parent directory must already exist. Existing output is appended to rather than truncated. Each sample records local elapsed milliseconds and the process ID. The service uses an ordinary Tokio wait followed by one write; it does not introduce a periodic scheduling or retry framework.

## Composition and responsibility

- [main.rs](main.rs) parses application input, optionally explains it, and runs the assembled App.
- [settings.rs](settings.rs) merges through `config`, validates the result, and reads the effective origins from the same merged values. It does not infer provenance by repeating the merge.
- [bootstrap.rs](bootstrap.rs) shows the App builder and initialization: open the output, transfer its cleanup, then run the sampler.
- [services.rs](services.rs) contains the output resource and the sampling service, receiving settings and an `Arc` through its constructor.

The normal path acquires no file before start accepts, so build/start refusal needs no asynchronous resource rollback. An open failure becomes a service failure in the final report. A stop before initialization never opens the output.

Initialization uses `context.defer_cleanup` before `ready()`, including when stop arrives during acquisition. If registration is rejected, the application recovers the closure with `into_inner()` and executes it. Once accepted, cleanup belongs to the driver, including after service failure.

The file open uses `context.work().spawn_blocking` and transfers its result through a one-shot channel. Cancellation can stop a queued factory; an operation that already started remains owned until it returns. Cancellation before acquisition is normal stopping, while a delivered I/O error remains a service failure even after stop. This also prevents a cancelled Tokio filesystem waiter from leaving an untracked open behind the final report.

Use preconstructed resources when the application needs to share an existing resource before startup; the [shared resource example](../shared_resource.rs) demonstrates that separate boundary. It requires independent rollback access until start accepts. The [preconstructed-resource test fixture](../../tests/support/configured_prebuilt.rs) retains the earlier file rollback path, including original and rollback errors, while the normal example avoids that bookkeeping.

The output's `close` action takes its writer, flushes it, and releases it before returning. A second close attempt is an error; it cannot certify another still-pending close. Cleanup is registered once. A slow flush keeps the App driver responsible even after a bounded shutdown observation returns `Pending`.

Logging is a separate application choice. If bootstrap installs a subscriber or a buffered log writer, main must retain its guard through `run_until` and final-report output, then flush or drop it. This sample adds no logging dependencies, subscriber, log-directory policy, or rotation setup.

## Runtime configuration

[watched_settings](../watched_settings.rs) is a separate small example using `watch::Receiver<Arc<Settings>>`: one round retains one immutable snapshot and the next reads the latest value. Updates can coalesce. Sender closure retains the last settings until normal service stop, and a notification does not certify that the service applied the value. No file watcher or reload framework is provided.

The actual configured assembly, pre-start resource absence, initialization errors, file flush and preconstructed rollback paths are tested in [configured_app.rs](../../tests/configured_app.rs), source precedence in [configuration.rs](../../tests/configuration.rs), and runtime updates in [watched_settings.rs](../../tests/watched_settings.rs).
