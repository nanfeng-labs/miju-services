# JNI application

The Java bridge owns five static native declarations and loads the library directly. Rust parses the two application arguments and calls `assemble(address, output)`. The optional JNI host owns one Tokio runtime per generation; ordinary Rust applications continue to own their own runtime.

The factory returns an unstarted `App` whose captures are safe to drop synchronously. Sockets and address files are acquired by admitted services, with cleanup registered before readiness. An output file must not already exist; the example never overwrites it.

Run with explicit JDK and evidence/build paths:

```sh
python3 run.py --java-home "$JAVA_HOME" --target-dir /absolute/build/jni --output /absolute/results/jni --toolchain stable
```

The runner materializes `Cargo.toml.in` into the selected output directory, leaving this checkout unchanged. This keeps the complete example available in the published source archive. It uses `-Xcheck:jni`, sends a real UDP packet, stops, verifies file removal and port rebind, then starts a second generation. Output must be a new directory. Java calls that may wait belong on application background threads.

`nativeStart(String[])` waits for healthy required readiness or a first failure. `nativeStop(long)` requests stop and waits up to the supplied milliseconds; `-124` means observation expired while ownership and cleanup continue. It does not permit another generation. A successful stop is published only after actual core drain, release of report-held user references, and runtime destruction. `nativeStatus`, `nativeLastError` and `nativeVersion` inspect process-local metadata.

Status: Idle=0, Starting=1, Running=2, Stopping=3, Failed=5. Restarting=4 is reserved. Start results: success=0, already running=1, busy retiring/quarantined=3, startup cancelled=4. Stop without a live generation=2. Failures: usage=-64, startup=-69, unconfirmed lifecycle=-70, OS/runtime=-71, configuration=-78, contained panic=-99. Factory messages must already be sanitized; arbitrary business errors are never formatted for Java. Start observation timeout remains -69 even when cleanup finishes later.

Use one export per cdylib. Renamed dependencies and a nested export module are supported. `panic=unwind` is required; unconditional `forbid(unsafe_code)` consumers need a separate small export crate. The bridge class and loader are pinned for process lifetime. Windows desktop JNI has [recorded host acceptance](https://github.com/nanfeng-labs/miju-services/releases/download/v0.2.0-beta.5/BETA5-WINDOWS-ACCEPTANCE.md) for the specified workstation and source revision. Hot unloading, multiple JVMs and Android ART require their own host acceptance.
