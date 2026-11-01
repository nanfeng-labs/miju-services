# Java-hosted UDP application

This independent `cdylib` example enables only `miju-services/jni`, reuses the
ordinary UDP example helper, and echoes real loopback datagrams. Java passes a
bind address and an address-file path; application code owns this configuration
and removes the file in asynchronous cleanup.

The bridge class itself calls `System.loadLibrary` in its static initializer and
exclusively declares the five static native methods. Keep that loading pattern.
The strong class pin intentionally retains the class, defining loader and native
library for the process lifetime; hot unloading and helper-class loading are not
supported. Every service generation receives its own owned Tokio runtime.

With a JDK 17 or 21 and Python 3, run from the design/validation directory. Replace
the JDK home and use new output paths:

```sh
python3 /path/to/miju-services/examples/jni/run.py \
  --java-home /path/to/jdk \
  --target-dir /path/to/validation/jni-example-target \
  --output /path/to/validation/jni-example-run
```

The script requires Rust 1.96.0 by default; `--toolchain 1.89.0` verifies the MSRV.
It compiles the native library and Java, enables `-Xcheck:jni`, then runs two
start/echo/stop/cleanup/rebind generations in one real JVM. Cargo, javac and Java
logs are written under the explicit output path. The example does not install
logging, OS signal handlers or a JVM shutdown hook.

The `nativeStart` call waits for the first healthy Required availability, not just
initialization. Call start/stop on an application background thread. A stop return
of `-124` only ends that caller's observation; keep the JVM alive and observe or
wait again until actual termination. Normal JVM exit must follow a successful
stop; neither `System.exit` nor coroutine cancellation is an asynchronous cleanup
barrier. This example uses a 5-second stop budget and checks the final result.

The crate has `unsafe_code = "deny"`; only the small export module permits the
controlled ABI macro expansion. A downstream unconditional crate-wide
`forbid(unsafe_code)` is incompatible with generated JNI ABI exports.

Rivium migration retains one `export!(class, app)` and five Java declarations,
while moving app construction to `jni::App::build`. This host has a startup
observation budget and uses the stop timeout only for Java waiting; it does not
change the application's core shutdown budget or provide Rivium configuration,
root directories, reload, signal or automatic restart facilities.

## Five-method results

`nativeStatus()` returns Idle=0, Starting=1, Running=2, Stopping=3 or Failed=5.
Restarting=4 is reserved and is not produced. Running means first healthy
availability was observed; later health degradation does not change this
lifecycle status. Use the application's own endpoint for current core health.
Failed can be a quarantined, non-reusable generation, so do not infer drain from
that value alone.

| Result | Meaning |
| --- | --- |
| `0` | Start observed availability or successful finite completion; Stop observed successful actual release |
| `1` | Start already has a Starting/Running generation; factory is not called again |
| `2` | Stop found no active generation |
| `3` | Start is busy with stopping, reclaiming or quarantined ownership |
| `4` | Start was cancelled by an external stop before first availability |
| `-64` | Usage or an unrepresentable timeout; no stop is sent for an invalid stop deadline |
| `-69` | Startup failure or startup observation deadline |
| `-70` | Runtime lifecycle failure, DriverLost or host/JNI fault |
| `-71` | Owner OS thread or Tokio runtime could not be created |
| `-78` | Explicit factory Configuration error |
| `-99` | A Rust unwind was contained at the factory, owner or JNI boundary |
| `-124` | This Stop observer timed out; ownership remains active |

Negative stop timeouts mean zero waiting but still request stop. An extreme
positive timeout is accepted only if the monotonic deadline can represent it.
`nativeStart` has one immutable first result; later failure remains visible in
final stop/status/error. A captured stop waiter never targets a replacement
generation. A poisoned/unreadable private lock returns Fault without reuse;
a readable quarantined generation rejects Start as Busy.

`nativeLastError()` normally returns a non-null string, empty when no error has
been recorded. It contains at most 4096 UTF-8 bytes and never formats arbitrary
business Error/source chains or panic payloads. `BuildError::new` takes text the
application has already sanitized. Pending Java exceptions are preserved;
allocation failure may return null with an exception. Raw pointers not supplied
by the JVM, OOM/abort, double panic and application-leaked globals or detached
work are outside the containment/release guarantee.

Java null arguments/elements become an empty vector/empty string. Input is
copied to Rust-owned strings, with invalid UTF-16 surrogate sequences replaced
by the Unicode replacement character. Per-element local references are released
before moving to the next array element.

Kotlin calls the same Java declarations on its I/O dispatcher:

```kotlin
val result = withContext(Dispatchers.IO) {
    MijuBridge.nativeStart(arrayOf("127.0.0.1:0", addressFile))
}
```

Coroutine cancellation and Java thread interruption do not cancel an accepted
Rust lifecycle. The application must call `nativeStop` and retain the JVM until
completion. There is no automatic JVM shutdown hook or Java callback.
