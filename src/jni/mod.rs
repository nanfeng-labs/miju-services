//! A synchronous five-method JVM host for an application.
//!
//! Enable the default-off `jni` feature, implement [`AppFactory`] in a `cdylib`, and
//! invoke [`export!`] once. The Java bridge class must directly load the library
//! in its own static initializer and declare the five static native methods.
//! Successful loading pins that class and its loader for the process lifetime;
//! hot unloading and multiple JVMs are not supported.
//!
//! Java start/stop calls belong on background threads. Stop timeouts limit the
//! caller's observation, while the owner keeps driving the application and its
//! runtime until actual cleanup finishes. Applications supply configuration,
//! resources and logging; owner threads never attach to the JVM.
#![deny(unsafe_code)]

#[cfg(panic = "abort")]
compile_error!("miju-services JNI requires panic=unwind for FFI containment");

mod ffi;
mod host;

use std::{fmt, time::Duration};

/// An application's static factory, called once inside its generation's runtime.
///
/// Every capture in the returned, unstarted App must be safe to abandon by
/// synchronous drop. Stop or startup timeout may arrive while this factory runs;
/// the owner then drops its result without starting services or async cleanup.
/// A builder cleanup alone is not pre-start rollback. Acquire async-close
/// resources inside service initialization and register `defer_cleanup` before
/// reporting ready. On a factory error, local rollback must already be complete.
///
/// Return an assembled, unstarted [`crate::App`]. Do not spawn detached
/// tasks or perform indefinite blocking initialization here; asynchronous setup
/// belongs in service roots. Factory-owned resources need their own rollback if
/// construction fails. `START_TIMEOUT` limits Java observation, not factory work.
pub trait AppFactory: Send + Sync + 'static {
    /// Display name used by Java's version method.
    const NAME: &'static str;

    /// Display version used by Java's version method.
    const VERSION: &'static str;

    /// Nonzero, representable budget for the first healthy Required readiness.
    const START_TIMEOUT: Duration = Duration::from_secs(30);

    /// Construct the application using owned Java arguments.
    fn build(args: Vec<String>) -> Result<crate::App, BuildError>;
}

/// Classification of a factory error at the Java ABI boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BuildErrorKind {
    /// Invalid arguments, mapped to -64.
    Usage,
    /// Invalid application configuration, mapped to -78.
    Configuration,
    /// Other construction failure, mapped to -69.
    Startup,
}

/// An explicit, application-sanitized factory failure.
///
/// `safe_message` may be returned to Java. Remove secrets before constructing
/// this value; arbitrary error objects and source chains are never formatted by
/// the host. Java error summaries are capped at 4096 UTF-8 bytes.
#[derive(Debug)]
#[non_exhaustive]
pub struct BuildError {
    kind: BuildErrorKind,
    safe_message: String,
}

impl BuildError {
    /// Create an error from a classification and an already-sanitized message.
    pub fn new(kind: BuildErrorKind, safe_message: impl Into<String>) -> Self {
        Self {
            kind,
            safe_message: safe_message.into(),
        }
    }

    /// The factory failure category.
    pub fn kind(&self) -> BuildErrorKind {
        self.kind
    }

    /// The message the application has explicitly approved for Java exposure.
    pub fn safe_message(&self) -> &str {
        &self.safe_message
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for BuildError {}

/// Export the single JVM loader entry point for this native library.
///
/// ```no_run
/// use miju_services::{App, service::{Lifetime, ServiceSpec}};
/// use miju_services::jni::{AppFactory, BuildError, BuildErrorKind};
/// struct DeviceApp;
/// impl AppFactory for DeviceApp {
///     const NAME: &'static str = "device-service";
///     const VERSION: &'static str = "1";
///     fn build(_args: Vec<String>) -> Result<App, BuildError> {
///         App::builder()
///             .service_fn(ServiceSpec::new("initialize").lifetime(Lifetime::Finite),
///                 |_| async { Ok(()) })
///             .build()
///             .map_err(|_| BuildError::new(BuildErrorKind::Startup, "assembly failed"))
///     }
/// }
/// // In a cdylib consumer, export once for the directly loading Java class.
/// miju_services::jni::export!(class = "com/example/DeviceBridge", app = DeviceApp);
/// ```
///
/// The class must declare exactly `nativeStart(String[])`, `nativeStop(long)`,
/// `nativeStatus()`, `nativeLastError()`, and `nativeVersion()` as static native
/// methods. One library may invoke this macro once, including from a nested
/// module or through a renamed dependency. A consuming crate with an unconditional
/// `forbid(unsafe_code)` must use a separate export crate; its lint cannot be
/// overridden by the controlled ABI function this macro emits.
#[doc(inline)]
pub use crate::__miju_services_jni_export as export;

/// Implementation support for exported ABI code; not a general host interface.
#[doc(hidden)]
pub mod __private {
    pub use super::ffi::register;
    pub use ::jni::sys::{JavaVM, jint};
}

/// Implementation of [`crate::jni::export!`]; prefer its module-qualified name.
#[doc(hidden)]
#[macro_export]
macro_rules! __miju_services_jni_export {
    (class = $class:literal, app = $app:ty $(,)?) => {
        /// The JVM-supplied loader entry point for the dedicated bridge class.
        ///
        /// # Safety
        /// `vm` must be the live JVM pointer supplied to JNI_OnLoad.
        #[allow(unsafe_code)]
        #[unsafe(no_mangle)]
        pub unsafe extern "system" fn JNI_OnLoad(
            vm: *mut $crate::jni::__private::JavaVM,
            _reserved: *mut ::core::ffi::c_void,
        ) -> $crate::jni::__private::jint {
            // SAFETY: the JVM calls this ABI with its own live VM pointer.
            unsafe { $crate::jni::__private::register::<$app>(vm, $class) }
        }
    };
    ($($invalid:tt)*) => {
        compile_error!("expected export!(class = \"package/Class\", app = FactoryType)");
    };
}
