//! The sole raw JNI boundary: thread-local references, registration and ABI calls.
#![allow(unsafe_code)]

use super::{AppFactory, host::Bridge};
use ::jni::{strings::JNIString, sys};
use std::{
    any::TypeId,
    ffi::{CStr, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{
        OnceLock,
        atomic::{AtomicU8, Ordering},
    },
};

const COLD: u8 = 0;
const LOADING: u8 = 1;
const READY: u8 = 2;
const FAILED: u8 = 3;
const FAULT: sys::jint = -70;
const PANICKED: sys::jint = -99;

static PHASE: AtomicU8 = AtomicU8::new(COLD);
static BINDING: OnceLock<Binding> = OnceLock::new();

struct Binding {
    bridge: Bridge,
    vm: usize,
    application: TypeId,
    // A successful global reference is deliberately retained for process life.
    // It pins the bridge Class and, under the documented direct-load contract,
    // the ClassLoader that owns this native library. It is never dereferenced.
    _class_pin: usize,
}

/// Register this application's five native methods during JVM library loading.
///
/// # Safety
/// `vm` must be the live JVM pointer invoking this library's JNI_OnLoad. The
/// dedicated bridge class must directly load this library in its initializer.
pub unsafe fn register<A: AppFactory>(vm: *mut sys::JavaVM, class_name: &str) -> sys::jint {
    let mut attempt = LoadAttempt::default();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if vm.is_null() {
            return Err(());
        }
        if PHASE
            .compare_exchange(COLD, LOADING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return if PHASE.load(Ordering::Acquire) == READY
                && BINDING.get().is_some_and(|binding| {
                    binding.vm == vm as usize && binding.application == TypeId::of::<A>()
                }) {
                Ok(())
            } else {
                Err(())
            };
        }
        attempt.owns_loading = true;
        let mut raw = ptr::null_mut();
        // SAFETY: caller supplies the loader's live VM; GetEnv writes a current
        // thread attachment pointer, and no JVM attachment is created here.
        let result = unsafe { ((**vm).v1_2.GetEnv)(vm, &mut raw, sys::JNI_VERSION_1_6) };
        if result != sys::JNI_OK || raw.is_null() {
            return Err(());
        }
        let env = Context { raw: raw.cast() };
        attempt.env = Some(env);
        if env.pending() || !valid_class_name(class_name) {
            return Err(());
        }
        let bridge = Bridge::new::<A>();
        let name = JNIString::new(class_name);
        let class = jni_call!(env, FindClass(name.as_ptr()));
        env.check_object(class)?;
        attempt.class = class;
        validate_declarations(&env, class)?;
        let pin = jni_call!(env, NewGlobalRef(class));
        env.check_object(pin)?;
        attempt.pin = pin;
        let methods = native_methods();
        attempt.registration_attempted = true;
        // SAFETY: declarations were verified as exactly the five static native
        // methods; the table functions below have those exact JNI signatures.
        let result = jni_call!(
            env,
            RegisterNatives(class, methods.as_ptr(), methods.len() as sys::jint)
        );
        if result != sys::JNI_OK || env.pending() {
            return Err(());
        }
        let binding = Binding {
            bridge,
            vm: vm as usize,
            application: TypeId::of::<A>(),
            _class_pin: pin as usize,
        };
        // The loading CAS admits one writer. Moving into OnceLock allocates no
        // memory, performs no JNI operation and invokes no application code.
        if BINDING.set(binding).is_err() {
            return Err(());
        }
        attempt.pin = ptr::null_mut();
        PHASE.store(READY, Ordering::Release);
        Ok(())
    }));
    let success = match outcome {
        Ok(Ok(())) => true,
        Ok(Err(())) => false,
        Err(payload) => {
            std::mem::forget(payload);
            false
        }
    };
    // Rollback remains outside all host/loading locks. Its own panic boundary
    // ensures a malformed failure path cannot unwind into JNI_OnLoad.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| attempt.finish(success))) {
        std::mem::forget(payload);
        if attempt.owns_loading {
            PHASE.store(FAILED, Ordering::Release);
        }
        return sys::JNI_ERR;
    }
    if success {
        sys::JNI_VERSION_1_6
    } else {
        sys::JNI_ERR
    }
}

fn valid_class_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains(['.', '[', ']', ';', '\0'])
        && name.split('/').all(|part| !part.is_empty())
}

#[derive(Default)]
struct LoadAttempt {
    env: Option<Context>,
    class: sys::jclass,
    pin: sys::jobject,
    owns_loading: bool,
    registration_attempted: bool,
}

impl LoadAttempt {
    fn finish(&mut self, success: bool) {
        if !self.owns_loading {
            return;
        }
        if let Some(env) = self.env {
            if !success {
                if self.registration_attempted {
                    let original = Local::new(&env, jni_call!(env, ExceptionOccurred()));
                    // JNI specifies only controlled rollback while pending.
                    if env.pending() {
                        jni_call!(env, ExceptionClear());
                    }
                    let _rollback = jni_call!(env, UnregisterNatives(self.class));
                    if !original.raw.is_null() {
                        if env.pending() {
                            jni_call!(env, ExceptionClear());
                        }
                        jni_call!(env, Throw(original.raw));
                    }
                }
                if !env.pending() {
                    env.throw(
                        c"java/lang/UnsatisfiedLinkError",
                        c"miju-services JNI registration failed",
                    );
                }
            }
            if !self.pin.is_null() {
                jni_call!(env, DeleteGlobalRef(self.pin));
                self.pin = ptr::null_mut();
            }
            if !self.class.is_null() {
                jni_call!(env, DeleteLocalRef(self.class));
                self.class = ptr::null_mut();
            }
        }
        if !success {
            PHASE.store(FAILED, Ordering::Release);
        }
    }
}

// This macro is confined to the raw boundary. Each helper constrains arguments
// to live references belonging to its Context's current JNI frame. All methods
// called are present in JNI 1.6; the loader requests that version above.
macro_rules! jni_call {
    ($env:expr, $method:ident($($argument:expr),* $(,)?)) => {{
        let context = &$env;
        // SAFETY: Context is created only from JVM-owned, current-thread JNI
        // pointers at a native entry or GetEnv; helpers preserve pointer types.
        unsafe { ((**context.raw).v1_6.$method)(context.raw, $($argument),*) }
    }};
}
use jni_call;

#[derive(Clone, Copy)]
struct Context {
    raw: *mut sys::JNIEnv,
}

impl Context {
    fn pending(&self) -> bool {
        jni_call!(self, ExceptionCheck())
    }

    fn check(&self) -> Result<(), ()> {
        if self.pending() { Err(()) } else { Ok(()) }
    }

    fn check_object(&self, object: sys::jobject) -> Result<(), ()> {
        self.check()?;
        if object.is_null() { Err(()) } else { Ok(()) }
    }

    fn nullable_object_call(
        &self,
        object: sys::jobject,
        name: &CStr,
        signature: &CStr,
    ) -> Result<Local<'_>, ()> {
        let class = Local::new(self, jni_call!(self, GetObjectClass(object)));
        self.check_object(class.raw)?;
        let method = jni_call!(
            self,
            GetMethodID(class.raw, name.as_ptr(), signature.as_ptr())
        );
        if method.is_null() || self.pending() {
            return Err(());
        }
        let value = Local::new(
            self,
            jni_call!(self, CallObjectMethodA(object, method, ptr::null())),
        );
        self.check()?;
        Ok(value)
    }

    fn object_call(
        &self,
        object: sys::jobject,
        name: &CStr,
        signature: &CStr,
    ) -> Result<Local<'_>, ()> {
        let result = self.nullable_object_call(object, name, signature)?;
        self.check_object(result.raw)?;
        Ok(result)
    }

    fn integer_call(&self, object: sys::jobject, name: &CStr) -> Result<sys::jint, ()> {
        let class = Local::new(self, jni_call!(self, GetObjectClass(object)));
        self.check_object(class.raw)?;
        let method = jni_call!(self, GetMethodID(class.raw, name.as_ptr(), c"()I".as_ptr()));
        if method.is_null() || self.pending() {
            return Err(());
        }
        let value = jni_call!(self, CallIntMethodA(object, method, ptr::null()));
        self.check()?;
        Ok(value)
    }

    fn string(&self, value: sys::jstring) -> Result<String, ()> {
        if value.is_null() {
            return Ok(String::new());
        }
        let length = jni_call!(self, GetStringLength(value));
        self.check()?;
        let mut utf16 = vec![0; usize::try_from(length).map_err(|_| ())?];
        jni_call!(self, GetStringRegion(value, 0, length, utf16.as_mut_ptr()));
        self.check()?;
        Ok(String::from_utf16_lossy(&utf16))
    }

    fn new_string(&self, value: &str) -> Result<sys::jstring, ()> {
        let utf16: Vec<u16> = value.encode_utf16().collect();
        let length = sys::jsize::try_from(utf16.len()).map_err(|_| ())?;
        let string = jni_call!(self, NewString(utf16.as_ptr(), length));
        self.check_object(string)?;
        Ok(string)
    }

    fn throw(&self, class: &CStr, message: &CStr) {
        if self.pending() {
            return;
        }
        let class = Local::new(self, jni_call!(self, FindClass(class.as_ptr())));
        if !class.raw.is_null() && !self.pending() {
            jni_call!(self, ThrowNew(class.raw, message.as_ptr()));
        }
    }

    fn array_length(&self, array: sys::jarray) -> Result<sys::jsize, ()> {
        let length = jni_call!(self, GetArrayLength(array));
        self.check()?;
        Ok(length)
    }

    fn element(&self, array: sys::jobjectArray, index: sys::jsize) -> Result<Local<'_>, ()> {
        let value = Local::new(self, jni_call!(self, GetObjectArrayElement(array, index)));
        self.check()?;
        Ok(value)
    }
}

struct Local<'env> {
    env: &'env Context,
    raw: sys::jobject,
}

impl<'env> Local<'env> {
    fn new(env: &'env Context, raw: sys::jobject) -> Self {
        Self { env, raw }
    }
}

impl Drop for Local<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            jni_call!(self.env, DeleteLocalRef(self.raw));
        }
    }
}

fn java_class_name(env: &Context, class: sys::jclass) -> Result<String, ()> {
    let name = env.object_call(class, c"getName", c"()Ljava/lang/String;")?;
    env.string(name.raw)
}

fn validate_declarations(env: &Context, class: sys::jclass) -> Result<(), ()> {
    validate_no_inherited_bridge_methods(env, class)?;
    let methods = env.object_call(
        class,
        c"getDeclaredMethods",
        c"()[Ljava/lang/reflect/Method;",
    )?;
    let mut found = [false; 5];
    for index in 0..env.array_length(methods.raw)? {
        let method = env.element(methods.raw, index)?;
        let modifiers = env.integer_call(method.raw, c"getModifiers")?;
        if modifiers & 0x100 == 0 {
            continue;
        }
        if modifiers & 0x8 == 0 {
            return Err(());
        }
        let name = env.object_call(method.raw, c"getName", c"()Ljava/lang/String;")?;
        let name = env.string(name.raw)?;
        let (slot, expected_parameters, expected_return): (usize, &[&str], &str) =
            match name.as_str() {
                "nativeStart" => (0, &["[Ljava.lang.String;"], "int"),
                "nativeStop" => (1, &["long"], "int"),
                "nativeStatus" => (2, &[], "int"),
                "nativeLastError" => (3, &[], "java.lang.String"),
                "nativeVersion" => (4, &[], "java.lang.String"),
                _ => return Err(()),
            };
        if found[slot] {
            return Err(());
        }
        let parameters =
            env.object_call(method.raw, c"getParameterTypes", c"()[Ljava/lang/Class;")?;
        if usize::try_from(env.array_length(parameters.raw)?).map_err(|_| ())?
            != expected_parameters.len()
        {
            return Err(());
        }
        for (index, expected) in expected_parameters.iter().enumerate() {
            let parameter = env.element(parameters.raw, index as sys::jsize)?;
            if java_class_name(env, parameter.raw)? != *expected {
                return Err(());
            }
        }
        let returns = env.object_call(method.raw, c"getReturnType", c"()Ljava/lang/Class;")?;
        if java_class_name(env, returns.raw)? != expected_return {
            return Err(());
        }
        found[slot] = true;
    }
    if found.iter().all(|present| *present) {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_no_inherited_bridge_methods(env: &Context, class: sys::jclass) -> Result<(), ()> {
    let mut parent = env.nullable_object_call(class, c"getSuperclass", c"()Ljava/lang/Class;")?;
    while !parent.raw.is_null() {
        let methods = env.object_call(
            parent.raw,
            c"getDeclaredMethods",
            c"()[Ljava/lang/reflect/Method;",
        )?;
        for index in 0..env.array_length(methods.raw)? {
            let method = env.element(methods.raw, index)?;
            if env.integer_call(method.raw, c"getModifiers")? & 0x100 == 0 {
                continue;
            }
            let name = env.object_call(method.raw, c"getName", c"()Ljava/lang/String;")?;
            if matches!(
                env.string(name.raw)?.as_str(),
                "nativeStart" | "nativeStop" | "nativeStatus" | "nativeLastError" | "nativeVersion"
            ) {
                return Err(());
            }
        }
        parent = env.nullable_object_call(parent.raw, c"getSuperclass", c"()Ljava/lang/Class;")?;
    }
    Ok(())
}

fn native_methods() -> [sys::JNINativeMethod; 5] {
    macro_rules! method {
        ($name:literal, $signature:literal, $function:ident) => {
            sys::JNINativeMethod {
                name: $name.as_ptr().cast_mut(),
                signature: $signature.as_ptr().cast_mut(),
                fnPtr: $function as *const () as *mut c_void,
            }
        };
    }
    [
        method!(c"nativeStart", c"([Ljava/lang/String;)I", native_start),
        method!(c"nativeStop", c"(J)I", native_stop),
        method!(c"nativeStatus", c"()I", native_status),
        method!(
            c"nativeLastError",
            c"()Ljava/lang/String;",
            native_last_error
        ),
        method!(c"nativeVersion", c"()Ljava/lang/String;", native_version),
    ]
}

fn bridge() -> Result<&'static Bridge, ()> {
    if PHASE.load(Ordering::Acquire) != READY {
        return Err(());
    }
    BINDING.get().map(|binding| &binding.bridge).ok_or(())
}

fn integer_boundary(
    raw: *mut sys::JNIEnv,
    operation: impl FnOnce(&Context) -> Result<sys::jint, ()>,
) -> sys::jint {
    match catch_unwind(AssertUnwindSafe(|| {
        let env = Context { raw };
        env.check()?;
        operation(&env)
    })) {
        Ok(Ok(result)) => result,
        Ok(Err(())) => FAULT,
        Err(payload) => {
            std::mem::forget(payload);
            PANICKED
        }
    }
}

fn string_boundary(
    raw: *mut sys::JNIEnv,
    operation: impl FnOnce() -> Result<String, ()>,
) -> sys::jstring {
    let env = Context { raw };
    match catch_unwind(AssertUnwindSafe(|| {
        env.check()?;
        let value = operation()?;
        env.new_string(&value)
    })) {
        Ok(Ok(result)) => result,
        Ok(Err(())) => {
            env.throw(
                c"java/lang/IllegalStateException",
                c"miju-services JNI call failed",
            );
            ptr::null_mut()
        }
        Err(payload) => {
            std::mem::forget(payload);
            // String failure must not unwind while attempting its diagnostic.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                env.throw(
                    c"java/lang/IllegalStateException",
                    c"miju-services JNI call panicked",
                )
            })) {
                std::mem::forget(payload);
            }
            ptr::null_mut()
        }
    }
}

// SAFETY for each entry: the registered descriptors guarantee the supplied live
// current-thread environment, bridge Class, and parameter types. Raw references
// stay on this thread; Rust owns every argument before crossing to the owner.
unsafe extern "system" fn native_start(
    raw: *mut sys::JNIEnv,
    _: sys::jclass,
    args: sys::jobjectArray,
) -> sys::jint {
    integer_boundary(raw, |env| {
        let mut arguments = Vec::new();
        if !args.is_null() {
            let length = env.array_length(args)?;
            for index in 0..length {
                let argument = env.element(args, index)?;
                arguments.push(env.string(argument.raw)?);
            }
        }
        Ok(bridge()?.start(arguments))
    })
}

unsafe extern "system" fn native_stop(
    raw: *mut sys::JNIEnv,
    _: sys::jclass,
    timeout: sys::jlong,
) -> sys::jint {
    integer_boundary(raw, |_| Ok(bridge()?.stop(timeout)))
}

unsafe extern "system" fn native_status(raw: *mut sys::JNIEnv, _: sys::jclass) -> sys::jint {
    integer_boundary(raw, |_| Ok(bridge()?.status()))
}

unsafe extern "system" fn native_last_error(raw: *mut sys::JNIEnv, _: sys::jclass) -> sys::jstring {
    string_boundary(raw, || Ok(bridge()?.last_error()))
}

unsafe extern "system" fn native_version(raw: *mut sys::JNIEnv, _: sys::jclass) -> sys::jstring {
    string_boundary(raw, || Ok(bridge()?.version()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_names_require_nonempty_slash_separated_components() {
        for name in ["Bridge", "com/example/Bridge", "应用/桥接"] {
            assert!(valid_class_name(name));
        }
        for name in [
            "",
            "com.example.Bridge",
            "[LBridge;",
            "/Bridge",
            "Bridge/",
            "a//b",
            "a\0b",
        ] {
            assert!(!valid_class_name(name));
        }
    }
}
