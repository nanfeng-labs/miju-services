//! Keeps panics in user code and user destructors from unwinding into the lifecycle.
#![forbid(unsafe_code)]

use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

/// Drop a user value. Returns whether its destructor panicked.
pub(crate) fn drop_contained<T>(value: T) -> bool {
    catch_unwind(AssertUnwindSafe(|| drop(value))).is_err()
}

/// Extract a panic message, then drop the payload, which may panic as well.
pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> Arc<str> {
    let message: Arc<str> = if let Some(message) = payload.downcast_ref::<String>() {
        Arc::from(message.as_str())
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        Arc::from(*message)
    } else {
        Arc::from("non-string panic payload")
    };
    drop_contained(payload);
    message
}
