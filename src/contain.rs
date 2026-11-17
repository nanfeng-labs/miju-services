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

pin_project_lite::pin_project! {
    /// Maps a future's error without dropping the future when it completes.
    /// Whoever owns it drops it afterwards, under its own containment, so a
    /// destructor panic is reported next to the result instead of replacing it.
    pub(crate) struct KeepUntilDropped<F> {
        #[pin]
        pub(crate) inner: F,
    }
}

impl<F, E> std::future::Future for KeepUntilDropped<F>
where
    F: std::future::Future<Output = Result<(), E>>,
    E: Into<crate::BoxError>,
{
    type Output = Result<(), crate::BoxError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project()
            .inner
            .poll(cx)
            .map(|result| result.map_err(Into::into))
    }
}
