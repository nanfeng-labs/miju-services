//! The result slot between a task wrapper and its `Work` handle.
//!
//! At completion the wrapper offers the outcome to the handle. If the handle
//! is gone the outcome comes back and the wrapper drops it before it releases
//! its count. If the handle exists the outcome belongs to the handle from
//! that moment and the count is released at once; it never waits to be read.
#![forbid(unsafe_code)]

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

enum State<T> {
    Pending(Option<Waker>),
    Ready(T),
    Taken,
}

struct Inner<T> {
    state: State<T>,
    handle_gone: bool,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
}

/// The wrapper's end. Completing consumes it.
pub(crate) struct Sender<T>(Arc<Shared<T>>);

/// The submitter's end. Dropping it never cancels the work.
pub(crate) struct Receiver<T>(Arc<Shared<T>>);

pub(crate) fn slot<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            state: State::Pending(None),
            handle_gone: false,
        }),
    });
    (Sender(shared.clone()), Receiver(shared))
}

impl<T> Sender<T> {
    /// Offer the outcome. `None` means the handle now owns it. `Some` hands it
    /// back because the handle is gone; the caller drops it before releasing.
    pub(crate) fn complete(self, outcome: T) -> Option<T> {
        let waker = {
            let mut inner = self.0.inner.lock().expect("the slot runs no user code");
            if inner.handle_gone {
                return Some(outcome);
            }
            match std::mem::replace(&mut inner.state, State::Ready(outcome)) {
                State::Pending(waker) => waker,
                State::Ready(_) | State::Taken => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        None
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let (outcome, replaced) = {
            let mut inner = self.0.inner.lock().expect("the slot runs no user code");
            match std::mem::replace(&mut inner.state, State::Taken) {
                State::Ready(outcome) => (Some(outcome), None),
                State::Pending(previous) => {
                    inner.state = State::Pending(Some(cx.waker().clone()));
                    (None, previous)
                }
                State::Taken => panic!("work handle polled after it returned its outcome"),
            }
        };
        // A replaced waker is dropped outside the lock.
        drop(replaced);
        match outcome {
            Some(outcome) => Poll::Ready(outcome),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let leftover = match self.0.inner.lock() {
            Ok(mut inner) => {
                inner.handle_gone = true;
                std::mem::replace(&mut inner.state, State::Taken)
            }
            Err(_) => return,
        };
        // An unread outcome is dropped by whoever dropped the handle, outside the lock.
        drop(leftover);
    }
}
