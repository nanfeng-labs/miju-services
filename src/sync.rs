//! The synchronization types `gate`, `registry` and `slot` use.
//!
//! They import from here, not from `std` and `tokio` directly, so that the
//! loom model (outside this repository) can compile those three files
//! unchanged against loom's types and a model abort handle.
#![forbid(unsafe_code)]

pub(crate) use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
pub(crate) use tokio::task::AbortHandle;
