#![doc = include_str!("../README.md")]
// Only the JNI boundary (`jni::ffi` and the `export!` macro) uses unsafe code.
#![deny(unsafe_code)]
#![cfg_attr(not(feature = "jni"), forbid(unsafe_code))]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod app;
pub mod observe;
pub mod report;
pub mod scope;
pub mod service;

#[cfg(feature = "periodic")]
#[cfg_attr(docsrs, doc(cfg(feature = "periodic")))]
pub mod periodic;

#[cfg(feature = "miju")]
#[cfg_attr(docsrs, doc(cfg(feature = "miju")))]
pub mod miju;

#[cfg(feature = "jni")]
#[cfg_attr(docsrs, doc(cfg(feature = "jni")))]
pub mod jni;

mod cleanup;
mod contain;
mod driver;
mod gate;
mod registry;
mod slot;
mod sync;

pub use app::{App, AssemblyError, RunError, Spec, StartError};
pub use observe::{AppHandle, Lost, NotReady, Observer};
pub use report::{Failure, Health, Outstanding, Phase, Report, Snapshot, Status};
pub use scope::{Lease, Rejected, Scope, Work, WorkError, critical};
pub use service::{Context, Resource, ResourceError, Service};

/// An owned error that can cross task and thread boundaries.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error kept by snapshots and reports.
pub type SharedError = std::sync::Arc<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` with [`BoxError`] as the default error and `()` as the default value.
pub type Result<T = (), E = BoxError> = std::result::Result<T, E>;

#[cfg(feature = "docsrs")]
#[doc = document_features::document_features!()]
pub mod features {}
