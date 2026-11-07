#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod app;
pub mod cleanup;
pub mod observe;
pub mod report;
pub mod service;
pub mod work;

#[cfg(feature = "miju")]
#[cfg_attr(docsrs, doc(cfg(feature = "miju")))]
pub mod miju;

#[cfg(feature = "periodic")]
#[cfg_attr(docsrs, doc(cfg(feature = "periodic")))]
pub mod periodic;

mod task;

pub use app::{App, AppBuilder, AppHandle, BuildError, RunError, StartError};
pub use observe::ShutdownProgress;
pub use report::{ExitReport, Health};
pub use service::{Service, ServiceContext};

/// An owned application error that can cross task and thread boundaries.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error retained by immutable snapshots and reports.
pub type SharedError = std::sync::Arc<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "docsrs")]
#[doc = document_features::document_features!()]
pub mod features {}
