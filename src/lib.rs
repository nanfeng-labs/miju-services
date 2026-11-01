#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(feature = "docsrs", doc = document_features::document_features!())]

pub mod app;
pub mod cleanup;

pub mod observe;

pub mod report;
pub mod service;
mod util;
pub mod work;

pub use app::{AppHandle, AppOptions, Application, ApplicationBuilder, BuildError, StartError};
pub use cleanup::{CleanupId, CleanupRejected, CleanupReport};
pub use observe::{AppObserver, DriverLost, ReadyError, SnapshotReceiver};
pub use report::*;
pub use service::{
    ContextClosed, Criticality, Lifetime, Service, ServiceContext, ServiceId, ServiceSpec,
    service_fn,
};
pub use work::{
    SpawnError, WorkCompletion, WorkContext, WorkHandle, WorkId, WorkOutcome, WorkReport,
    WorkScope, WorkSpec,
};

/// An owned application error crossing the heterogeneous service boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A clonable error retaining its original source chain.
pub type SharedError = std::sync::Arc<dyn std::error::Error + Send + Sync + 'static>;
