//! # Journal

#![allow(dead_code)] // TODO: Remove after initial implementation.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

// Internally visible modules.
pub(crate) mod log;
pub(crate) mod ring;
pub(crate) mod schema;

// Publicly exposed types.
pub use log::{Log, LogBuf, LogBufIter};

/// Result from operations where buffers are shared.
pub type BufResult<T, E> = Result<T, BufError<T, E>>;

/// Error from operations where buffers are shared.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BufError<T, E>(pub E, pub T);
