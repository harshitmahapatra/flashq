//! FlashQ cluster metadata management.
//!
//! This crate provides cluster metadata management for FlashQ, including broker/topic metadata,
//! leader epochs, and in-sync replica tracking. It exposes control-plane services over gRPC
//! for broker communication.

pub mod error;
pub mod manifest;
pub mod types;

pub use error::ClusterError;

// Re-export logging macros for consistent usage across the crate
pub use log::{debug, error, info, trace, warn};
