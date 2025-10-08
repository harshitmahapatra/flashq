//! FlashQ broker implementation.
//!
//! This crate provides the broker services (Producer, Consumer, Admin) that handle
//! client requests and interact with the FlashQ core.

pub mod broker;
pub mod client;

// Re-export protocol buffer types from flashq-proto
pub use flashq_proto::flashq;
pub use flashq_proto::*;

// Re-export cluster types for convenience
pub use flashq_cluster::storage::ConsumerOffsetStore;
pub use flashq_proto::cluster;
