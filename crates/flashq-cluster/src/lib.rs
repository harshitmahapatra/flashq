//! FlashQ cluster metadata management.
//!
//! This crate provides cluster metadata management for FlashQ, including broker/topic metadata,
//! leader epochs, and in-sync replica tracking. It exposes control-plane services over gRPC
//! for broker communication.

pub mod client;
pub mod error;
pub mod manifest;
pub mod metadata_store;
pub mod server;
pub mod service;
pub mod traits;
pub mod types;

// Generated protobuf/gRPC modules
pub mod proto {
    tonic::include_proto!("flashq.cluster");
}

pub use error::ClusterError;

// Re-export commonly used metadata store types for ergonomics
pub use metadata_store::{InMemoryMetadataStore, MetadataBackend, MetadataStore};

// Re-export cluster service traits
pub use traits::{ClusterBroker, ClusterService};

// Re-export cluster server and client adapters
pub use client::ClusterClient;
pub use server::ClusterServer;

// Re-export cluster service implementation
pub use service::ClusterServiceImpl;

// Re-export core flashq types and services for flashq-grpc
pub use flashq::{FlashQ, Record, RecordWithOffset};
pub mod storage {
    pub use flashq::*;
}

// Re-export logging macros for consistent usage across the crate
pub use log::{debug, error, info, trace, warn};
