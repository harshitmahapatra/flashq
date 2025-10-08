//! Shared Protocol Buffer definitions for FlashQ.
//!
//! This crate contains all Protocol Buffer schemas and generated code used across
//! the FlashQ ecosystem. It consolidates both the broker API protocol (flashq.proto)
//! and the cluster coordination protocol (cluster.proto).

// Generated protobuf modules via `tonic_build` in build.rs

/// FlashQ broker API protocol
pub mod flashq {
    pub mod v1 {
        tonic::include_proto!("flashq.v1");
    }
}

/// FlashQ cluster coordination protocol
pub mod cluster {
    pub mod v1 {
        tonic::include_proto!("flashq.cluster.v1");
    }
}

// Re-export broker API types for convenience
pub use flashq::v1::*;

// Re-export client and server types for broker API
pub use flashq::v1::admin_client::AdminClient;
pub use flashq::v1::admin_server::AdminServer;
pub use flashq::v1::consumer_client::ConsumerClient;
pub use flashq::v1::consumer_server::ConsumerServer;
pub use flashq::v1::producer_client::ProducerClient;
pub use flashq::v1::producer_server::ProducerServer;

// Re-export cluster protocol client and server
pub use cluster::v1::cluster_client::ClusterClient;
pub use cluster::v1::cluster_server::ClusterServer;
