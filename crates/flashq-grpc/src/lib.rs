//! gRPC interface for FlashQ.
//! This crate exposes the generated protobuf/gRPC code and will later
//! provide server and client helpers built on top of `flashq` core.

// Generated protobuf modules via `tonic_build` in build.rs
pub mod flashq {
    pub mod v1 {
        tonic::include_proto!("flashq.v1");
    }
}

pub use flashq::v1::*;

pub mod client;
pub mod server;

// Re-export v1 API service clients/servers for ergonomics
pub use flashq::v1::admin_client::AdminClient;
pub use flashq::v1::admin_server::AdminServer;
pub use flashq::v1::consumer_client::ConsumerClient;
pub use flashq::v1::consumer_server::ConsumerServer;
pub use flashq::v1::producer_client::ProducerClient;
pub use flashq::v1::producer_server::ProducerServer;
