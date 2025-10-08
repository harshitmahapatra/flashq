//! FlashQ client library.
//!
//! This crate provides a convenient client wrapper for connecting to FlashQ brokers
//! and accessing Producer, Consumer, and Admin services.

use tonic::transport::{Channel, Endpoint};

/// Convenience wrapper that provides typed clients for all services using a shared channel.
pub struct FlashqClient {
    channel: Channel,
}

impl FlashqClient {
    /// Connect to a FlashQ broker at the given URI, e.g., "http://127.0.0.1:50051".
    pub async fn connect<D: TryInto<Endpoint>>(dst: D) -> Result<Self, Box<dyn std::error::Error>>
    where
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let channel = Endpoint::new(dst)?.connect().await?;
        Ok(Self { channel })
    }

    pub fn producer(&self) -> flashq_proto::producer_client::ProducerClient<Channel> {
        flashq_proto::producer_client::ProducerClient::new(self.channel.clone())
    }

    pub fn consumer(&self) -> flashq_proto::consumer_client::ConsumerClient<Channel> {
        flashq_proto::consumer_client::ConsumerClient::new(self.channel.clone())
    }

    pub fn admin(&self) -> flashq_proto::admin_client::AdminClient<Channel> {
        flashq_proto::admin_client::AdminClient::new(self.channel.clone())
    }
}
