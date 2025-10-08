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

    pub fn producer(&self) -> crate::flashq::v1::producer_client::ProducerClient<Channel> {
        crate::flashq::v1::producer_client::ProducerClient::new(self.channel.clone())
    }

    pub fn consumer(&self) -> crate::flashq::v1::consumer_client::ConsumerClient<Channel> {
        crate::flashq::v1::consumer_client::ConsumerClient::new(self.channel.clone())
    }

    pub fn admin(&self) -> crate::flashq::v1::admin_client::AdminClient<Channel> {
        crate::flashq::v1::admin_client::AdminClient::new(self.channel.clone())
    }
}
