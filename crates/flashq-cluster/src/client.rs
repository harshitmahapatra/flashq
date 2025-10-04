use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::error::ClusterError;
use crate::proto::{
    DescribeClusterRequest, DescribeClusterResponse, HeartbeatRequest, HeartbeatResponse,
    ReportPartitionStatusRequest, ReportPartitionStatusResponse,
    cluster_client::ClusterClient as TonicClusterClient,
};

/// Client for connecting to cluster services.
///
/// This client provides convenient methods for brokers to communicate with
/// cluster coordinators, handling connection management and error conversion.
#[derive(Debug, Clone)]
pub struct ClusterClient {
    client: TonicClusterClient<Channel>,
}

impl ClusterClient {
    /// Connect to a cluster service at the given endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, ClusterError>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::connect_with_endpoint_config(dst, |endpoint| endpoint).await
    }

    /// Connect to a cluster service with custom connection timeout.
    pub async fn connect_with_timeout<D>(dst: D, timeout: Duration) -> Result<Self, ClusterError>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::connect_with_endpoint_config(dst, |endpoint| endpoint.timeout(timeout)).await
    }

    /// Internal helper method to reduce duplication between connect methods.
    async fn connect_with_endpoint_config<D, F>(dst: D, config_fn: F) -> Result<Self, ClusterError>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        F: FnOnce(Endpoint) -> Endpoint,
    {
        let endpoint = dst
            .try_into()
            .map_err(|e| ClusterError::from_transport_error(e.into(), "Invalid endpoint"))?;

        let configured_endpoint = config_fn(endpoint);

        let client = TonicClusterClient::connect(configured_endpoint)
            .await
            .map_err(|e| ClusterError::from_transport_error(e, "Failed to connect"))?;

        Ok(Self { client })
    }

    /// Get information about the cluster state.
    ///
    /// Returns details about all brokers, topics, and partition assignments.
    pub async fn describe_cluster(&mut self) -> Result<DescribeClusterResponse, ClusterError> {
        let request = Request::new(DescribeClusterRequest {});

        let response = self
            .client
            .describe_cluster(request)
            .await
            .map_err(status_to_cluster_error)?;

        Ok(response.into_inner())
    }

    /// Start a bidirectional heartbeat stream with the cluster.
    ///
    /// Returns a tuple of (sender, receiver) for sending heartbeat requests
    /// and receiving heartbeat responses.
    pub async fn start_heartbeat_stream(
        &mut self,
    ) -> Result<
        (
            mpsc::Sender<HeartbeatRequest>,
            impl StreamExt<Item = Result<HeartbeatResponse, ClusterError>>,
        ),
        ClusterError,
    > {
        let (tx, rx) = mpsc::channel(128);
        let request_stream = ReceiverStream::new(rx);

        let response_stream = self
            .client
            .heartbeat(Request::new(request_stream))
            .await
            .map_err(status_to_cluster_error)?
            .into_inner();

        let mapped_stream = response_stream.map(|result| result.map_err(status_to_cluster_error));

        Ok((tx, mapped_stream))
    }

    /// Report the status of a partition to the cluster.
    ///
    /// This is used to inform the cluster coordinator about partition state changes,
    /// such as leadership changes or ISR updates.
    pub async fn report_partition_status(
        &mut self,
        request: ReportPartitionStatusRequest,
    ) -> Result<ReportPartitionStatusResponse, ClusterError> {
        let response = self
            .client
            .report_partition_status(Request::new(request))
            .await
            .map_err(status_to_cluster_error)?;

        Ok(response.into_inner())
    }

    /// Get a mutable reference to the underlying tonic client.
    ///
    /// This allows access to lower-level tonic functionality if needed.
    pub fn client_mut(&mut self) -> &mut TonicClusterClient<Channel> {
        &mut self.client
    }

    /// Get a reference to the underlying tonic client.
    pub fn client(&self) -> &TonicClusterClient<Channel> {
        &self.client
    }
}

/// Convert a tonic Status to a ClusterError.
fn status_to_cluster_error(status: Status) -> ClusterError {
    match status.code() {
        tonic::Code::NotFound => {
            if status.message().contains("topic") {
                ClusterError::TopicNotFound {
                    topic: extract_identifier_from_message(status.message()),
                }
            } else if status.message().contains("broker") {
                ClusterError::BrokerNotFound {
                    broker_id: 0, // Default broker ID since we can't extract it
                }
            } else {
                ClusterError::from_transport_error(status.message(), "Not found")
            }
        }
        tonic::Code::InvalidArgument => {
            ClusterError::from_transport_error(status.message(), "Invalid argument")
        }
        tonic::Code::Internal => {
            ClusterError::from_transport_error(status.message(), "Internal error")
        }
        tonic::Code::Unavailable => {
            ClusterError::from_transport_error(status.message(), "Service unavailable")
        }
        tonic::Code::DeadlineExceeded => {
            ClusterError::from_transport_error(status.message(), "Request timeout")
        }
        _ => ClusterError::from_transport_error(status.message(), "RPC error"),
    }
}

/// Extract an identifier from an error message (best effort).
fn extract_identifier_from_message(message: &str) -> String {
    // Try to extract quoted identifiers or fall back to the whole message
    if let Some(start) = message.find('"') {
        if let Some(end) = message[start + 1..].find('"') {
            return message[start + 1..start + 1 + end].to_string();
        }
    }
    message.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_to_cluster_error_conversion() {
        // Test NotFound with topic
        let status = Status::not_found("Topic 'test-topic' not found");
        let error = status_to_cluster_error(status);
        matches!(error, ClusterError::TopicNotFound { .. });

        // Test NotFound with broker
        let status = Status::not_found("Broker not found");
        let error = status_to_cluster_error(status);
        matches!(error, ClusterError::BrokerNotFound { .. });

        // Test InvalidArgument
        let status = Status::invalid_argument("Invalid partition ID");
        let error = status_to_cluster_error(status);
        matches!(error, ClusterError::Transport { .. });

        // Test Internal
        let status = Status::internal("Internal server error");
        let error = status_to_cluster_error(status);
        matches!(error, ClusterError::Transport { .. });
    }

    #[test]
    fn test_extract_identifier_from_message() {
        assert_eq!(
            extract_identifier_from_message("Topic \"test-topic\" not found"),
            "test-topic"
        );
        assert_eq!(
            extract_identifier_from_message("Broker 'broker-1' not found"),
            "Broker 'broker-1' not found"
        );
        assert_eq!(
            extract_identifier_from_message("Simple message"),
            "Simple message"
        );
    }
}
