use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::ClusterError;
use crate::proto::{
    cluster_server::Cluster, DescribeClusterRequest, DescribeClusterResponse, HeartbeatRequest,
    HeartbeatResponse, ReportPartitionStatusRequest, ReportPartitionStatusResponse,
};
use crate::traits::ClusterService;

/// Server adapter that implements the Cluster service.
///
/// This adapter converts tonic requests into ClusterService trait calls,
/// providing the bridge between the network layer and business logic.
#[derive(Debug)]
pub struct ClusterServer<T: ClusterService> {
    cluster_service: Arc<T>,
}

impl<T: ClusterService> ClusterServer<T> {
    /// Create a new cluster server with the given cluster service implementation.
    pub fn new(cluster_service: Arc<T>) -> Self {
        Self { cluster_service }
    }

    /// Get a reference to the underlying cluster service.
    pub fn cluster_service(&self) -> &T {
        &self.cluster_service
    }
}

#[tonic::async_trait]
impl<T: ClusterService + 'static> Cluster for ClusterServer<T> {
    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        let response = self
            .cluster_service
            .describe_cluster()
            .await
            .map_err(cluster_error_to_status)?;

        Ok(Response::new(response))
    }

    type HeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut stream = request.into_inner();
        let cluster_service = Arc::clone(&self.cluster_service);

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(result) = stream.message().await.transpose() {
                match result {
                    Ok(heartbeat_request) => {
                        match cluster_service.handle_heartbeat(heartbeat_request).await {
                            Ok(response) => {
                                if tx.send(Ok(response)).await.is_err() {
                                    // Client disconnected
                                    break;
                                }
                            }
                            Err(err) => {
                                let status = cluster_error_to_status(err);
                                if tx.send(Err(status)).await.is_err() {
                                    // Client disconnected
                                    break;
                                }
                            }
                        }
                    }
                    Err(status) => {
                        if tx.send(Err(status)).await.is_err() {
                            // Client disconnected
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn report_partition_status(
        &self,
        request: Request<ReportPartitionStatusRequest>,
    ) -> Result<Response<ReportPartitionStatusResponse>, Status> {
        let response = self
            .cluster_service
            .report_partition_status(request.into_inner())
            .await
            .map_err(cluster_error_to_status)?;

        Ok(Response::new(response))
    }
}

/// Convert a ClusterError to a tonic Status for gRPC responses.
fn cluster_error_to_status(error: ClusterError) -> Status {
    match error {
        ClusterError::TopicNotFound { .. } => Status::not_found(error.to_string()),
        ClusterError::PartitionNotFound { .. } => Status::not_found(error.to_string()),
        ClusterError::BrokerNotFound { .. } => Status::not_found(error.to_string()),
        ClusterError::InvalidEpoch { .. } => Status::invalid_argument(error.to_string()),
        ClusterError::InvalidReplica { .. } => Status::invalid_argument(error.to_string()),
        ClusterError::InvalidManifest { .. } => Status::invalid_argument(error.to_string()),
        ClusterError::ManifestIo { .. } => Status::internal(error.to_string()),
        ClusterError::Transport { .. } => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_store::InMemoryMetadataStore;
    use crate::proto::{BrokerInfo, PartitionInfo, TopicAssignment};
    use crate::traits::ClusterService;

    /// Test implementation of ClusterService for testing the gRPC server.
    struct TestClusterService {
        metadata_store: InMemoryMetadataStore,
    }

    impl TestClusterService {
        fn new() -> Self {
            Self {
                metadata_store: InMemoryMetadataStore::new(),
            }
        }
    }

    #[tonic::async_trait]
    impl ClusterService for TestClusterService {
        async fn describe_cluster(&self) -> Result<DescribeClusterResponse, ClusterError> {
            // Create a simple test response
            let brokers = vec![BrokerInfo {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                is_alive: true,
                last_heartbeat: "2024-01-01T00:00:00Z".to_string(),
            }];

            let topics = vec![TopicAssignment {
                topic: "test-topic".to_string(),
                partitions: vec![PartitionInfo {
                    topic: "test-topic".to_string(),
                    partition: 0,
                    leader: 1,
                    replicas: vec![1],
                    in_sync_replicas: vec![1],
                    epoch: 1,
                }],
            }];

            Ok(DescribeClusterResponse {
                brokers,
                topics,
                controller_id: 1,
            })
        }

        async fn handle_heartbeat(
            &self,
            _request: HeartbeatRequest,
        ) -> Result<HeartbeatResponse, ClusterError> {
            // Echo back a simple response
            Ok(HeartbeatResponse {
                epoch_updates: vec![],
                should_shutdown: false,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            })
        }

        async fn report_partition_status(
            &self,
            _request: ReportPartitionStatusRequest,
        ) -> Result<ReportPartitionStatusResponse, ClusterError> {
            Ok(ReportPartitionStatusResponse {
                accepted: true,
                message: "Status accepted".to_string(),
            })
        }

        fn metadata_store(&self) -> &dyn crate::metadata_store::MetadataStore {
            &self.metadata_store
        }
    }

    #[tokio::test]
    async fn test_describe_cluster() {
        let service = Arc::new(TestClusterService::new());
        let server = ClusterServer::new(service);

        let request = Request::new(DescribeClusterRequest {});
        let response = server.describe_cluster(request).await.unwrap();

        let cluster_info = response.into_inner();
        assert_eq!(cluster_info.brokers.len(), 1);
        assert_eq!(cluster_info.brokers[0].broker_id, 1);
        assert_eq!(cluster_info.topics.len(), 1);
        assert_eq!(cluster_info.topics[0].topic, "test-topic");
        assert_eq!(cluster_info.controller_id, 1);
    }

    #[tokio::test]
    async fn test_report_partition_status() {
        let service = Arc::new(TestClusterService::new());
        let server = ClusterServer::new(service);

        let request = Request::new(ReportPartitionStatusRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            in_sync_replicas: vec![1],
            high_water_mark: 100,
            log_start_offset: 0,
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        });

        let response = server.report_partition_status(request).await.unwrap();
        let status_response = response.into_inner();
        assert!(status_response.accepted);
        assert_eq!(status_response.message, "Status accepted");
    }
}