//! Concrete implementation of cluster service operations.
//!
//! This module provides the main implementation of the `ClusterService` trait,
//! integrating the metadata store with cluster client functionality.

use std::sync::Arc;
use async_trait::async_trait;

use crate::{
    ClusterError,
    client::ClusterClient,
    metadata_store::MetadataStore,
    proto::{
        BrokerInfo, DescribeClusterResponse, HeartbeatRequest, HeartbeatResponse,
        PartitionInfo, ReportPartitionStatusRequest, ReportPartitionStatusResponse,
        TopicAssignment,
    },
    traits::ClusterService,
    types::*,
};

/// Implementation of ClusterService that coordinates cluster metadata operations.
///
/// This service wraps a metadata store and optionally uses a cluster client
/// for inter-node communication in distributed scenarios.
pub struct ClusterServiceImpl {
    metadata_store: Arc<dyn MetadataStore>,
    cluster_client: Option<ClusterClient>,
    broker_id: BrokerId,
}

impl ClusterServiceImpl {
    /// Create a new cluster service with the given metadata store.
    ///
    /// This creates a standalone service suitable for single-node or controller scenarios.
    pub fn new(metadata_store: Arc<dyn MetadataStore>, broker_id: BrokerId) -> Self {
        Self {
            metadata_store,
            cluster_client: None,
            broker_id,
        }
    }

    /// Create a new cluster service with metadata store and cluster client.
    ///
    /// This creates a distributed service that can communicate with other cluster nodes.
    pub fn with_client(
        metadata_store: Arc<dyn MetadataStore>,
        cluster_client: ClusterClient,
        broker_id: BrokerId,
    ) -> Self {
        Self {
            metadata_store,
            cluster_client: Some(cluster_client),
            broker_id,
        }
    }

    /// Get the broker ID for this service instance.
    pub fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// Get a reference to the cluster client, if available.
    pub fn cluster_client(&self) -> Option<&ClusterClient> {
        self.cluster_client.as_ref()
    }

    /// Convert internal manifest data to proto response format.
    fn manifest_to_describe_response(&self) -> Result<DescribeClusterResponse, ClusterError> {
        let manifest = self.metadata_store.export_to_manifest()?;

        let brokers: Vec<BrokerInfo> = manifest.brokers.into_iter().map(|broker| {
            BrokerInfo {
                broker_id: broker.id.into(),
                host: broker.host,
                port: broker.port as u32,
                is_alive: true, // TODO: Implement broker liveness tracking
                last_heartbeat: chrono::Utc::now().to_rfc3339(),
            }
        }).collect();

        let topics: Vec<TopicAssignment> = manifest.topics.into_iter().map(|(topic_name, topic_assignment)| {
            let partitions: Vec<PartitionInfo> = topic_assignment.partitions.into_iter().map(|partition| {
                PartitionInfo {
                    topic: topic_name.clone(),
                    partition: partition.id.into(),
                    leader: partition.leader.into(),
                    replicas: partition.replicas.into_iter().map(|id| id.into()).collect(),
                    in_sync_replicas: partition.in_sync_replicas.into_iter().map(|id| id.into()).collect(),
                    epoch: partition.epoch.into(),
                }
            }).collect();

            TopicAssignment {
                topic: topic_name,
                partitions,
            }
        }).collect();

        Ok(DescribeClusterResponse {
            brokers,
            topics,
            controller_id: self.broker_id.into(), // Use current broker as controller for now
        })
    }
}

#[async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn describe_cluster(&self) -> Result<DescribeClusterResponse, ClusterError> {
        self.manifest_to_describe_response()
    }

    async fn handle_heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClusterError> {
        let _broker_id = BrokerId::from(request.broker_id);

        // Update broker liveness
        // TODO: Implement broker heartbeat tracking in metadata store

        let epoch_updates = Vec::new();

        // Process partition heartbeats
        for partition_hb in request.partitions {
            let topic = &partition_hb.topic;
            let partition_id = PartitionId::from(partition_hb.partition);

            // Check if epoch has changed and update in_sync_replicas based on current_in_sync_replicas field
            if let Ok(_current_epoch) = self.metadata_store.get_partition_epoch(topic, partition_id) {
                // For now, assume epoch is always current - we don't have epoch comparison in heartbeat
                // TODO: Add epoch tracking to PartitionHeartbeat in proto if needed
            }

            // Update in_sync_replicas based on current_in_sync_replicas list from heartbeat
            for in_sync_replica_broker_id in &partition_hb.current_in_sync_replicas {
                let broker_id = BrokerId::from(*in_sync_replica_broker_id);
                let _ = self.metadata_store.update_in_sync_replica(
                    topic,
                    partition_id,
                    broker_id,
                    true
                );
            }
        }

        Ok(HeartbeatResponse {
            epoch_updates,
            should_shutdown: false, // TODO: Implement shutdown coordination
            timestamp: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn report_partition_status(
        &self,
        request: ReportPartitionStatusRequest,
    ) -> Result<ReportPartitionStatusResponse, ClusterError> {
        let topic = &request.topic;
        let partition_id = PartitionId::from(request.partition);
        let leader_id = BrokerId::from(request.leader);

        // Update partition leader if changed
        if let Ok(current_leader) = self.metadata_store.get_partition_leader(topic, partition_id) {
            if current_leader != leader_id {
                // Note: MetadataStore trait doesn't have set_partition_leader, so skip for now
                self.metadata_store.bump_leader_epoch(topic, partition_id)?;
            }
        }

        // Update in_sync_replicas
        let in_sync_replica_broker_ids: Vec<BrokerId> = request.in_sync_replicas.into_iter()
            .map(BrokerId::from)
            .collect();

        for broker_id in &in_sync_replica_broker_ids {
            self.metadata_store.update_in_sync_replica(topic, partition_id, *broker_id, true)?;
        }

        // TODO: Update high water mark and log start offset tracking

        Ok(ReportPartitionStatusResponse {
            accepted: true,
            message: "Partition status updated successfully".to_string(),
        })
    }

    fn metadata_store(&self) -> &dyn MetadataStore {
        self.metadata_store.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_store::InMemoryMetadataStore;

    fn create_test_service() -> ClusterServiceImpl {
        let metadata_store = Arc::new(InMemoryMetadataStore::new());
        ClusterServiceImpl::new(metadata_store, BrokerId::from(1))
    }

    #[tokio::test]
    async fn test_describe_empty_cluster() {
        let service = create_test_service();
        let response = service.describe_cluster().await.unwrap();

        assert_eq!(response.brokers.len(), 0);
        assert_eq!(response.topics.len(), 0);
        assert_eq!(response.controller_id, 1);
    }

    #[tokio::test]
    async fn test_heartbeat_basic() {
        let service = create_test_service();

        let request = HeartbeatRequest {
            broker_id: 1,
            partitions: vec![],
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        let response = service.handle_heartbeat(request).await.unwrap();
        assert_eq!(response.epoch_updates.len(), 0);
        assert!(!response.should_shutdown);
    }

    #[tokio::test]
    async fn test_report_partition_status() {
        let service = create_test_service();

        // First, we need to set up some basic cluster state for the test to work
        // Since MetadataStore starts empty, we need to load some test data
        // For now, let's just test that the call returns an error for non-existent topic
        let request = ReportPartitionStatusRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            in_sync_replicas: vec![1],
            high_water_mark: 100,
            log_start_offset: 0,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        // This should fail because the topic doesn't exist yet
        let result = service.report_partition_status(request).await;
        assert!(result.is_err());
    }
}