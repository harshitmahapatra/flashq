//! Concrete implementation of cluster service operations.
//!
//! This module provides the main implementation of the `ClusterService` trait,
//! integrating the metadata store with cluster client functionality.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    ClusterError,
    client::ClusterClient,
    metadata_store::MetadataStore,
    proto::{
        BrokerDirective, BrokerInfo, BrokerStatus, DescribeClusterResponse, HeartbeatRequest,
        HeartbeatResponse, PartitionEpochUpdate, PartitionHeartbeat, PartitionInfo,
        ReportPartitionStatusRequest, ReportPartitionStatusResponse, TopicAssignment,
    },
    traits::{ClusterBroker, ClusterService},
    types::*,
};

/// Implementation of ClusterService that coordinates cluster metadata operations.
///
/// This service wraps a metadata store, optionally uses a cluster client
/// for inter-node communication, and integrates with a FlashQ broker instance
/// to collect real-time partition status data.
pub struct ClusterServiceImpl {
    metadata_store: Arc<dyn MetadataStore>,
    cluster_client: Option<ClusterClient>,
    broker_id: BrokerId,
    flashq_broker: Option<Arc<dyn ClusterBroker>>,
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
            flashq_broker: None,
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
            flashq_broker: None,
        }
    }

    /// Create a new cluster service with metadata store and FlashQ broker.
    ///
    /// This creates a service that can collect real-time partition data from the broker.
    pub fn with_broker(
        metadata_store: Arc<dyn MetadataStore>,
        broker_id: BrokerId,
        flashq_broker: Arc<dyn ClusterBroker>,
    ) -> Self {
        Self {
            metadata_store,
            cluster_client: None,
            broker_id,
            flashq_broker: Some(flashq_broker),
        }
    }

    /// Create a new cluster service with metadata store, cluster client, and FlashQ broker.
    ///
    /// This creates a fully-featured distributed service with real-time broker integration.
    pub fn with_client_and_broker(
        metadata_store: Arc<dyn MetadataStore>,
        cluster_client: ClusterClient,
        broker_id: BrokerId,
        flashq_broker: Arc<dyn ClusterBroker>,
    ) -> Self {
        Self {
            metadata_store,
            cluster_client: Some(cluster_client),
            broker_id,
            flashq_broker: Some(flashq_broker),
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

    /// Get a reference to the FlashQ broker, if available.
    pub fn flashq_broker(&self) -> Option<&Arc<dyn ClusterBroker>> {
        self.flashq_broker.as_ref()
    }

    /// Start a background heartbeat task if this is a follower broker.
    ///
    /// This creates a periodic task that sends heartbeats to the controller
    /// using real partition data from the FlashQ broker.
    pub async fn start_follower_heartbeat_task(&self) -> Result<(), ClusterError> {
        if let Some(client) = self.cluster_client.as_ref() {
            let broker_id = self.broker_id;
            tracing::info!(%broker_id, "Starting follower heartbeat task");

            // Clone necessary components for the background task
            let mut client = client.clone();
            let metadata_store = self.metadata_store.clone();
            let flashq_broker = self.flashq_broker.clone();

            // Spawn the periodic heartbeat task
            tokio::spawn(async move {
                if let Err(e) = Self::run_streaming_heartbeat_task(
                    broker_id,
                    &mut client,
                    &metadata_store,
                    flashq_broker.as_ref(),
                )
                .await
                {
                    tracing::error!(%broker_id, error = %e, "Heartbeat task failed");
                }
            });

            tracing::info!(%broker_id, "Follower heartbeat task started successfully");
            Ok(())
        } else {
            tracing::debug!("No cluster client available, skipping heartbeat task");
            Ok(())
        }
    }

    /// Run the streaming heartbeat task in the background.
    ///
    /// This task establishes a bidirectional stream with the controller
    /// and sends periodic heartbeats while processing responses.
    async fn run_streaming_heartbeat_task(
        broker_id: BrokerId,
        client: &mut ClusterClient,
        metadata_store: &Arc<dyn MetadataStore>,
        flashq_broker: Option<&Arc<dyn ClusterBroker>>,
    ) -> Result<(), ClusterError> {
        use tokio_stream::StreamExt;

        // Start the heartbeat stream
        let (sender, mut receiver) = client.start_heartbeat_stream().await?;
        tracing::info!(%broker_id, "Heartbeat stream established with controller");

        // Create a periodic timer for sending heartbeats
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10)); // 10-second heartbeat interval
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Send periodic heartbeats
                _ = interval.tick() => {
                    match Self::send_heartbeat_on_stream(
                        broker_id,
                        &sender,
                        metadata_store,
                        flashq_broker,
                    ).await {
                        Ok(()) => {
                            tracing::trace!(%broker_id, "Heartbeat sent on stream");
                        }
                        Err(e) => {
                            tracing::warn!(%broker_id, error = %e, "Failed to send heartbeat on stream");
                            // Continue the loop - don't exit on send failures
                        }
                    }
                }

                // Process responses from controller
                response = receiver.next() => {
                    match response {
                        Some(Ok(heartbeat_response)) => {
                            tracing::debug!(
                                broker_id = %broker_id,
                                epoch_updates = heartbeat_response.epoch_updates.len(),
                                directives = heartbeat_response.directives.len(),
                                "Received heartbeat response from controller"
                            );

                            if let Err(e) = Self::process_heartbeat_response(
                                metadata_store,
                                flashq_broker,
                                heartbeat_response,
                            ).await {
                                tracing::error!(%broker_id, error = %e, "Failed to process heartbeat response");
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(%broker_id, error = %e, "Received error from heartbeat stream");
                            // Continue the loop - don't exit on response errors
                        }
                        None => {
                            tracing::info!(%broker_id, "Heartbeat stream closed by controller");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Send a single heartbeat on the established stream.
    async fn send_heartbeat_on_stream(
        broker_id: BrokerId,
        sender: &tokio::sync::mpsc::Sender<HeartbeatRequest>,
        metadata_store: &Arc<dyn MetadataStore>,
        flashq_broker: Option<&Arc<dyn ClusterBroker>>,
    ) -> Result<(), ClusterError> {
        // 1. Collect current partition status
        let partition_heartbeats =
            Self::collect_partition_heartbeats(metadata_store, broker_id, flashq_broker).await;

        // 2. Create and send HeartbeatRequest
        let request = HeartbeatRequest {
            broker_id: broker_id.into(),
            partitions: partition_heartbeats,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        tracing::debug!(
            broker_id = %broker_id,
            partition_count = request.partitions.len(),
            "Sending heartbeat on stream"
        );

        sender
            .send(request)
            .await
            .map_err(|_| ClusterError::Transport {
                context: "Failed to send heartbeat".to_string(),
                reason: "Channel closed".to_string(),
            })?;

        Ok(())
    }

    /// Collect partition status for heartbeat messages.
    async fn collect_partition_heartbeats(
        metadata_store: &Arc<dyn MetadataStore>,
        broker_id: BrokerId,
        flashq_broker: Option<&Arc<dyn ClusterBroker>>,
    ) -> Vec<PartitionHeartbeat> {
        let mut heartbeats = Vec::new();

        if let Some(broker) = flashq_broker {
            // Get all partitions assigned to this broker
            if let Ok(partitions) = broker.get_assigned_partitions().await {
                for (topic, partition_id) in partitions {
                    // Get current epoch from metadata store
                    let leader_epoch = metadata_store
                        .get_partition_epoch(&topic, partition_id)
                        .unwrap_or_else(|_| Epoch::from(0));

                    // Check if this broker is the leader for the partition
                    let is_leader = broker
                        .is_partition_leader(&topic, partition_id)
                        .await
                        .unwrap_or(false);

                    // Get high water mark and log start offset
                    let high_water_mark = broker
                        .get_high_water_mark(&topic, partition_id)
                        .await
                        .unwrap_or(0);

                    let log_start_offset = broker
                        .get_log_start_offset(&topic, partition_id)
                        .await
                        .unwrap_or(0);

                    // Get current in-sync replicas from metadata store
                    let current_in_sync_replicas = metadata_store
                        .get_in_sync_replicas(&topic, partition_id)
                        .map(|isr| isr.into_iter().map(|id| id.into()).collect())
                        .unwrap_or_else(|_| vec![broker_id.into()]);

                    heartbeats.push(PartitionHeartbeat {
                        topic: topic.clone(),
                        partition: partition_id.into(),
                        leader_epoch: leader_epoch.into(),
                        is_leader,
                        high_water_mark,
                        log_start_offset,
                        current_in_sync_replicas,
                        leader_override: None, // No leader override for normal heartbeats
                    });
                }
            }
        }

        heartbeats
    }

    /// Process heartbeat response from controller.
    async fn process_heartbeat_response(
        metadata_store: &Arc<dyn MetadataStore>,
        flashq_broker: Option<&Arc<dyn ClusterBroker>>,
        response: HeartbeatResponse,
    ) -> Result<(), ClusterError> {
        // Process epoch updates
        for epoch_update in response.epoch_updates {
            let topic = &epoch_update.topic;
            let partition_id = PartitionId::from(epoch_update.partition);
            let new_epoch = Epoch::from(epoch_update.new_epoch);

            // Apply epoch update if it's newer than current
            if let Ok(current_epoch) = metadata_store.get_partition_epoch(topic, partition_id) {
                if new_epoch > current_epoch {
                    // Update the epoch in metadata store
                    metadata_store.compare_and_set_epoch(
                        topic,
                        partition_id,
                        current_epoch,
                        new_epoch,
                    )?;
                    tracing::info!(%topic, %partition_id, old_epoch = %current_epoch, new_epoch = %new_epoch, "Applied epoch update from controller");
                }
            }
        }

        // Process directives
        for directive in response.directives {
            match BrokerDirective::try_from(directive) {
                Ok(BrokerDirective::None) => {
                    // No directive - ignore
                }
                Ok(BrokerDirective::Resync) => {
                    tracing::info!("Received RESYNC directive from controller");
                    // TODO: Implement resync logic - fetch fresh cluster state
                    // This would typically involve refreshing partition assignments
                    // and synchronizing with the controller's view of the cluster
                }
                Ok(BrokerDirective::Drain) => {
                    tracing::info!("Received DRAIN directive from controller");
                    // TODO: Implement drain logic - stop accepting new requests
                    // This would involve gracefully stopping new producer requests
                    // while allowing existing operations to complete
                }
                Ok(BrokerDirective::Shutdown) => {
                    tracing::warn!("Received SHUTDOWN directive from controller");
                    if let Some(broker) = flashq_broker {
                        if let Err(e) = broker.initiate_shutdown().await {
                            tracing::error!("Failed to initiate broker shutdown: {}", e);
                        }
                    } else {
                        tracing::info!("No FlashQ broker instance available for shutdown");
                    }
                }
                Err(_) => {
                    tracing::warn!("Received unknown directive: {}", directive);
                }
            }
        }

        Ok(())
    }

    /// Report partition status change to the controller.
    pub async fn report_partition_status_to_controller(
        &self,
        topic: &str,
        partition: PartitionId,
        leader: BrokerId,
        in_sync_replicas: Vec<BrokerId>,
        high_water_mark: u64,
        log_start_offset: u64,
    ) -> Result<(), ClusterError> {
        if let Some(mut client) = self.cluster_client.as_ref().cloned() {
            let request = ReportPartitionStatusRequest {
                topic: topic.to_string(),
                partition: partition.into(),
                leader: leader.into(),
                replicas: vec![], // TODO: Pass actual replicas list
                in_sync_replicas: in_sync_replicas.into_iter().map(|id| id.into()).collect(),
                high_water_mark,
                log_start_offset,
                timestamp: chrono::Utc::now().to_rfc3339(),
            };

            let response = client.report_partition_status(request).await?;

            if !response.accepted {
                tracing::warn!(
                    "Controller rejected partition status report: {}",
                    response.message
                );
            } else {
                tracing::debug!("Partition status report accepted by controller");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn describe_cluster(&self) -> Result<DescribeClusterResponse, ClusterError> {
        let manifest = self.metadata_store.export_to_manifest()?;

        // Get broker runtime status information
        let broker_statuses = self.metadata_store.list_brokers_with_status()?;
        let broker_status_map: std::collections::HashMap<BrokerId, BrokerRuntimeStatus> =
            broker_statuses.into_iter().collect();

        let brokers: Vec<BrokerInfo> = manifest
            .brokers
            .into_iter()
            .map(|broker| {
                let broker_id = broker.id;
                let status = broker_status_map.get(&broker_id);

                let (is_alive, last_heartbeat, is_draining) = if let Some(runtime_status) = status {
                    // Consider a broker alive if we have recent heartbeat data
                    // This logic can be enhanced with configurable heartbeat timeout
                    let now = chrono::Utc::now();
                    let heartbeat_age = now.signed_duration_since(runtime_status.last_heartbeat);
                    let is_alive = heartbeat_age.num_seconds() < 30; // 30 second timeout

                    (
                        is_alive,
                        runtime_status.last_heartbeat.to_rfc3339(),
                        runtime_status.is_draining,
                    )
                } else {
                    // Fallback for brokers without runtime status
                    (false, chrono::Utc::now().to_rfc3339(), false)
                };

                BrokerInfo {
                    broker_id: broker_id.into(),
                    host: broker.host,
                    port: broker.port as u32,
                    is_alive,
                    last_heartbeat: last_heartbeat.clone(),
                    status: Some(BrokerStatus {
                        is_alive,
                        last_heartbeat,
                        is_draining,
                    }),
                }
            })
            .collect();

        let topics: Vec<TopicAssignment> = manifest
            .topics
            .into_iter()
            .map(|(topic_name, topic_assignment)| {
                let partitions: Vec<PartitionInfo> = topic_assignment
                    .partitions
                    .into_iter()
                    .map(|partition| PartitionInfo {
                        topic: topic_name.clone(),
                        partition: partition.id.into(),
                        leader: partition.leader.into(),
                        replicas: partition.replicas.into_iter().map(|id| id.into()).collect(),
                        in_sync_replicas: partition
                            .in_sync_replicas
                            .into_iter()
                            .map(|id| id.into())
                            .collect(),
                        epoch: partition.epoch.into(),
                    })
                    .collect();

                TopicAssignment {
                    topic: topic_name,
                    partitions,
                }
            })
            .collect();

        Ok(DescribeClusterResponse {
            brokers,
            topics,
            controller_id: self.broker_id.into(), // Use current broker as controller for now
        })
    }

    async fn handle_heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClusterError> {
        let broker_id = BrokerId::from(request.broker_id);

        // Parse timestamp and record broker heartbeat
        let timestamp = chrono::DateTime::parse_from_rfc3339(&request.timestamp)
            .map_err(|e| ClusterError::from_parse_error(e, "parsing heartbeat timestamp"))?
            .with_timezone(&chrono::Utc);

        // Record broker heartbeat (assume not draining unless explicitly specified)
        self.metadata_store
            .record_broker_heartbeat(broker_id, timestamp, false)?;

        let mut epoch_updates = Vec::new();
        let mut directives = Vec::new();

        // Process partition heartbeats
        for partition_hb in request.partitions {
            let topic = &partition_hb.topic;
            let partition_id = PartitionId::from(partition_hb.partition);
            let reported_epoch = Epoch::from(partition_hb.leader_epoch);

            // Get current epoch and validate
            if let Ok(current_epoch) = self.metadata_store.get_partition_epoch(topic, partition_id)
            {
                // Check for epoch staleness - reject if reported epoch is behind
                if reported_epoch < current_epoch {
                    // Stale partition data - emit RESYNC directive
                    directives.push(BrokerDirective::Resync as i32);
                    continue;
                }

                // Check if epoch has advanced and needs update
                if reported_epoch > current_epoch {
                    // Try to advance the epoch using compare-and-set
                    if self.metadata_store.compare_and_set_epoch(
                        topic,
                        partition_id,
                        current_epoch,
                        reported_epoch,
                    )? {
                        // Epoch successfully updated
                        epoch_updates.push(PartitionEpochUpdate {
                            topic: topic.clone(),
                            partition: partition_id.into(),
                            new_epoch: reported_epoch.into(),
                            new_leader: if partition_hb.is_leader {
                                broker_id.into()
                            } else {
                                0 // No leader change indicated
                            },
                        });
                    } else {
                        // CAS failed - another broker updated the epoch
                        directives.push(BrokerDirective::Resync as i32);
                        continue;
                    }
                }
            }

            // Update partition offsets from heartbeat
            self.metadata_store.update_partition_offsets(
                topic,
                partition_id,
                partition_hb.high_water_mark,
                partition_hb.log_start_offset,
            )?;

            // Update in_sync_replicas based on current_in_sync_replicas list from heartbeat
            if let Ok(current_isr) = self
                .metadata_store
                .get_in_sync_replicas(topic, partition_id)
            {
                let reported_isr: HashSet<BrokerId> = partition_hb
                    .current_in_sync_replicas
                    .iter()
                    .map(|&id| BrokerId::from(id))
                    .collect();

                // Add newly in-sync replicas
                for &broker_id in &reported_isr {
                    if !current_isr.contains(&broker_id) {
                        self.metadata_store.update_in_sync_replica(
                            topic,
                            partition_id,
                            broker_id,
                            true,
                        )?;
                    }
                }

                // Remove out-of-sync replicas
                for &broker_id in &current_isr {
                    if !reported_isr.contains(&broker_id) {
                        self.metadata_store.update_in_sync_replica(
                            topic,
                            partition_id,
                            broker_id,
                            false,
                        )?;
                    }
                }
            }
        }

        Ok(HeartbeatResponse {
            epoch_updates,
            timestamp: chrono::Utc::now().to_rfc3339(),
            directives,
        })
    }

    async fn report_partition_status(
        &self,
        request: ReportPartitionStatusRequest,
    ) -> Result<ReportPartitionStatusResponse, ClusterError> {
        let topic = &request.topic;
        let partition_id = PartitionId::from(request.partition);
        let new_leader_id = BrokerId::from(request.leader);

        // Get current partition epoch for CAS operation
        let current_epoch = self
            .metadata_store
            .get_partition_epoch(topic, partition_id)?;
        let new_epoch = Epoch::from(current_epoch.0 + 1);

        // Update partition leader using the new epoch
        self.metadata_store
            .set_partition_leader(topic, partition_id, new_leader_id, new_epoch)?;

        // Try to persist the new epoch using compare-and-set to ensure atomicity
        let epoch_updated = self.metadata_store.compare_and_set_epoch(
            topic,
            partition_id,
            current_epoch,
            new_epoch,
        )?;

        if !epoch_updated {
            // CAS failed - another operation updated the epoch concurrently
            return Ok(ReportPartitionStatusResponse {
                accepted: false,
                message: "Partition epoch changed concurrently, retry required".to_string(),
            });
        }

        // Update in_sync_replicas based on the report
        let in_sync_replica_broker_ids: Vec<BrokerId> = request
            .in_sync_replicas
            .into_iter()
            .map(BrokerId::from)
            .collect();

        // Get current ISR to determine which replicas to add/remove
        if let Ok(current_isr) = self
            .metadata_store
            .get_in_sync_replicas(topic, partition_id)
        {
            let new_isr: HashSet<BrokerId> = in_sync_replica_broker_ids.into_iter().collect();

            // Add newly in-sync replicas
            for &broker_id in &new_isr {
                if !current_isr.contains(&broker_id) {
                    self.metadata_store.update_in_sync_replica(
                        topic,
                        partition_id,
                        broker_id,
                        true,
                    )?;
                }
            }

            // Remove out-of-sync replicas
            for &broker_id in &current_isr {
                if !new_isr.contains(&broker_id) {
                    self.metadata_store.update_in_sync_replica(
                        topic,
                        partition_id,
                        broker_id,
                        false,
                    )?;
                }
            }
        }

        // Update high water mark and log start offset
        self.metadata_store.update_partition_offsets(
            topic,
            partition_id,
            request.high_water_mark,
            request.log_start_offset,
        )?;

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
    use crate::manifest::{BrokerSpec, ClusterManifest};
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

        // First, add the broker to the manifest so heartbeats can be recorded
        let manifest = ClusterManifest {
            brokers: vec![BrokerSpec {
                id: BrokerId::from(1),
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: std::collections::HashMap::new(),
        };
        service
            .metadata_store()
            .load_from_manifest(manifest)
            .unwrap();

        let request = HeartbeatRequest {
            broker_id: 1,
            partitions: vec![],
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        let response = service.handle_heartbeat(request).await.unwrap();
        assert_eq!(response.epoch_updates.len(), 0);
        assert_eq!(response.directives.len(), 0);
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
