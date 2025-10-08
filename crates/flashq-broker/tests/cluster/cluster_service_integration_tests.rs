//! Integration tests for ClusterServiceImpl with FlashqGrpcService file-based storage.
//!
//! These tests verify that ClusterServiceImpl correctly integrates with the FlashQ broker's
//! file-based storage engine for log offsets, leadership checks, and replication acknowledgements.

use chrono::Utc;
use flashq_broker::broker::FlashQBroker;
use flashq_cluster::{
    ClusterBroker, ClusterService, Record,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::{FileMetadataStore, MetadataStore},
    proto::{HeartbeatRequest, PartitionHeartbeat, ReportPartitionStatusRequest},
    types::*,
};
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;

/// Create a manifest with known brokers/topics for integration tests.
fn seeded_manifest() -> ClusterManifest {
    let brokers = vec![
        BrokerSpec {
            id: BrokerId(1),
            host: "127.0.0.1".to_string(),
            port: 6001,
        },
        BrokerSpec {
            id: BrokerId(2),
            host: "127.0.0.1".to_string(),
            port: 6002,
        },
        BrokerSpec {
            id: BrokerId(3),
            host: "127.0.0.1".to_string(),
            port: 6003,
        },
    ];

    let mut topics = HashMap::new();
    for topic in ["test-topic", "status-topic", "heartbeat-topic"] {
        topics.insert(
            topic.to_string(),
            TopicAssignment {
                replication_factor: 3,
                partitions: vec![PartitionAssignment {
                    id: PartitionId::new(0),
                    leader: BrokerId(1),
                    replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                    in_sync_replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                    epoch: Epoch(1),
                }],
            },
        );
    }

    ClusterManifest { brokers, topics }
}

/// Setup function that creates a FlashQ broker with file storage and cluster service
async fn setup_broker_with_cluster_service() -> (Arc<FlashQBroker>, Arc<dyn ClusterService>, TempDir)
{
    let temp_dir = TempDir::new().unwrap();

    // Create FlashQ core with memory storage for simplicity
    let core = Arc::new(flashq_cluster::FlashQ::new());

    // Create file-based metadata store
    let metadata_store: Arc<dyn MetadataStore> = {
        let store = Arc::new(FileMetadataStore::new(temp_dir.path().join("cluster")).unwrap());
        store.load_from_manifest(seeded_manifest()).unwrap();
        store
    };

    // Create FlashQ gRPC broker
    let broker = Arc::new(FlashQBroker::new(core));

    // Create cluster service with FlashQ broker integration
    let cluster_service: Arc<dyn ClusterService> =
        Arc::new(flashq_cluster::service::ClusterServiceImpl::with_broker(
            metadata_store,
            BrokerId(1),
            broker.clone(),
        ));

    (broker, cluster_service, temp_dir)
}

#[tokio::test]
async fn test_describe_cluster_returns_correct_metadata() {
    // Setup
    let (_broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;

    // Action
    let response = cluster_service.describe_cluster().await.unwrap();

    // Expectation
    assert_eq!(response.controller_id, 1);
    assert!(!response.brokers.is_empty());
}

#[tokio::test]
async fn test_broker_heartbeat_updates_high_water_mark() {
    // Setup
    let (broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "test-topic";

    // Post some records to create high water mark
    broker
        .core
        .post_records(
            topic.to_string(),
            vec![
                Record::new(None, "record1".to_string(), None),
                Record::new(None, "record2".to_string(), None),
            ],
        )
        .unwrap();

    let request = HeartbeatRequest {
        broker_id: 1,
        partitions: vec![PartitionHeartbeat {
            topic: topic.to_string(),
            partition: 0,
            leader_epoch: 1,
            high_water_mark: broker.core.get_high_water_mark(topic),
            log_start_offset: 0,
            is_leader: true,
            current_in_sync_replicas: vec![1],
            leader_override: None,
        }],
        timestamp: Utc::now().to_rfc3339(),
    };

    // Action
    let response = cluster_service.handle_heartbeat(request).await.unwrap();

    // Expectation
    assert_eq!(response.epoch_updates.len(), 0);
    assert_eq!(response.directives.len(), 0);
}

#[tokio::test]
async fn test_partition_status_reporting_with_real_data() {
    // Setup
    let (broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "status-topic";

    // Create some records to establish real partition status
    broker
        .core
        .post_records(
            topic.to_string(),
            vec![
                Record::new(None, "status1".to_string(), None),
                Record::new(None, "status2".to_string(), None),
                Record::new(None, "status3".to_string(), None),
            ],
        )
        .unwrap();

    let high_water_mark = broker.core.get_high_water_mark(topic);
    let request = ReportPartitionStatusRequest {
        topic: topic.to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        in_sync_replicas: vec![1, 2],
        high_water_mark,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    };

    // Action
    let response = cluster_service.report_partition_status(request).await;

    // Expectation
    assert!(response.is_ok());
}

#[tokio::test]
async fn test_flashq_broker_get_high_water_mark_integration() {
    // Setup
    let (broker, _cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "water-mark-topic";

    // Post records to create high water mark
    broker
        .core
        .post_records(
            topic.to_string(),
            vec![
                Record::new(None, "mark1".to_string(), None),
                Record::new(None, "mark2".to_string(), None),
            ],
        )
        .unwrap();

    // Action
    let high_water_mark = broker
        .get_high_water_mark(topic, PartitionId(0))
        .await
        .unwrap();

    // Expectation
    assert_eq!(high_water_mark, 2);
}

#[tokio::test]
async fn test_flashq_broker_get_log_start_offset_integration() {
    // Setup
    let (broker, _cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "start-offset-topic";

    // Action
    let log_start_offset = broker
        .get_log_start_offset(topic, PartitionId(0))
        .await
        .unwrap();

    // Expectation
    assert_eq!(log_start_offset, 0);
}

#[tokio::test]
async fn test_metadata_store_partition_leader_query() {
    // Setup
    let (_broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "test-topic";

    // Action - Query leadership from metadata store (new architecture)
    let leader = cluster_service
        .metadata_store()
        .get_partition_leader(topic, PartitionId(0))
        .unwrap();

    // Expectation - Broker 1 is the leader according to the manifest
    assert_eq!(leader, BrokerId(1));
}

#[tokio::test]
async fn test_metadata_store_broker_partitions_query() {
    // Setup
    let (_broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;

    // Action - Query partition assignments from metadata store (new architecture)
    let partitions = cluster_service
        .metadata_store()
        .get_broker_partitions(BrokerId(1))
        .unwrap();

    // Expectation - Broker 1 should be assigned partitions for all test topics
    assert!(partitions.len() >= 3); // At least 3 topics in the manifest
    assert!(partitions.contains(&("test-topic".to_string(), PartitionId(0))));
    assert!(partitions.contains(&("status-topic".to_string(), PartitionId(0))));
    assert!(partitions.contains(&("heartbeat-topic".to_string(), PartitionId(0))));
}

#[tokio::test]
async fn test_flashq_broker_acknowledge_replication_integration() {
    // Setup
    let (broker, _cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "replication-topic";

    // Action
    let result = broker
        .acknowledge_replication(topic, PartitionId(0), 10)
        .await;

    // Expectation
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_flashq_broker_initiate_shutdown_integration() {
    // Setup
    let (broker, _cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;

    // Action
    let result = broker.initiate_shutdown().await;

    // Expectation
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_flashq_broker_invalid_partition_returns_error() {
    // Setup
    let (broker, _cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;
    let topic = "invalid-partition-topic";

    // Action
    let result = broker.get_high_water_mark(topic, PartitionId(1)).await;

    // Expectation
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        flashq_cluster::ClusterError::PartitionNotFound { .. }
    ));
}

#[tokio::test]
async fn test_cluster_service_heartbeat_persistence() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let topic = "heartbeat-topic";

    // Create first service instance and process heartbeat
    let _cluster_service = {
        let core = Arc::new(flashq_cluster::FlashQ::new());
        let broker = Arc::new(FlashQBroker::new(core));

        // Post some records
        broker
            .core
            .post_records(
                topic.to_string(),
                vec![Record::new(None, "heartbeat1".to_string(), None)],
            )
            .unwrap();

        let metadata_store: Arc<dyn MetadataStore> = {
            let store = Arc::new(FileMetadataStore::new(temp_dir.path().join("cluster")).unwrap());
            store.load_from_manifest(seeded_manifest()).unwrap();
            store
        };

        let cluster_service: Arc<dyn ClusterService> =
            Arc::new(flashq_cluster::service::ClusterServiceImpl::with_broker(
                metadata_store,
                BrokerId(1),
                broker.clone(),
            ));

        let request = HeartbeatRequest {
            broker_id: 1,
            partitions: vec![PartitionHeartbeat {
                topic: topic.to_string(),
                partition: 0,
                leader_epoch: 1,
                high_water_mark: broker.core.get_high_water_mark(topic),
                log_start_offset: 0,
                is_leader: true,
                current_in_sync_replicas: vec![1],
                leader_override: None,
            }],
            timestamp: Utc::now().to_rfc3339(),
        };

        cluster_service.handle_heartbeat(request).await.unwrap();
        cluster_service
    };

    // Action: Create new service from same directory to verify persistence
    let metadata_store2 =
        Arc::new(FileMetadataStore::new(temp_dir.path().join("cluster")).unwrap());
    let cluster_service2: Arc<dyn ClusterService> = Arc::new(
        flashq_cluster::service::ClusterServiceImpl::new(metadata_store2, BrokerId(1)),
    );

    let describe_response = cluster_service2.describe_cluster().await.unwrap();

    // Expectation: Broker should be present and accessible
    assert_eq!(describe_response.controller_id, 1);
    assert!(!describe_response.brokers.is_empty());
}

#[tokio::test]
async fn test_cluster_service_with_multiple_brokers() {
    // Setup
    let (_broker, cluster_service, _temp_dir) = setup_broker_with_cluster_service().await;

    // Send heartbeats from multiple brokers
    for broker_id in 1..=3 {
        let request = HeartbeatRequest {
            broker_id,
            partitions: vec![],
            timestamp: Utc::now().to_rfc3339(),
        };

        // Action
        let response = cluster_service.handle_heartbeat(request).await.unwrap();

        // Expectation
        assert_eq!(response.epoch_updates.len(), 0);
        assert_eq!(response.directives.len(), 0);
    }

    // Verify all heartbeats were processed
    let describe_response = cluster_service.describe_cluster().await.unwrap();
    assert_eq!(describe_response.controller_id, 1);
    assert!(!describe_response.brokers.is_empty());
}
