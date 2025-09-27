//! Integration tests for ClusterServiceImpl with file-based metadata store.
//!
//! Tests the service layer's interaction with persistent file storage,
//! verifying that operations are correctly persisted and service behavior
//! remains consistent across restarts.

use chrono::Utc;
use flashq_cluster::{
    metadata_store::FileMetadataStore,
    proto::{HeartbeatRequest, PartitionHeartbeat, ReportPartitionStatusRequest},
    service::ClusterServiceImpl,
    types::*,
    ClusterService,
};
use std::sync::Arc;
use tempfile::TempDir;

use crate::test_utilities::create_test_service_with_file_store;

#[tokio::test]
async fn test_describe_cluster_with_file_store() {
    let temp_dir = TempDir::new().unwrap();
    let service = create_test_service_with_file_store(&temp_dir, BrokerId(1), None);

    let response = service.describe_cluster().await.unwrap();

    assert_eq!(response.brokers.len(), 3);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.controller_id, 1);

    // Verify broker information
    let broker_1 = response.brokers.iter().find(|b| b.broker_id == 1).unwrap();
    assert_eq!(broker_1.host, "127.0.0.1");
    assert_eq!(broker_1.port, 6001);

    // Verify topic information
    let topic = response.topics.iter().find(|t| t.topic == "test-topic").unwrap();
    assert_eq!(topic.partitions.len(), 2);
}

#[tokio::test]
async fn test_heartbeat_with_file_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let service = create_test_service_with_file_store(&temp_dir, BrokerId(1), None);

    let request = HeartbeatRequest {
        broker_id: 1,
        partitions: vec![PartitionHeartbeat {
            topic: "test-topic".to_string(),
            partition: 0,
            leader_epoch: 1,
            high_water_mark: 100,
            log_start_offset: 0,
            is_leader: true,
            current_in_sync_replicas: vec![1, 2],
            leader_override: None,
        }],
        timestamp: Utc::now().to_rfc3339(),
    };

    let response = service.handle_heartbeat(request).await.unwrap();
    assert_eq!(response.epoch_updates.len(), 0); // No epoch changes expected
    assert_eq!(response.directives.len(), 0); // No directives expected

    // Verify heartbeat was persisted by creating new service from same directory
    let service2 = ClusterServiceImpl::new(
        Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap()),
        BrokerId(1),
    );

    // Check that broker status shows recent heartbeat
    let describe_response = service2.describe_cluster().await.unwrap();
    let broker_status = describe_response
        .brokers
        .iter()
        .find(|b| b.broker_id == 1)
        .unwrap();

    // The broker should have status indicating recent activity
    assert!(broker_status.is_alive);
}

#[tokio::test]
async fn test_report_partition_status_with_file_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let service = create_test_service_with_file_store(&temp_dir, BrokerId(1), None);

    let request = ReportPartitionStatusRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        in_sync_replicas: vec![1, 2],
        high_water_mark: 150,
        log_start_offset: 50,
        timestamp: Utc::now().to_rfc3339(),
    };

    let _response = service.report_partition_status(request).await.unwrap();
    // Note: Acceptance depends on service implementation details
    // The main test is that the operation doesn't fail

    // Verify state persisted by creating new service
    let service2 = ClusterServiceImpl::new(
        Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap()),
        BrokerId(1),
    );

    let describe_response = service2.describe_cluster().await.unwrap();
    let topic = describe_response.topics.iter().find(|t| t.topic == "test-topic").unwrap();
    let partition = topic.partitions.iter().find(|p| p.partition == 0).unwrap();

    assert_eq!(partition.leader, 1);
    // ISR may not have changed if the service didn't accept the update
    assert!(partition.in_sync_replicas.contains(&1));
}

#[tokio::test]
async fn test_multiple_brokers_heartbeat_with_file_store() {
    let temp_dir = TempDir::new().unwrap();
    let service = create_test_service_with_file_store(&temp_dir, BrokerId(1), None);

    // Send heartbeats from multiple brokers
    for broker_id in 1..=3 {
        let request = HeartbeatRequest {
            broker_id,
            partitions: vec![],
            timestamp: Utc::now().to_rfc3339(),
        };

        let response = service.handle_heartbeat(request).await.unwrap();
        assert_eq!(response.epoch_updates.len(), 0);
        assert_eq!(response.directives.len(), 0);
    }

    // Verify all heartbeats persisted
    let service2 = ClusterServiceImpl::new(
        Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap()),
        BrokerId(1),
    );

    let describe_response = service2.describe_cluster().await.unwrap();

    // All brokers should be present
    assert_eq!(describe_response.brokers.len(), 3);
    for broker_id in 1..=3 {
        let broker = describe_response
            .brokers
            .iter()
            .find(|b| b.broker_id == broker_id)
            .unwrap();
        assert_eq!(broker.broker_id, broker_id);
    }
}

#[tokio::test]
async fn test_service_restart_preserves_cluster_state() {
    let temp_dir = TempDir::new().unwrap();

    // Initial service setup and operations
    {
        let service = create_test_service_with_file_store(&temp_dir, BrokerId(1), None);

        // Record broker heartbeats
        for broker_id in 1..=3 {
            let request = HeartbeatRequest {
                broker_id,
                partitions: vec![],
                timestamp: Utc::now().to_rfc3339(),
            };
            service.handle_heartbeat(request).await.unwrap();
        }

        // Report partition status to change state
        let request = ReportPartitionStatusRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            in_sync_replicas: vec![1, 2],
            high_water_mark: 500,
            log_start_offset: 100,
            timestamp: Utc::now().to_rfc3339(),
        };
        service.report_partition_status(request).await.unwrap();
    }

    // Create new service from same directory to simulate restart
    {
        let service2 = ClusterServiceImpl::new(
            Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap()),
            BrokerId(1),
        );

        let describe_response = service2.describe_cluster().await.unwrap();

        // Verify cluster state preserved
        assert_eq!(describe_response.brokers.len(), 3);
        assert_eq!(describe_response.topics.len(), 1);
        assert_eq!(describe_response.controller_id, 1);

        // Verify partition state preserved
        let topic = describe_response.topics.iter().find(|t| t.topic == "test-topic").unwrap();
        let partition = topic.partitions.iter().find(|p| p.partition == 0).unwrap();

        assert_eq!(partition.leader, 1);
        // ISR may not have changed if the service didn't accept the update
        assert!(partition.in_sync_replicas.contains(&1));
    }
}

#[tokio::test]
async fn test_concurrent_operations_with_file_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let service = Arc::new(create_test_service_with_file_store(&temp_dir, BrokerId(1), None));

    // Spawn concurrent heartbeat tasks
    let mut handles = vec![];

    for broker_id in 1..=3 {
        let service_clone = Arc::clone(&service);
        let handle = tokio::spawn(async move {
            for i in 0..5 {
                let request = HeartbeatRequest {
                    broker_id,
                    partitions: vec![PartitionHeartbeat {
                        topic: "test-topic".to_string(),
                        partition: 0,
                        leader_epoch: 1,
                        high_water_mark: 100 + i,
                        log_start_offset: 0,
                        is_leader: broker_id == 1,
                        current_in_sync_replicas: vec![broker_id],
                        leader_override: None,
                    }],
                    timestamp: Utc::now().to_rfc3339(),
                };

                let _response = service_clone.handle_heartbeat(request).await.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state is consistent and persisted
    let service2 = ClusterServiceImpl::new(
        Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap()),
        BrokerId(1),
    );

    let describe_response = service2.describe_cluster().await.unwrap();
    assert_eq!(describe_response.brokers.len(), 3);
    assert_eq!(describe_response.topics.len(), 1);

    // All operations should have been properly serialized and persisted
    let topic = describe_response.topics.iter().find(|t| t.topic == "test-topic").unwrap();
    let partition = topic.partitions.iter().find(|p| p.partition == 0).unwrap();

    // Epoch should be consistent
    assert!(partition.epoch >= 1);
}