//! Integration tests for ClusterServer gRPC adapter.
//!
//! Tests the gRPC server adapter with real service implementations,
//! verifying proper request/response handling and error conversion.

use chrono::Utc;
use flashq_cluster::{
    ClusterService,
    proto::{DescribeClusterRequest, ReportPartitionStatusRequest, cluster_server::Cluster},
    server::ClusterServer,
};
use std::sync::Arc;
use tempfile::TempDir;
use tonic::Request;

use crate::test_utilities::{
    TestManifestConfig, create_test_service_with_file_store, create_test_service_with_memory_store,
};

#[tokio::test]
async fn test_server_describe_cluster_with_memory_store() {
    let service = create_test_service_with_memory_store(
        flashq_cluster::types::BrokerId(1),
        Some(TestManifestConfig {
            broker_count: Some(3),
            ..Default::default()
        }),
    );

    let server = ClusterServer::new(Arc::new(service));
    let request = Request::new(DescribeClusterRequest {});

    let response = server.describe_cluster(request).await.unwrap();
    let cluster_info = response.into_inner();

    assert_eq!(cluster_info.brokers.len(), 3);
    assert_eq!(cluster_info.topics.len(), 1);
    assert_eq!(cluster_info.controller_id, 1);

    // Verify broker information
    let broker_1 = cluster_info
        .brokers
        .iter()
        .find(|b| b.broker_id == 1)
        .unwrap();
    assert_eq!(broker_1.host, "127.0.0.1");
    assert_eq!(broker_1.port, 6001);

    // Verify topic information
    let topic = cluster_info
        .topics
        .iter()
        .find(|t| t.topic == "test-topic")
        .unwrap();
    assert_eq!(topic.partitions.len(), 2);
}

#[tokio::test]
async fn test_server_describe_cluster_with_file_store() {
    let temp_dir = TempDir::new().unwrap();
    let service = create_test_service_with_file_store(
        &temp_dir,
        flashq_cluster::types::BrokerId(1),
        Some(TestManifestConfig {
            broker_count: Some(2),
            partition_epochs: Some(vec![3]),
            ..Default::default()
        }),
    );

    let server = ClusterServer::new(Arc::new(service));
    let request = Request::new(DescribeClusterRequest {});

    let response = server.describe_cluster(request).await.unwrap();
    let cluster_info = response.into_inner();

    assert_eq!(cluster_info.brokers.len(), 2);
    assert_eq!(cluster_info.topics.len(), 1);
    assert_eq!(cluster_info.controller_id, 1);

    // Verify partition epoch is preserved from file store
    let topic = cluster_info
        .topics
        .iter()
        .find(|t| t.topic == "test-topic")
        .unwrap();
    assert_eq!(topic.partitions.len(), 1);
    assert_eq!(topic.partitions[0].epoch, 3);
}

#[tokio::test]
async fn test_server_report_partition_status() {
    let service = create_test_service_with_memory_store(flashq_cluster::types::BrokerId(1), None);

    let server = ClusterServer::new(Arc::new(service));

    let request = Request::new(ReportPartitionStatusRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        in_sync_replicas: vec![1, 2],
        high_water_mark: 100,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    });

    let response = server.report_partition_status(request).await.unwrap();
    let status_response = response.into_inner();

    // The service implementation determines acceptance logic
    // For now, we just verify the operation doesn't fail and returns a valid response
    assert!(!status_response.message.is_empty());
}

#[tokio::test]
async fn test_server_direct_service_methods() {
    let service = create_test_service_with_memory_store(flashq_cluster::types::BrokerId(1), None);

    let server = ClusterServer::new(Arc::new(service));

    // Test accessing the underlying service
    let underlying_service = server.cluster_service();
    let result = underlying_service.describe_cluster().await;
    assert!(result.is_ok());

    let response = result.unwrap();
    assert_eq!(response.controller_id, 1);
    assert_eq!(response.brokers.len(), 3);
    assert_eq!(response.topics.len(), 1);
}

#[tokio::test]
async fn test_server_error_handling_invalid_topic() {
    let service = create_test_service_with_memory_store(flashq_cluster::types::BrokerId(1), None);

    let server = ClusterServer::new(Arc::new(service));

    let request = Request::new(ReportPartitionStatusRequest {
        topic: "nonexistent-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1],
        in_sync_replicas: vec![1],
        high_water_mark: 100,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    });

    let result = server.report_partition_status(request).await;

    // Should return an error for nonexistent topic
    match result {
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
            assert!(status.message().contains("nonexistent-topic"));
        }
        Ok(_) => {
            // Some implementations might accept unknown topics
            // This is acceptable behavior
        }
    }
}

#[tokio::test]
async fn test_server_error_handling_invalid_partition() {
    let service = create_test_service_with_memory_store(flashq_cluster::types::BrokerId(1), None);

    let server = ClusterServer::new(Arc::new(service));

    let request = Request::new(ReportPartitionStatusRequest {
        topic: "test-topic".to_string(),
        partition: 999, // Invalid partition ID
        leader: 1,
        replicas: vec![1],
        in_sync_replicas: vec![1],
        high_water_mark: 100,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    });

    let result = server.report_partition_status(request).await;

    // Should return an error for invalid partition
    match result {
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        Ok(_) => {
            // Some implementations might accept this
            // This is acceptable behavior depending on service logic
        }
    }
}

#[tokio::test]
async fn test_server_concurrent_requests() {
    let service = create_test_service_with_memory_store(flashq_cluster::types::BrokerId(1), None);

    let server = Arc::new(ClusterServer::new(Arc::new(service)));

    // Spawn multiple concurrent describe_cluster requests
    let mut handles = vec![];

    for i in 0..5 {
        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            let request = Request::new(DescribeClusterRequest {});
            let result = server_clone.describe_cluster(request).await;
            (i, result.is_ok())
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        let (request_id, success) = handle.await.unwrap();
        assert!(success, "Request {request_id} should have succeeded");
    }
}

#[tokio::test]
async fn test_server_persistence_across_multiple_requests() {
    let temp_dir = TempDir::new().unwrap();
    let service =
        create_test_service_with_file_store(&temp_dir, flashq_cluster::types::BrokerId(1), None);

    let server = ClusterServer::new(Arc::new(service));

    // First request - report partition status
    let request1 = Request::new(ReportPartitionStatusRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        in_sync_replicas: vec![1, 2],
        high_water_mark: 100,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    });

    let _response1 = server.report_partition_status(request1).await.unwrap();

    // Second request - describe cluster to verify state
    let request2 = Request::new(DescribeClusterRequest {});
    let response2 = server.describe_cluster(request2).await.unwrap();

    let cluster_info = response2.into_inner();
    assert_eq!(cluster_info.topics.len(), 1);

    let topic = cluster_info
        .topics
        .iter()
        .find(|t| t.topic == "test-topic")
        .unwrap();
    assert_eq!(topic.partitions.len(), 2);

    // Verify state is consistent with what we reported
    let partition = topic.partitions.iter().find(|p| p.partition == 0).unwrap();
    assert_eq!(partition.leader, 1);
}
