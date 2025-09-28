//! Integration tests for ClusterClient with real gRPC server.
//!
//! These tests verify that ClusterClient correctly communicates with a running
//! ClusterService server, handling connection management, heartbeat streams,
//! and error scenarios.

use chrono::Utc;
use flashq_cluster::{
    client::ClusterClient,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::{FileMetadataStore, MetadataStore},
    proto::{HeartbeatRequest, PartitionHeartbeat, ReportPartitionStatusRequest},
    service::ClusterServiceImpl,
    types::*,
};
use flashq_grpc::{ClusterServer, server::FlashQGrpcBroker};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tonic::transport::Server;

use crate::test_utilities::*;

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
    ];

    let mut topics = HashMap::new();
    topics.insert(
        "test-topic".to_string(),
        TopicAssignment {
            replication_factor: 2,
            partitions: vec![PartitionAssignment {
                id: PartitionId::new(0),
                leader: BrokerId(1),
                replicas: vec![BrokerId(1), BrokerId(2)],
                in_sync_replicas: vec![BrokerId(1), BrokerId(2)],
                epoch: Epoch(1),
            }],
        },
    );

    ClusterManifest { brokers, topics }
}

/// Setup a test cluster server and return the server address and temp directory.
async fn setup_cluster_server() -> (String, TempDir, tokio::task::JoinHandle<()>) {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().unwrap();
    let addr = format!("127.0.0.1:{port}");
    let server_addr = format!("http://{addr}");

    // Create FlashQ core
    let core = Arc::new(flashq_cluster::FlashQ::new());
    let grpc_service = Arc::new(FlashQGrpcBroker::new(core));

    // Create file-based metadata store
    let metadata_store: Arc<dyn MetadataStore> = {
        let store = Arc::new(FileMetadataStore::new(temp_dir.path().join("cluster")).unwrap());
        store.load_from_manifest(seeded_manifest()).unwrap();
        store
    };

    // Create cluster service
    let cluster_service = Arc::new(ClusterServiceImpl::with_broker(
        metadata_store,
        BrokerId(1),
        grpc_service,
    ));

    // Start the server
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ClusterServer::new(flashq_cluster::ClusterServer::new(
                cluster_service,
            )))
            .serve(addr.parse().unwrap())
            .await
            .unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (server_addr, temp_dir, server_handle)
}

#[tokio::test]
async fn test_cluster_client_connect_success() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;

    // Action
    let result = ClusterClient::connect(server_addr).await;

    // Expectation
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cluster_client_connect_with_timeout_success() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let timeout_duration = Duration::from_secs(5);

    // Action
    let result = ClusterClient::connect_with_timeout(server_addr, timeout_duration).await;

    // Expectation
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cluster_client_connect_invalid_endpoint_fails() {
    // Setup
    let invalid_addr = "invalid-endpoint";

    // Action
    let result = ClusterClient::connect(invalid_addr).await;

    // Expectation
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        flashq_cluster::ClusterError::Transport { .. }
    ));
}

#[tokio::test]
async fn test_cluster_client_connect_unreachable_server_fails() {
    // Setup
    let unreachable_addr = "http://127.0.0.1:19999"; // Unlikely to be in use

    // Action
    let result = ClusterClient::connect(unreachable_addr).await;

    // Expectation
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        flashq_cluster::ClusterError::Transport { .. }
    ));
}

#[tokio::test]
async fn test_cluster_client_describe_cluster_returns_metadata() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    // Action
    let response = client.describe_cluster().await;

    // Expectation
    assert!(response.is_ok());
    let cluster_info = response.unwrap();
    assert_eq!(cluster_info.controller_id, 1);
    assert!(!cluster_info.brokers.is_empty());
}

#[tokio::test]
async fn test_cluster_client_start_heartbeat_stream_success() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    // Action
    let result = client.start_heartbeat_stream().await;

    // Expectation
    assert!(result.is_ok());
    let (sender, _receiver) = result.unwrap();
    assert!(!sender.is_closed());
}

#[tokio::test]
async fn test_cluster_client_heartbeat_stream_bidirectional_communication() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();
    let (sender, mut receiver) = client.start_heartbeat_stream().await.unwrap();

    let heartbeat_request = HeartbeatRequest {
        broker_id: 1,
        partitions: vec![PartitionHeartbeat {
            topic: "test-topic".to_string(),
            partition: 0,
            leader_epoch: 1,
            high_water_mark: 0,
            log_start_offset: 0,
            is_leader: true,
            current_in_sync_replicas: vec![1],
            leader_override: None,
        }],
        timestamp: Utc::now().to_rfc3339(),
    };

    // Action
    let send_result = sender.send(heartbeat_request).await;
    let response_result = timeout(Duration::from_secs(1), receiver.next()).await;

    // Expectation
    assert!(send_result.is_ok());
    assert!(response_result.is_ok());
    let response = response_result.unwrap().unwrap();
    assert!(response.is_ok());
}

#[tokio::test]
async fn test_cluster_client_report_partition_status_success() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    let status_request = ReportPartitionStatusRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2],
        in_sync_replicas: vec![1, 2],
        high_water_mark: 5,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    };

    // Action
    let result = client.report_partition_status(status_request).await;

    // Expectation
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cluster_client_report_partition_status_invalid_topic_fails() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    let status_request = ReportPartitionStatusRequest {
        topic: "nonexistent-topic".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2],
        in_sync_replicas: vec![1, 2],
        high_water_mark: 5,
        log_start_offset: 0,
        timestamp: Utc::now().to_rfc3339(),
    };

    // Action
    let result = client.report_partition_status(status_request).await;

    // Expectation
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        flashq_cluster::ClusterError::TopicNotFound { .. }
    ));
}

#[tokio::test]
async fn test_cluster_client_heartbeat_stream_creation_after_server_disconnect() {
    // Setup
    let (server_addr, _temp_dir, server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr.clone()).await.unwrap();

    // Action: Abort the server to simulate disconnect
    server_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Try to start a new heartbeat stream after server is down
    let result = client.start_heartbeat_stream().await;

    // Expectation: Should fail to create new stream
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cluster_client_multiple_describe_cluster_calls() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    // Action: Make multiple calls
    let result1 = client.describe_cluster().await;
    let result2 = client.describe_cluster().await;

    // Expectation: Both calls should succeed with same data
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    let response1 = result1.unwrap();
    let response2 = result2.unwrap();
    assert_eq!(response1.controller_id, response2.controller_id);
    assert_eq!(response1.brokers.len(), response2.brokers.len());
}

#[tokio::test]
async fn test_cluster_client_concurrent_heartbeat_streams() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client1 = ClusterClient::connect(server_addr.clone()).await.unwrap();
    let mut client2 = ClusterClient::connect(server_addr).await.unwrap();

    // Action: Start streams from both clients
    let result1 = client1.start_heartbeat_stream().await;
    let result2 = client2.start_heartbeat_stream().await;

    // Expectation: Both streams should start successfully
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    let (sender1, _receiver1) = result1.unwrap();
    let (sender2, _receiver2) = result2.unwrap();
    assert!(!sender1.is_closed());
    assert!(!sender2.is_closed());
}

#[tokio::test]
async fn test_cluster_client_accessor_methods() {
    // Setup
    let (server_addr, _temp_dir, _server_handle) = setup_cluster_server().await;
    let mut client = ClusterClient::connect(server_addr).await.unwrap();

    // Action: Test accessor methods separately
    {
        let _client_ref = client.client();
        // Expectation: Should be able to get immutable reference
    }
    {
        let _client_mut_ref = client.client_mut();
        // Expectation: Should be able to get mutable reference
    }
}
