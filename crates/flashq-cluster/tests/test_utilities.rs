//! Test utilities for flashq-cluster integration tests.
//!
//! Common functions and helpers used across multiple test modules
//! to reduce duplication and ensure consistency.

use flashq_cluster::{
    client::ClusterClient,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::FileMetadataStore,
    server::ClusterServer,
    service::ClusterServiceImpl,
    types::*,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Configuration options for creating test manifests
#[derive(Default)]
pub struct TestManifestConfig {
    /// Override the epochs for partitions (default: [1, 1])
    pub partition_epochs: Option<Vec<u32>>,
    /// Override the topic name (default: "test-topic")
    pub topic_name: Option<String>,
    /// Override the number of brokers (default: 3)
    pub broker_count: Option<usize>,
    /// Override replication factor (default: 3)
    pub replication_factor: Option<u8>,
}

/// Create a configurable test cluster manifest.
///
/// Default configuration:
/// - Brokers: 3 (IDs 1, 2, 3) on ports 6001-6003
/// - Topics: 1 ("test-topic") with replication factor 3
/// - Partitions: 2 (IDs 0, 1) with epochs [1, 1] and different leaders
pub fn create_test_manifest(config: Option<TestManifestConfig>) -> ClusterManifest {
    let config = config.unwrap_or_default();
    let broker_count = config.broker_count.unwrap_or(3);
    let topic_name = config
        .topic_name
        .unwrap_or_else(|| "test-topic".to_string());
    let replication_factor = config.replication_factor.unwrap_or(3);
    let partition_epochs = config.partition_epochs.unwrap_or_else(|| vec![1, 1]);

    let brokers = (1..=broker_count)
        .map(|i| BrokerSpec {
            id: BrokerId(i as u32),
            host: "127.0.0.1".to_string(),
            port: 6000 + i as u16,
        })
        .collect();

    let mut topics = HashMap::new();

    // Create partitions based on the provided epochs
    let partitions: Vec<PartitionAssignment> = partition_epochs
        .iter()
        .enumerate()
        .map(|(i, &epoch)| {
            let partition_id = PartitionId::new(i as u32);
            let leader = BrokerId(((i % broker_count) + 1) as u32);

            // Create replicas from the available brokers
            let replicas: Vec<BrokerId> = (0..replication_factor as usize)
                .map(|j| BrokerId(((i + j) % broker_count + 1) as u32))
                .collect();

            // For ISR, use different configurations for different partitions
            let in_sync_replicas = if i == 0 {
                replicas.clone() // All replicas in sync for partition 0
            } else {
                replicas.iter().skip(1).cloned().collect() // Skip leader for other partitions
            };

            PartitionAssignment {
                id: partition_id,
                leader,
                replicas,
                in_sync_replicas,
                epoch: Epoch(epoch.into()),
            }
        })
        .collect();

    topics.insert(
        topic_name,
        TopicAssignment {
            replication_factor,
            partitions,
        },
    );

    ClusterManifest { brokers, topics }
}

/// Create a test service with file-based metadata store.
///
/// # Arguments
/// * `temp_dir` - Temporary directory for file storage
/// * `controller_id` - ID of the broker acting as controller
/// * `config` - Optional configuration for the test manifest
///
/// # Returns
/// Configured ClusterServiceImpl ready for testing
#[allow(dead_code)]
pub fn create_test_service_with_file_store(
    temp_dir: &TempDir,
    controller_id: BrokerId,
    config: Option<TestManifestConfig>,
) -> ClusterServiceImpl {
    let manifest = create_test_manifest(config);
    let file_store = FileMetadataStore::new_with_manifest(temp_dir.path(), manifest)
        .expect("Failed to create file metadata store");

    ClusterServiceImpl::new(Arc::new(file_store), controller_id)
}

/// Create a test service with in-memory metadata store.
///
/// # Arguments
/// * `controller_id` - ID of the broker acting as controller
/// * `config` - Optional configuration for the test manifest
///
/// # Returns
/// Configured ClusterServiceImpl ready for testing
#[allow(dead_code)]
pub fn create_test_service_with_memory_store(
    controller_id: BrokerId,
    config: Option<TestManifestConfig>,
) -> ClusterServiceImpl {
    use flashq_cluster::metadata_store::InMemoryMetadataStore;

    let manifest = create_test_manifest(config);
    let memory_store = InMemoryMetadataStore::new_with_manifest(manifest)
        .expect("Failed to create in-memory metadata store");

    ClusterServiceImpl::new(Arc::new(memory_store), controller_id)
}

/// Test server configuration for gRPC testing
#[allow(dead_code)]
pub struct TestServerConfig {
    /// The service implementation to use
    pub service: ClusterServiceImpl,
    /// Optional port to bind to (0 for random port)
    pub port: u16,
}

/// Start a test gRPC server and return its address and a shutdown handle.
///
/// # Arguments
/// * `config` - Configuration for the test server
///
/// # Returns
/// Tuple of (server_address, shutdown_handle)
#[allow(dead_code)]
pub async fn start_test_server(config: TestServerConfig) -> (String, tokio::task::JoinHandle<()>) {
    use flashq_cluster::proto::cluster_server::ClusterServer as ClusterGrpcService;

    let listener = if config.port == 0 {
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port")
    } else {
        TcpListener::bind(format!("127.0.0.1:{}", config.port))
            .await
            .expect("Failed to bind to specified port")
    };

    let addr = listener.local_addr().expect("Failed to get local address");
    let server_addr = format!("http://{addr}");

    let cluster_server = ClusterServer::new(Arc::new(config.service));
    let grpc_service = ClusterGrpcService::from_arc(Arc::new(cluster_server));

    let shutdown_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    (server_addr, shutdown_handle)
}

/// Create a test client connected to a server address.
///
/// # Arguments
/// * `server_addr` - Address of the server to connect to
///
/// # Returns
/// Connected ClusterClient
#[allow(dead_code)]
pub async fn create_test_client(server_addr: &str) -> ClusterClient {
    ClusterClient::connect(server_addr.to_string())
        .await
        .expect("Failed to connect test client")
}
