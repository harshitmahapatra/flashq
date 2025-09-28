//! Test utilities for flashq-cluster integration tests.
//!
//! Common functions and helpers used across multiple test modules
//! to reduce duplication and ensure consistency.

use async_trait::async_trait;
use flashq_cluster::{
    ClusterError,
    client::ClusterClient,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::FileMetadataStore,
    server::ClusterServer,
    service::ClusterServiceImpl,
    traits::FlashQBroker,
    types::*,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Partition metadata: (high_water_mark, log_start_offset, is_leader)
type PartitionMetadata = (u64, u64, bool);

/// Partition key: (topic, partition_id)
type PartitionKey = (String, PartitionId);

/// Collection of partition assignments for a broker
type PartitionMap = HashMap<PartitionKey, PartitionMetadata>;

/// Mock FlashQ broker for testing cluster service integration.
///
/// This mock implementation allows tests to simulate broker behavior
/// without requiring a full FlashQ broker instance.
#[derive(Debug)]
pub struct MockFlashQBroker {
    /// Partitions assigned to this broker: (topic, partition_id) -> (high_water_mark, log_start_offset, is_leader)
    pub partitions: Mutex<PartitionMap>,
    /// Whether the broker should return errors for testing failure scenarios
    pub should_error: Mutex<bool>,
}

#[allow(dead_code)]
impl Default for MockFlashQBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl MockFlashQBroker {
    /// Create a new mock broker with no assigned partitions.
    pub fn new() -> Self {
        Self {
            partitions: Mutex::new(HashMap::new()),
            should_error: Mutex::new(false),
        }
    }

    /// Create a new mock broker with predefined partitions for testing.
    ///
    /// Default setup:
    /// - test-topic, partition 0: HWM=100, LSO=0, is_leader=true
    /// - test-topic, partition 1: HWM=50, LSO=0, is_leader=false
    pub fn new_with_test_partitions() -> Self {
        let mut partitions = HashMap::new();
        partitions.insert(
            ("test-topic".to_string(), PartitionId::new(0)),
            (100, 0, true), // High water mark, log start offset, is_leader
        );
        partitions.insert(
            ("test-topic".to_string(), PartitionId::new(1)),
            (50, 0, false),
        );

        Self {
            partitions: Mutex::new(partitions),
            should_error: Mutex::new(false),
        }
    }

    /// Add a partition to this mock broker.
    pub fn add_partition(
        &self,
        topic: &str,
        partition_id: PartitionId,
        high_water_mark: u64,
        log_start_offset: u64,
        is_leader: bool,
    ) {
        let mut partitions = self.partitions.lock().unwrap();
        partitions.insert(
            (topic.to_string(), partition_id),
            (high_water_mark, log_start_offset, is_leader),
        );
    }

    /// Set whether this broker should return errors for testing failure scenarios.
    pub fn set_should_error(&self, should_error: bool) {
        *self.should_error.lock().unwrap() = should_error;
    }

    /// Update the high water mark for a partition.
    pub fn update_high_water_mark(&self, topic: &str, partition_id: PartitionId, new_hwm: u64) {
        let mut partitions = self.partitions.lock().unwrap();
        if let Some((hwm, _lso, _is_leader)) =
            partitions.get_mut(&(topic.to_string(), partition_id))
        {
            *hwm = new_hwm;
        }
    }
}

#[allow(dead_code)]
#[async_trait]
impl FlashQBroker for MockFlashQBroker {
    async fn get_high_water_mark(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&(topic.to_string(), partition))
            .map(|(hwm, _, _)| *hwm)
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            })
    }

    async fn get_log_start_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&(topic.to_string(), partition))
            .map(|(_, lso, _)| *lso)
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            })
    }

    async fn is_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<bool, ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&(topic.to_string(), partition))
            .map(|(_, _, is_leader)| *is_leader)
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            })
    }

    async fn get_assigned_partitions(&self) -> Result<Vec<(String, PartitionId)>, ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        let partitions = self.partitions.lock().unwrap();
        Ok(partitions.keys().cloned().collect())
    }

    async fn acknowledge_replication(
        &self,
        _topic: &str,
        _partition: PartitionId,
        _offset: u64,
    ) -> Result<(), ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        // Mock implementation - just return success
        Ok(())
    }

    async fn initiate_shutdown(&self) -> Result<(), ClusterError> {
        if *self.should_error.lock().unwrap() {
            return Err(ClusterError::Transport {
                context: "Mock broker error".to_string(),
                reason: "Simulated failure".to_string(),
            });
        }

        // Mock implementation - just return success
        Ok(())
    }
}

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

/// Create a test service with in-memory metadata store and FlashQ broker.
///
/// # Arguments
/// * `controller_id` - ID of the broker acting as controller
/// * `config` - Optional configuration for the test manifest
/// * `flashq_broker` - FlashQ broker implementation to use
///
/// # Returns
/// Configured ClusterServiceImpl ready for testing with FlashQ broker integration
#[allow(dead_code)]
pub fn create_test_service_with_memory_store_and_broker(
    controller_id: BrokerId,
    config: Option<TestManifestConfig>,
    flashq_broker: Arc<dyn FlashQBroker>,
) -> ClusterServiceImpl {
    use flashq_cluster::metadata_store::InMemoryMetadataStore;

    let manifest = create_test_manifest(config);
    let memory_store = InMemoryMetadataStore::new_with_manifest(manifest)
        .expect("Failed to create in-memory metadata store");

    ClusterServiceImpl::with_broker(Arc::new(memory_store), controller_id, flashq_broker)
}

/// Create a test service with file-based metadata store and FlashQ broker.
///
/// # Arguments
/// * `temp_dir` - Temporary directory for file storage
/// * `controller_id` - ID of the broker acting as controller
/// * `config` - Optional configuration for the test manifest
/// * `flashq_broker` - FlashQ broker implementation to use
///
/// # Returns
/// Configured ClusterServiceImpl ready for testing with FlashQ broker integration
#[allow(dead_code)]
pub fn create_test_service_with_file_store_and_broker(
    temp_dir: &TempDir,
    controller_id: BrokerId,
    config: Option<TestManifestConfig>,
    flashq_broker: Arc<dyn FlashQBroker>,
) -> ClusterServiceImpl {
    let manifest = create_test_manifest(config);
    let file_store = FileMetadataStore::new_with_manifest(temp_dir.path(), manifest)
        .expect("Failed to create file metadata store");

    ClusterServiceImpl::with_broker(Arc::new(file_store), controller_id, flashq_broker)
}

/// Create a follower service with cluster client and FlashQ broker for testing streaming heartbeats.
///
/// This is a helper specifically for testing heartbeat functionality between
/// a follower broker and a controller.
///
/// # Returns
/// Tuple of (follower_service, controller_server_address, shutdown_handle)
#[allow(dead_code)]
pub async fn create_follower_with_controller_setup()
-> (ClusterServiceImpl, String, tokio::task::JoinHandle<()>) {
    // Start controller server
    let controller_service = create_test_service_with_memory_store(BrokerId::from(1), None);
    let (server_addr, shutdown_handle) = start_test_server(TestServerConfig {
        service: controller_service,
        port: 0,
    })
    .await;

    // Create cluster client for follower
    let cluster_client = ClusterClient::connect(server_addr.clone())
        .await
        .expect("Failed to connect follower to controller");

    // Create mock broker with test partitions
    let mock_broker = Arc::new(MockFlashQBroker::new_with_test_partitions());

    // Create follower service metadata store
    let metadata_store = Arc::new(
        flashq_cluster::metadata_store::InMemoryMetadataStore::new_with_manifest(
            create_test_manifest(None),
        )
        .expect("Failed to create follower metadata store"),
    );

    // Create follower service with both client and broker
    let follower_service = ClusterServiceImpl::with_client_and_broker(
        metadata_store,
        cluster_client,
        BrokerId::from(2), // Follower broker ID
        mock_broker,
    );

    (follower_service, server_addr, shutdown_handle)
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
