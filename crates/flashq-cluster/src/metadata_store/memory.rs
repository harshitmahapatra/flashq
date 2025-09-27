//! In-memory metadata store implementation.

use crate::{
    ClusterError,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::r#trait::MetadataStore,
    types::*,
};
use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// In-memory implementation of the MetadataStore trait.
///
/// This implementation stores all cluster metadata in memory using thread-safe
/// data structures. It's suitable for development, testing, and single-node deployments
/// where persistence across restarts is not required.
#[derive(Debug)]
pub struct InMemoryMetadataStore {
    /// Internal cluster state protected by RwLock for concurrent access
    state: Arc<RwLock<ClusterState>>,
}

/// Internal representation of cluster state
#[derive(Debug, Clone)]
struct ClusterState {
    /// Broker information indexed by broker ID
    brokers: HashMap<BrokerId, BrokerSpec>,
    /// Topic assignments indexed by topic name
    topics: HashMap<String, TopicAssignment>,
    /// Broker runtime status indexed by broker ID
    broker_runtime: HashMap<BrokerId, BrokerRuntimeStatus>,
    /// Partition runtime state indexed by (topic, partition_id)
    partition_runtime: HashMap<(String, PartitionId), PartitionRuntimeState>,
}

impl ClusterState {
    fn new() -> Self {
        Self {
            brokers: HashMap::new(),
            topics: HashMap::new(),
            broker_runtime: HashMap::new(),
            partition_runtime: HashMap::new(),
        }
    }

    fn get_partition_mut(
        &mut self,
        topic: &str,
        partition_id: PartitionId,
    ) -> Result<&mut PartitionAssignment, ClusterError> {
        let topic_assignment = self
            .topics
            .get_mut(topic)
            .ok_or(ClusterError::TopicNotFound {
                topic: topic.to_string(),
            })?;

        topic_assignment
            .partitions
            .iter_mut()
            .find(|p| p.id == partition_id)
            .ok_or(ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
            })
    }

    fn get_partition(
        &self,
        topic: &str,
        partition_id: PartitionId,
    ) -> Result<&PartitionAssignment, ClusterError> {
        let topic_assignment = self.topics.get(topic).ok_or(ClusterError::TopicNotFound {
            topic: topic.to_string(),
        })?;

        topic_assignment
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .ok_or(ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
            })
    }
}

impl InMemoryMetadataStore {
    /// Create a new empty in-memory metadata store.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new())),
        }
    }

    /// Create a new in-memory metadata store initialized with the given manifest.
    pub fn new_with_manifest(manifest: ClusterManifest) -> Result<Self, ClusterError> {
        let store = Self::new();
        store.load_from_manifest(manifest)?;
        Ok(store)
    }
}

impl Default for InMemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataStore for InMemoryMetadataStore {
    fn get_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<BrokerId, ClusterError> {
        let state = self.state.read();
        let partition = state.get_partition(topic, partition)?;
        Ok(partition.leader)
    }

    fn get_in_sync_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<HashSet<BrokerId>, ClusterError> {
        let state = self.state.read();
        let partition = state.get_partition(topic, partition)?;
        Ok(partition.in_sync_replicas.iter().copied().collect())
    }

    fn bump_leader_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Epoch, ClusterError> {
        let mut state = self.state.write();
        let partition = state.get_partition_mut(topic, partition)?;

        // Increment epoch atomically
        let new_epoch = Epoch(partition.epoch.0 + 1);
        partition.epoch = new_epoch;

        Ok(new_epoch)
    }

    fn update_in_sync_replica(
        &self,
        topic: &str,
        partition_id: PartitionId,
        replica: BrokerId,
        in_sync: bool,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();
        let partition = state.get_partition_mut(topic, partition_id)?;

        // Verify the replica is actually part of the replica set
        if !partition.replicas.contains(&replica) {
            return Err(ClusterError::InvalidReplica {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
                replica_id: replica.into(),
            });
        }

        if in_sync {
            // Add to in-sync replicas if not already present
            if !partition.in_sync_replicas.contains(&replica) {
                partition.in_sync_replicas.push(replica);
            }
        } else {
            // Remove from in-sync replicas
            partition.in_sync_replicas.retain(|&r| r != replica);
        }

        Ok(())
    }

    fn get_partition_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Epoch, ClusterError> {
        let state = self.state.read();
        let partition = state.get_partition(topic, partition)?;
        Ok(partition.epoch)
    }

    fn get_all_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Vec<BrokerId>, ClusterError> {
        let state = self.state.read();
        let partition = state.get_partition(topic, partition)?;
        Ok(partition.replicas.clone())
    }

    fn load_from_manifest(&self, manifest: ClusterManifest) -> Result<(), ClusterError> {
        let mut state = self.state.write();

        // Clear existing state including runtime state
        state.brokers.clear();
        state.topics.clear();
        state.broker_runtime.clear();
        state.partition_runtime.clear();

        // Load brokers
        for broker in manifest.brokers {
            state.brokers.insert(broker.id, broker);
        }

        // Load topics
        for (topic_name, topic_assignment) in manifest.topics {
            state.topics.insert(topic_name, topic_assignment);
        }

        Ok(())
    }

    fn export_to_manifest(&self) -> Result<ClusterManifest, ClusterError> {
        let state = self.state.read();

        let brokers: Vec<BrokerSpec> = state.brokers.values().cloned().collect();
        let topics: HashMap<String, TopicAssignment> = state.topics.clone();

        Ok(ClusterManifest { brokers, topics })
    }

    // ===========================
    // Broker Runtime Tracking
    // ===========================

    fn record_broker_heartbeat(
        &self,
        broker: BrokerId,
        ts: DateTime<Utc>,
        draining: bool,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();

        // Verify broker exists in the cluster
        if !state.brokers.contains_key(&broker) {
            return Err(ClusterError::UnknownBroker {
                broker_id: broker.into(),
            });
        }

        // Update or insert broker runtime status
        state.broker_runtime.insert(
            broker,
            BrokerRuntimeStatus {
                last_heartbeat: ts,
                is_draining: draining,
            },
        );

        Ok(())
    }

    fn list_brokers_with_status(
        &self,
    ) -> Result<Vec<(BrokerId, BrokerRuntimeStatus)>, ClusterError> {
        let state = self.state.read();
        let heartbeat_timeout = Duration::seconds(10); // 10 second timeout for testing

        let mut result = Vec::new();
        for broker_id in state.brokers.keys() {
            let status = if let Some(runtime_status) = state.broker_runtime.get(broker_id) {
                // Check if broker is alive based on heartbeat timeout
                let _is_alive = Utc::now().signed_duration_since(runtime_status.last_heartbeat)
                    < heartbeat_timeout;
                BrokerRuntimeStatus {
                    last_heartbeat: runtime_status.last_heartbeat,
                    is_draining: runtime_status.is_draining,
                }
            } else {
                // No heartbeat recorded yet - consider dead
                BrokerRuntimeStatus {
                    last_heartbeat: DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
                    is_draining: false,
                }
            };
            result.push((*broker_id, status));
        }

        Ok(result)
    }

    // ===========================
    // Enhanced Partition Operations
    // ===========================

    fn set_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        leader: BrokerId,
        epoch: Epoch,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();
        let partition_assignment = state.get_partition_mut(topic, partition)?;

        // Verify the new leader is in the replica set
        if !partition_assignment.replicas.contains(&leader) {
            return Err(ClusterError::InvalidReplica {
                topic: topic.to_string(),
                partition_id: partition.into(),
                replica_id: leader.into(),
            });
        }

        // Verify epoch is not going backwards
        if epoch < partition_assignment.epoch {
            return Err(ClusterError::InvalidEpoch {
                topic: topic.to_string(),
                partition_id: partition.into(),
                current_epoch: partition_assignment.epoch.into(),
                new_epoch: epoch.into(),
            });
        }

        partition_assignment.leader = leader;
        partition_assignment.epoch = epoch;

        Ok(())
    }

    fn compare_and_set_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
        expected: Epoch,
        new: Epoch,
    ) -> Result<bool, ClusterError> {
        let mut state = self.state.write();
        let partition_assignment = state.get_partition_mut(topic, partition)?;

        if partition_assignment.epoch != expected {
            return Ok(false); // CAS failed - epoch didn't match
        }

        // Verify new epoch is greater than current
        if new <= expected {
            return Err(ClusterError::InvalidEpoch {
                topic: topic.to_string(),
                partition_id: partition.into(),
                current_epoch: expected.into(),
                new_epoch: new.into(),
            });
        }

        partition_assignment.epoch = new;
        Ok(true)
    }

    fn update_partition_offsets(
        &self,
        topic: &str,
        partition: PartitionId,
        high_water_mark: u64,
        log_start_offset: u64,
    ) -> Result<(), ClusterError> {
        let state = self.state.read();
        // Verify partition exists
        let _ = state.get_partition(topic, partition)?;
        drop(state);

        // Update partition runtime state
        let mut state = self.state.write();
        state.partition_runtime.insert(
            (topic.to_string(), partition),
            PartitionRuntimeState {
                high_water_mark,
                log_start_offset,
            },
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::types::{BrokerSpec, PartitionAssignment, TopicAssignment};
    use std::collections::HashMap;

    fn create_test_store() -> InMemoryMetadataStore {
        let manifest = create_test_manifest();
        InMemoryMetadataStore::new_with_manifest(manifest).unwrap()
    }

    fn create_test_manifest() -> ClusterManifest {
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
        topics.insert(
            "test-topic".to_string(),
            TopicAssignment {
                replication_factor: 3,
                partitions: vec![
                    PartitionAssignment {
                        id: PartitionId::new(0),
                        leader: BrokerId(1),
                        replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                        in_sync_replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                        epoch: Epoch(5),
                    },
                    PartitionAssignment {
                        id: PartitionId::new(1),
                        leader: BrokerId(2),
                        replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                        in_sync_replicas: vec![BrokerId(2), BrokerId(3)],
                        epoch: Epoch(2),
                    },
                ],
            },
        );

        ClusterManifest { brokers, topics }
    }

    #[test]
    fn test_empty_store_creation() {
        let store = InMemoryMetadataStore::new();
        let manifest = store.export_to_manifest().unwrap();
        assert!(manifest.brokers.is_empty());
        assert!(manifest.topics.is_empty());
    }

    #[test]
    fn test_store_with_manifest_creation() {
        let manifest = create_test_manifest();
        let store = InMemoryMetadataStore::new_with_manifest(manifest.clone()).unwrap();

        let exported = store.export_to_manifest().unwrap();
        assert_eq!(exported.brokers.len(), 3);
        assert_eq!(exported.topics.len(), 1);
        assert!(exported.topics.contains_key("test-topic"));
    }

    #[test]
    fn test_get_partition_leader() {
        let store = create_test_store();

        // Test existing partition
        let leader = store
            .get_partition_leader("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(leader, BrokerId(1));

        let leader = store
            .get_partition_leader("test-topic", PartitionId::new(1))
            .unwrap();
        assert_eq!(leader, BrokerId(2));
    }

    #[test]
    fn test_get_partition_leader_errors() {
        let store = create_test_store();

        // Test non-existent topic
        let result = store.get_partition_leader("nonexistent", PartitionId::new(0));
        assert!(matches!(result, Err(ClusterError::TopicNotFound { .. })));

        // Test non-existent partition
        let result = store.get_partition_leader("test-topic", PartitionId::new(999));
        assert!(matches!(
            result,
            Err(ClusterError::PartitionNotFound { .. })
        ));
    }

    #[test]
    fn test_get_in_sync_replicas() {
        let store = create_test_store();

        // Test partition 0 - all replicas in sync
        let replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(replicas.len(), 3);
        assert!(replicas.contains(&BrokerId(1)));
        assert!(replicas.contains(&BrokerId(2)));
        assert!(replicas.contains(&BrokerId(3)));

        // Test partition 1 - only brokers 2 and 3 in sync
        let replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert_eq!(replicas.len(), 2);
        assert!(replicas.contains(&BrokerId(2)));
        assert!(replicas.contains(&BrokerId(3)));
        assert!(!replicas.contains(&BrokerId(1)));
    }

    #[test]
    fn test_get_all_replicas() {
        let store = create_test_store();

        let replicas = store
            .get_all_replicas("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas, vec![BrokerId(1), BrokerId(2), BrokerId(3)]);
    }

    #[test]
    fn test_get_partition_epoch() {
        let store = create_test_store();

        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(5));

        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(1))
            .unwrap();
        assert_eq!(epoch, Epoch(2));
    }

    #[test]
    fn test_bump_leader_epoch() {
        let store = create_test_store();

        // Get initial epoch
        let initial_epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(initial_epoch, Epoch(5));

        // Bump epoch
        let new_epoch = store
            .bump_leader_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(new_epoch, Epoch(6));

        // Verify epoch was actually updated
        let current_epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(current_epoch, Epoch(6));

        // Bump again to ensure monotonic progression
        let newer_epoch = store
            .bump_leader_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(newer_epoch, Epoch(7));
    }

    #[test]
    fn test_bump_leader_epoch_errors() {
        let store = create_test_store();

        // Test non-existent topic
        let result = store.bump_leader_epoch("nonexistent", PartitionId::new(0));
        assert!(matches!(result, Err(ClusterError::TopicNotFound { .. })));

        // Test non-existent partition
        let result = store.bump_leader_epoch("test-topic", PartitionId::new(999));
        assert!(matches!(
            result,
            Err(ClusterError::PartitionNotFound { .. })
        ));
    }

    #[test]
    fn test_update_in_sync_replica_add() {
        let store = create_test_store();

        // Initially broker 1 is not in sync for partition 1
        let initial_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert!(!initial_replicas.contains(&BrokerId(1)));

        // Add broker 1 to in-sync replicas
        store
            .update_in_sync_replica("test-topic", PartitionId::new(1), BrokerId(1), true)
            .unwrap();

        // Verify broker 1 is now in sync
        let updated_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert!(updated_replicas.contains(&BrokerId(1)));
        assert_eq!(updated_replicas.len(), 3);
    }

    #[test]
    fn test_update_in_sync_replica_remove() {
        let store = create_test_store();

        // Initially broker 2 is in sync for partition 1
        let initial_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert!(initial_replicas.contains(&BrokerId(2)));

        // Remove broker 2 from in-sync replicas
        store
            .update_in_sync_replica("test-topic", PartitionId::new(1), BrokerId(2), false)
            .unwrap();

        // Verify broker 2 is no longer in sync
        let updated_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert!(!updated_replicas.contains(&BrokerId(2)));
        assert_eq!(updated_replicas.len(), 1);
        assert!(updated_replicas.contains(&BrokerId(3)));
    }

    #[test]
    fn test_update_in_sync_replica_idempotent() {
        let store = create_test_store();

        // Add broker that's already in sync (should be idempotent)
        let initial_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(0))
            .unwrap();
        assert!(initial_replicas.contains(&BrokerId(1)));

        store
            .update_in_sync_replica("test-topic", PartitionId::new(0), BrokerId(1), true)
            .unwrap();

        let updated_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(initial_replicas, updated_replicas);

        // Remove broker that's not in sync (should be idempotent)
        store
            .update_in_sync_replica("test-topic", PartitionId::new(1), BrokerId(1), false)
            .unwrap();

        let final_replicas = store
            .get_in_sync_replicas("test-topic", PartitionId::new(1))
            .unwrap();
        assert!(!final_replicas.contains(&BrokerId(1)));
    }

    #[test]
    fn test_update_in_sync_replica_invalid_replica() {
        let store = create_test_store();

        // Try to update in-sync status for a broker that's not in the replica set
        let result =
            store.update_in_sync_replica("test-topic", PartitionId::new(0), BrokerId(999), true);
        assert!(matches!(result, Err(ClusterError::InvalidReplica { .. })));
    }

    #[test]
    fn test_load_from_manifest() {
        let store = InMemoryMetadataStore::new();

        // Initially empty
        let initial_manifest = store.export_to_manifest().unwrap();
        assert!(initial_manifest.brokers.is_empty());
        assert!(initial_manifest.topics.is_empty());

        // Load test manifest
        let test_manifest = create_test_manifest();
        store.load_from_manifest(test_manifest.clone()).unwrap();

        // Verify data was loaded
        let loaded_manifest = store.export_to_manifest().unwrap();
        assert_eq!(loaded_manifest.brokers.len(), 3);
        assert_eq!(loaded_manifest.topics.len(), 1);

        // Verify specific data
        let leader = store
            .get_partition_leader("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(leader, BrokerId(1));

        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(5));
    }

    #[test]
    fn test_load_from_manifest_replaces_existing() {
        let store = create_test_store();

        // Verify initial state
        let initial_manifest = store.export_to_manifest().unwrap();
        assert_eq!(initial_manifest.topics.len(), 1);

        // Load a different manifest
        let new_manifest = ClusterManifest {
            brokers: vec![BrokerSpec {
                id: BrokerId(100),
                host: "new-host".to_string(),
                port: 7000,
            }],
            topics: [(
                "new-topic".to_string(),
                TopicAssignment {
                    replication_factor: 1,
                    partitions: vec![PartitionAssignment {
                        id: PartitionId::new(0),
                        leader: BrokerId(100),
                        replicas: vec![BrokerId(100)],
                        in_sync_replicas: vec![BrokerId(100)],
                        epoch: Epoch(1),
                    }],
                },
            )]
            .into_iter()
            .collect(),
        };

        store.load_from_manifest(new_manifest).unwrap();

        // Verify old data was replaced
        let result = store.get_partition_leader("test-topic", PartitionId::new(0));
        assert!(matches!(result, Err(ClusterError::TopicNotFound { .. })));

        // Verify new data is present
        let leader = store
            .get_partition_leader("new-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(leader, BrokerId(100));
    }

    #[test]
    fn test_export_to_manifest() {
        let store = create_test_store();
        let exported = store.export_to_manifest().unwrap();

        // Verify structure
        assert_eq!(exported.brokers.len(), 3);
        assert_eq!(exported.topics.len(), 1);
        assert!(exported.topics.contains_key("test-topic"));

        // Verify broker data
        let broker_ids: HashSet<BrokerId> = exported.brokers.iter().map(|b| b.id).collect();
        assert!(broker_ids.contains(&BrokerId(1)));
        assert!(broker_ids.contains(&BrokerId(2)));
        assert!(broker_ids.contains(&BrokerId(3)));

        // Verify topic data
        let topic = exported.topics.get("test-topic").unwrap();
        assert_eq!(topic.replication_factor, 3);
        assert_eq!(topic.partitions.len(), 2);

        // Verify partition data
        let partition_0 = topic
            .partitions
            .iter()
            .find(|p| p.id == PartitionId::new(0))
            .unwrap();
        assert_eq!(partition_0.leader, BrokerId(1));
        assert_eq!(partition_0.epoch, Epoch(5));
        assert_eq!(partition_0.replicas.len(), 3);
        assert_eq!(partition_0.in_sync_replicas.len(), 3);
    }

    #[test]
    fn test_default_implementation() {
        let store = InMemoryMetadataStore::default();
        let manifest = store.export_to_manifest().unwrap();
        assert!(manifest.brokers.is_empty());
        assert!(manifest.topics.is_empty());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(create_test_store());
        let mut handles = vec![];

        // Spawn multiple threads to test concurrent access
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = thread::spawn(move || {
                // Each thread bumps the epoch multiple times
                for _ in 0..5 {
                    let _ = store_clone.bump_leader_epoch("test-topic", PartitionId::new(0));
                }

                // Read operations
                let _ = store_clone.get_partition_leader("test-topic", PartitionId::new(0));
                let _ = store_clone.get_in_sync_replicas("test-topic", PartitionId::new(0));

                // Modify in-sync replicas
                if i % 2 == 0 {
                    let _ = store_clone.update_in_sync_replica(
                        "test-topic",
                        PartitionId::new(1),
                        BrokerId(1),
                        true,
                    );
                } else {
                    let _ = store_clone.update_in_sync_replica(
                        "test-topic",
                        PartitionId::new(1),
                        BrokerId(1),
                        false,
                    );
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state is consistent
        let final_epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert!(final_epoch.0 > 5); // Should have been incremented
        assert!(final_epoch.0 <= 55); // But not more than initial + (10 threads * 5 increments)
    }

    // ===========================
    // Tests for New API Methods
    // ===========================

    #[test]
    fn test_record_broker_heartbeat() {
        let store = create_test_store();
        let now = Utc::now();

        // Record heartbeat for existing broker
        store
            .record_broker_heartbeat(BrokerId(1), now, false)
            .unwrap();

        // Verify heartbeat was recorded
        let brokers = store.list_brokers_with_status().unwrap();
        let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
        assert_eq!(broker_1.1.last_heartbeat, now);
        assert!(!broker_1.1.is_draining);
    }

    #[test]
    fn test_record_broker_heartbeat_draining() {
        let store = create_test_store();
        let now = Utc::now();

        // Record heartbeat with draining status
        store
            .record_broker_heartbeat(BrokerId(2), now, true)
            .unwrap();

        // Verify draining status was recorded
        let brokers = store.list_brokers_with_status().unwrap();
        let broker_2 = brokers.iter().find(|(id, _)| *id == BrokerId(2)).unwrap();
        assert_eq!(broker_2.1.last_heartbeat, now);
        assert!(broker_2.1.is_draining);
    }

    #[test]
    fn test_record_broker_heartbeat_unknown_broker() {
        let store = create_test_store();
        let now = Utc::now();

        // Try to record heartbeat for unknown broker
        let result = store.record_broker_heartbeat(BrokerId(999), now, false);
        assert!(matches!(result, Err(ClusterError::UnknownBroker { .. })));
    }

    #[test]
    fn test_list_brokers_with_status() {
        let store = create_test_store();
        let now = Utc::now();

        // Record heartbeats for some brokers
        store
            .record_broker_heartbeat(BrokerId(1), now, false)
            .unwrap();
        store
            .record_broker_heartbeat(BrokerId(2), now - Duration::seconds(5), true)
            .unwrap();
        // Don't record heartbeat for broker 3

        let brokers = store.list_brokers_with_status().unwrap();
        assert_eq!(brokers.len(), 3);

        // Check broker 1 status
        let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
        assert_eq!(broker_1.1.last_heartbeat, now);
        assert!(!broker_1.1.is_draining);

        // Check broker 2 status
        let broker_2 = brokers.iter().find(|(id, _)| *id == BrokerId(2)).unwrap();
        assert_eq!(broker_2.1.last_heartbeat, now - Duration::seconds(5));
        assert!(broker_2.1.is_draining);

        // Check broker 3 status (no heartbeat recorded)
        let broker_3 = brokers.iter().find(|(id, _)| *id == BrokerId(3)).unwrap();
        assert_eq!(
            broker_3.1.last_heartbeat,
            DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
        );
        assert!(!broker_3.1.is_draining);
    }

    #[test]
    fn test_set_partition_leader() {
        let store = create_test_store();

        // Change leader from broker 1 to broker 2 for partition 0
        store
            .set_partition_leader("test-topic", PartitionId::new(0), BrokerId(2), Epoch(6))
            .unwrap();

        // Verify leader was changed
        let leader = store
            .get_partition_leader("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(leader, BrokerId(2));

        // Verify epoch was updated
        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(6));
    }

    #[test]
    fn test_set_partition_leader_invalid_replica() {
        let store = create_test_store();

        // Try to set leader to a broker not in replica set
        let result =
            store.set_partition_leader("test-topic", PartitionId::new(0), BrokerId(999), Epoch(6));
        assert!(matches!(result, Err(ClusterError::InvalidReplica { .. })));
    }

    #[test]
    fn test_set_partition_leader_backwards_epoch() {
        let store = create_test_store();

        // Try to set epoch backwards (current is 5, trying to set 4)
        let result =
            store.set_partition_leader("test-topic", PartitionId::new(0), BrokerId(2), Epoch(4));
        assert!(matches!(result, Err(ClusterError::InvalidEpoch { .. })));
    }

    #[test]
    fn test_compare_and_set_epoch_success() {
        let store = create_test_store();

        // Get current epoch for partition 0 (should be 5)
        let current_epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(current_epoch, Epoch(5));

        // Successfully CAS from 5 to 6
        let success = store
            .compare_and_set_epoch("test-topic", PartitionId::new(0), Epoch(5), Epoch(6))
            .unwrap();
        assert!(success);

        // Verify epoch was updated
        let new_epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(new_epoch, Epoch(6));
    }

    #[test]
    fn test_compare_and_set_epoch_failure() {
        let store = create_test_store();

        // Try CAS with wrong expected epoch
        let success = store
            .compare_and_set_epoch("test-topic", PartitionId::new(0), Epoch(999), Epoch(6))
            .unwrap();
        assert!(!success);

        // Verify epoch was not changed
        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(5));
    }

    #[test]
    fn test_compare_and_set_epoch_invalid_new_epoch() {
        let store = create_test_store();

        // Try CAS with new epoch <= expected epoch
        let result = store.compare_and_set_epoch(
            "test-topic",
            PartitionId::new(0),
            Epoch(5),
            Epoch(5), // same epoch
        );
        assert!(matches!(result, Err(ClusterError::InvalidEpoch { .. })));

        let result = store.compare_and_set_epoch(
            "test-topic",
            PartitionId::new(0),
            Epoch(5),
            Epoch(4), // lower epoch
        );
        assert!(matches!(result, Err(ClusterError::InvalidEpoch { .. })));
    }

    #[test]
    fn test_update_partition_offsets() {
        let store = create_test_store();

        // Update offsets for partition 0
        store
            .update_partition_offsets("test-topic", PartitionId::new(0), 100, 50)
            .unwrap();

        // Note: We can't directly verify the offsets since there's no getter method
        // in the trait yet, but the operation should succeed without error
    }

    #[test]
    fn test_update_partition_offsets_nonexistent_partition() {
        let store = create_test_store();

        // Try to update offsets for non-existent partition
        let result = store.update_partition_offsets("test-topic", PartitionId::new(999), 100, 50);
        assert!(matches!(
            result,
            Err(ClusterError::PartitionNotFound { .. })
        ));
    }

    #[test]
    fn test_update_partition_offsets_nonexistent_topic() {
        let store = create_test_store();

        // Try to update offsets for non-existent topic
        let result = store.update_partition_offsets("nonexistent", PartitionId::new(0), 100, 50);
        assert!(matches!(result, Err(ClusterError::TopicNotFound { .. })));
    }

    #[test]
    fn test_runtime_state_persistence_across_operations() {
        let store = create_test_store();
        let now = Utc::now();

        // Record heartbeat
        store
            .record_broker_heartbeat(BrokerId(1), now, true)
            .unwrap();

        // Update partition offsets
        store
            .update_partition_offsets("test-topic", PartitionId::new(0), 1000, 500)
            .unwrap();

        // Perform other metadata operations
        store
            .update_in_sync_replica("test-topic", PartitionId::new(0), BrokerId(1), false)
            .unwrap();

        // Verify runtime state persists
        let brokers = store.list_brokers_with_status().unwrap();
        let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
        assert_eq!(broker_1.1.last_heartbeat, now);
        assert!(broker_1.1.is_draining);
    }

    #[test]
    fn test_load_from_manifest_clears_runtime_state() {
        let store = create_test_store();
        let now = Utc::now();

        // Record some runtime state
        store
            .record_broker_heartbeat(BrokerId(1), now, true)
            .unwrap();
        store
            .update_partition_offsets("test-topic", PartitionId::new(0), 1000, 500)
            .unwrap();

        // Load a new manifest
        let new_manifest = create_test_manifest();
        store.load_from_manifest(new_manifest).unwrap();

        // Verify runtime state was cleared
        let brokers = store.list_brokers_with_status().unwrap();
        for (_, status) in brokers {
            assert_eq!(
                status.last_heartbeat,
                DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
            );
            assert!(!status.is_draining);
        }
    }
}
