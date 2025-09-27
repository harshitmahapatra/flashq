//! In-memory metadata store implementation.

use crate::{
    ClusterError,
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::r#trait::MetadataStore,
    types::*,
};
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
}

impl ClusterState {
    fn new() -> Self {
        Self {
            brokers: HashMap::new(),
            topics: HashMap::new(),
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

        // Clear existing state
        state.brokers.clear();
        state.topics.clear();

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
}
