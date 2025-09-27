//! Integration tests for file-based metadata store implementation.

use chrono::Utc;
use flashq_cluster::{
    manifest::types::{BrokerSpec, ClusterManifest, PartitionAssignment, TopicAssignment},
    metadata_store::{FileMetadataStore, MetadataStore},
    types::*,
};
use std::collections::HashMap;
use tempfile::TempDir;

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

fn create_test_store() -> (FileMetadataStore, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let manifest = create_test_manifest();
    let store = FileMetadataStore::new_with_manifest(temp_dir.path(), manifest).unwrap();
    (store, temp_dir)
}

#[test]
fn test_file_store_creation() {
    let temp_dir = TempDir::new().unwrap();
    let store = FileMetadataStore::new(temp_dir.path()).unwrap();

    let manifest = store.export_to_manifest().unwrap();
    assert!(manifest.brokers.is_empty());
    assert!(manifest.topics.is_empty());

    // Verify file was created
    let metadata_file = temp_dir.path().join("cluster_metadata.json");
    assert!(metadata_file.exists());
}

#[test]
fn test_file_store_with_manifest() {
    let (store, _temp_dir) = create_test_store();

    let exported = store.export_to_manifest().unwrap();
    assert_eq!(exported.brokers.len(), 3);
    assert_eq!(exported.topics.len(), 1);
    assert!(exported.topics.contains_key("test-topic"));
}

#[test]
fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let manifest = create_test_manifest();

    // Create store and load manifest
    {
        let store = FileMetadataStore::new_with_manifest(temp_dir.path(), manifest).unwrap();

        // Perform some operations
        store
            .record_broker_heartbeat(BrokerId(1), Utc::now(), false)
            .unwrap();
        store
            .update_partition_offsets("test-topic", PartitionId::new(0), 100, 50)
            .unwrap();

        let epoch = store
            .bump_leader_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(6));
    }

    // Create new store from same directory
    {
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        // Verify data persisted
        let exported = store.export_to_manifest().unwrap();
        assert_eq!(exported.brokers.len(), 3);
        assert_eq!(exported.topics.len(), 1);

        // Verify epoch was persisted
        let epoch = store
            .get_partition_epoch("test-topic", PartitionId::new(0))
            .unwrap();
        assert_eq!(epoch, Epoch(6));

        // Verify broker heartbeat persisted
        let brokers = store.list_brokers_with_status().unwrap();
        let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
        assert!(!broker_1.1.is_draining);
    }
}

#[test]
fn test_basic_operations() {
    let (store, _temp_dir) = create_test_store();

    // Test partition leader
    let leader = store
        .get_partition_leader("test-topic", PartitionId::new(0))
        .unwrap();
    assert_eq!(leader, BrokerId(1));

    // Test in-sync replicas
    let replicas = store
        .get_in_sync_replicas("test-topic", PartitionId::new(0))
        .unwrap();
    assert_eq!(replicas.len(), 3);

    // Test epoch operations
    let epoch = store
        .get_partition_epoch("test-topic", PartitionId::new(0))
        .unwrap();
    assert_eq!(epoch, Epoch(5));

    let new_epoch = store
        .bump_leader_epoch("test-topic", PartitionId::new(0))
        .unwrap();
    assert_eq!(new_epoch, Epoch(6));
}

#[test]
fn test_broker_heartbeat_operations() {
    let (store, _temp_dir) = create_test_store();
    let now = Utc::now();

    // Record heartbeat
    store
        .record_broker_heartbeat(BrokerId(1), now, true)
        .unwrap();

    // Verify heartbeat was recorded
    let brokers = store.list_brokers_with_status().unwrap();
    let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
    assert_eq!(broker_1.1.last_heartbeat, now);
    assert!(broker_1.1.is_draining);
}

#[test]
fn test_compare_and_set_operations() {
    let (store, _temp_dir) = create_test_store();

    // Successful CAS
    let success = store
        .compare_and_set_epoch("test-topic", PartitionId::new(0), Epoch(5), Epoch(6))
        .unwrap();
    assert!(success);

    // Failed CAS
    let success = store
        .compare_and_set_epoch("test-topic", PartitionId::new(0), Epoch(5), Epoch(7))
        .unwrap();
    assert!(!success);
}

#[test]
fn test_error_handling() {
    let (store, _temp_dir) = create_test_store();

    // Test non-existent topic
    let result = store.get_partition_leader("nonexistent", PartitionId::new(0));
    assert!(matches!(
        result,
        Err(flashq_cluster::ClusterError::TopicNotFound { .. })
    ));

    // Test unknown broker heartbeat
    let result = store.record_broker_heartbeat(BrokerId(999), Utc::now(), false);
    assert!(matches!(
        result,
        Err(flashq_cluster::ClusterError::UnknownBroker { .. })
    ));
}

#[test]
fn test_concurrent_modifications() {
    let (store, _temp_dir) = create_test_store();

    // Test that concurrent operations are properly serialized
    let store = std::sync::Arc::new(store);
    let mut handles = vec![];

    for i in 0..10 {
        let store_clone = std::sync::Arc::clone(&store);
        let handle = std::thread::spawn(move || {
            // Each thread performs multiple operations
            for _ in 0..5 {
                let _ = store_clone.bump_leader_epoch("test-topic", PartitionId::new(0));
            }

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

#[test]
fn test_large_manifest_persistence() {
    let temp_dir = TempDir::new().unwrap();

    // Create a large manifest with many brokers and topics
    let mut brokers = Vec::new();
    for i in 1..=100 {
        brokers.push(BrokerSpec {
            id: BrokerId(i),
            host: format!("broker-{i}.example.com"),
            port: 6000 + i as u16,
        });
    }

    let mut topics = HashMap::new();
    for i in 0..50 {
        let mut partitions = Vec::new();
        for j in 0..10 {
            partitions.push(PartitionAssignment {
                id: PartitionId::new(j),
                leader: BrokerId((j % 100) + 1),
                replicas: vec![
                    BrokerId((j % 100) + 1),
                    BrokerId(((j + 1) % 100) + 1),
                    BrokerId(((j + 2) % 100) + 1),
                ],
                in_sync_replicas: vec![BrokerId((j % 100) + 1), BrokerId(((j + 1) % 100) + 1)],
                epoch: Epoch(1),
            });
        }

        topics.insert(
            format!("topic-{i}"),
            TopicAssignment {
                replication_factor: 3,
                partitions,
            },
        );
    }

    let large_manifest = ClusterManifest { brokers, topics };

    // Test persistence with large manifest
    {
        let store = FileMetadataStore::new_with_manifest(temp_dir.path(), large_manifest).unwrap();

        // Perform some operations
        store
            .record_broker_heartbeat(BrokerId(1), Utc::now(), false)
            .unwrap();
        store
            .update_partition_offsets("topic-0", PartitionId::new(0), 1000, 500)
            .unwrap();
    }

    // Verify persistence
    {
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();
        let exported = store.export_to_manifest().unwrap();

        assert_eq!(exported.brokers.len(), 100);
        assert_eq!(exported.topics.len(), 50);

        // Verify specific data
        assert!(exported.topics.contains_key("topic-0"));
        assert!(exported.topics.contains_key("topic-49"));

        let brokers = store.list_brokers_with_status().unwrap();
        assert_eq!(brokers.len(), 100);
    }
}

#[test]
fn test_corrupted_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_file = temp_dir.path().join("cluster_metadata.json");

    // Write corrupted JSON to the file
    std::fs::write(&metadata_file, "{ invalid json }").unwrap();

    // Should handle corrupted file gracefully
    let result = FileMetadataStore::new(temp_dir.path());
    assert!(result.is_err());
}

#[test]
fn test_empty_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_file = temp_dir.path().join("cluster_metadata.json");

    // Create empty file
    std::fs::write(&metadata_file, "").unwrap();

    // Should handle empty file gracefully
    let store = FileMetadataStore::new(temp_dir.path()).unwrap();
    let manifest = store.export_to_manifest().unwrap();
    assert!(manifest.brokers.is_empty());
    assert!(manifest.topics.is_empty());
}

#[test]
fn test_file_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let _store = FileMetadataStore::new(temp_dir.path()).unwrap();

    // Verify the file was created with appropriate permissions
    let metadata_file = temp_dir.path().join("cluster_metadata.json");
    assert!(metadata_file.exists());

    // Should be readable and writable
    let metadata = std::fs::metadata(&metadata_file).unwrap();
    assert!(!metadata.permissions().readonly());
}

#[test]
fn test_runtime_state_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let manifest = create_test_manifest();
    let now = Utc::now();

    // Create store and set up runtime state
    {
        let store = FileMetadataStore::new_with_manifest(temp_dir.path(), manifest).unwrap();

        // Record broker heartbeats
        store
            .record_broker_heartbeat(BrokerId(1), now, false)
            .unwrap();
        store
            .record_broker_heartbeat(BrokerId(2), now, true)
            .unwrap();

        // Update partition offsets
        store
            .update_partition_offsets("test-topic", PartitionId::new(0), 1000, 500)
            .unwrap();
        store
            .update_partition_offsets("test-topic", PartitionId::new(1), 2000, 1000)
            .unwrap();
    }

    // Restart and verify runtime state persisted
    {
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        // Check broker heartbeats
        let brokers = store.list_brokers_with_status().unwrap();
        let broker_1 = brokers.iter().find(|(id, _)| *id == BrokerId(1)).unwrap();
        let broker_2 = brokers.iter().find(|(id, _)| *id == BrokerId(2)).unwrap();

        assert_eq!(broker_1.1.last_heartbeat, now);
        assert!(!broker_1.1.is_draining);

        assert_eq!(broker_2.1.last_heartbeat, now);
        assert!(broker_2.1.is_draining);

        // Note: Partition offsets can't be directly verified since there's no getter
        // method in the trait yet, but the persistence should work for when it's added
    }
}
