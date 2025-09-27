//! Integration tests for cluster manifest loading and parsing.

use flashq_cluster::manifest::{
    BrokerSpec, ClusterManifest, ManifestLoader, PartitionAssignment, TopicAssignment,
};
use flashq_cluster::types::{BrokerId, Epoch, PartitionId};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_manifest_load_save_round_trip() {
    // Create test manifest
    let original_manifest = create_test_manifest();

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&original_manifest).unwrap();

    // Write to temporary file
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json.as_bytes()).unwrap();

    // Load from file
    let loaded_manifest = ManifestLoader::from_path(temp_file.path()).unwrap();

    // Verify round-trip
    assert_eq!(original_manifest, loaded_manifest);
}

#[test]
fn test_manifest_broker_lookup() {
    let manifest = create_test_manifest();

    // Test successful broker lookup
    let broker = manifest.get_broker(BrokerId(1)).unwrap();
    assert_eq!(broker.host, "127.0.0.1");
    assert_eq!(broker.port, 6001);

    // Test broker not found
    let result = manifest.get_broker(BrokerId(999));
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

#[test]
fn test_manifest_topic_lookup() {
    let manifest = create_test_manifest();

    // Test successful topic lookup
    let topic = manifest.get_topic("orders").unwrap();
    assert_eq!(topic.replication_factor, 3);
    assert_eq!(topic.partitions.len(), 2);

    // Test topic not found
    let result = manifest.get_topic("nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

#[test]
fn test_manifest_partition_lookup() {
    let manifest = create_test_manifest();

    // Test successful partition lookup
    let partition = manifest
        .get_partition("orders", PartitionId::new(0))
        .unwrap();
    assert_eq!(partition.leader, BrokerId(1));
    assert_eq!(partition.epoch, Epoch(4));
    assert_eq!(
        partition.replicas,
        vec![BrokerId(1), BrokerId(2), BrokerId(3)]
    );
    assert_eq!(
        partition.in_sync_replicas,
        vec![BrokerId(1), BrokerId(2), BrokerId(3)]
    );

    // Test partition not found
    let result = manifest.get_partition("orders", PartitionId::new(999));
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());
}

#[test]
fn test_manifest_reload_functionality() {
    let manifest = create_test_manifest();
    let json = serde_json::to_string_pretty(&manifest).unwrap();

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json.as_bytes()).unwrap();

    // Initial load
    let loaded1 = ManifestLoader::from_path(temp_file.path()).unwrap();

    // Reload (should produce identical result)
    let loaded2 = ManifestLoader::reload(temp_file.path()).unwrap();

    assert_eq!(loaded1, loaded2);
    assert_eq!(manifest, loaded2);
}

#[test]
fn test_manifest_parsing_errors() {
    // Test invalid JSON
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(b"invalid json").unwrap();

    let result = ManifestLoader::from_path(temp_file.path());
    assert!(result.is_err());
    assert!(result.unwrap_err().is_client_error());
}

#[test]
fn test_yaml_manifest_loading() {
    // Test with a human-readable YAML format
    let yaml_content = r#"
# FlashQ Cluster Configuration
brokers:
  - id: 1
    host: "127.0.0.1"
    port: 6001
  - id: 2
    host: "127.0.0.1"
    port: 6002
  - id: 3
    host: "127.0.0.1"
    port: 6003

topics:
  orders:
    replication_factor: 3
    partitions:
      - id: 0
        leader: 1
        replicas: [1, 2, 3]
        in_sync_replicas: [1, 2, 3]
        epoch: 4
      - id: 1
        leader: 2
        replicas: [2, 3, 1]
        in_sync_replicas: [2, 3]
        epoch: 2
  inventory:
    replication_factor: 2
    partitions:
      - id: 0
        leader: 3
        replicas: [3, 1]
        in_sync_replicas: [3, 1]
        epoch: 1
"#;

    let mut temp_file = NamedTempFile::with_suffix(".yaml").unwrap();
    temp_file.write_all(yaml_content.as_bytes()).unwrap();

    let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();

    // Verify the loaded manifest structure
    assert_eq!(loaded.brokers.len(), 3);
    assert_eq!(loaded.topics.len(), 2);

    // Verify broker details
    let broker1 = loaded.get_broker(BrokerId(1)).unwrap();
    assert_eq!(broker1.host, "127.0.0.1");
    assert_eq!(broker1.port, 6001);

    // Verify topic details
    let orders = loaded.get_topic("orders").unwrap();
    assert_eq!(orders.replication_factor, 3);
    assert_eq!(orders.partitions.len(), 2);

    let inventory = loaded.get_topic("inventory").unwrap();
    assert_eq!(inventory.replication_factor, 2);
    assert_eq!(inventory.partitions.len(), 1);

    // Verify partition details
    let partition = loaded.get_partition("orders", PartitionId::new(0)).unwrap();
    assert_eq!(partition.leader, BrokerId(1));
    assert_eq!(partition.epoch, Epoch(4));
    assert_eq!(partition.replicas.len(), 3);
    assert_eq!(partition.in_sync_replicas.len(), 3);
}

#[test]
fn test_yml_extension_support() {
    let manifest = create_test_manifest();
    let yaml = serde_yaml::to_string(&manifest).unwrap();

    let mut temp_file = NamedTempFile::with_suffix(".yml").unwrap();
    temp_file.write_all(yaml.as_bytes()).unwrap();

    let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();
    assert_eq!(manifest, loaded);
}

#[test]
fn test_json_extension_support() {
    let manifest = create_test_manifest();
    let json = serde_json::to_string_pretty(&manifest).unwrap();

    let mut temp_file = NamedTempFile::with_suffix(".json").unwrap();
    temp_file.write_all(json.as_bytes()).unwrap();

    let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();
    assert_eq!(manifest, loaded);
}

#[test]
fn test_unknown_extension_fallback() {
    let manifest = create_test_manifest();

    // Test with JSON content and unknown extension
    let json = serde_json::to_string_pretty(&manifest).unwrap();
    let mut temp_file = NamedTempFile::with_suffix(".txt").unwrap();
    temp_file.write_all(json.as_bytes()).unwrap();

    let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();
    assert_eq!(manifest, loaded);

    // Test with YAML content and unknown extension
    let yaml = serde_yaml::to_string(&manifest).unwrap();
    let mut temp_file2 = NamedTempFile::with_suffix(".conf").unwrap();
    temp_file2.write_all(yaml.as_bytes()).unwrap();

    let loaded2 = ManifestLoader::from_path(temp_file2.path()).unwrap();
    assert_eq!(manifest, loaded2);
}

#[test]
fn test_yaml_parsing_error() {
    let mut temp_file = NamedTempFile::with_suffix(".yaml").unwrap();
    temp_file.write_all(b"invalid: yaml: content: [").unwrap();

    let result = ManifestLoader::from_path(temp_file.path());
    assert!(result.is_err());
    assert!(result.unwrap_err().is_client_error());
}

fn create_test_manifest() -> ClusterManifest {
    ClusterManifest {
        brokers: vec![
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
        ],
        topics: [
            (
                "orders".to_string(),
                TopicAssignment {
                    replication_factor: 3,
                    partitions: vec![
                        PartitionAssignment {
                            id: PartitionId::new(0),
                            leader: BrokerId(1),
                            replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                            in_sync_replicas: vec![BrokerId(1), BrokerId(2), BrokerId(3)],
                            epoch: Epoch(4),
                        },
                        PartitionAssignment {
                            id: PartitionId::new(1),
                            leader: BrokerId(2),
                            replicas: vec![BrokerId(2), BrokerId(3), BrokerId(1)],
                            in_sync_replicas: vec![BrokerId(2), BrokerId(3)],
                            epoch: Epoch(2),
                        },
                    ],
                },
            ),
            (
                "inventory".to_string(),
                TopicAssignment {
                    replication_factor: 2,
                    partitions: vec![PartitionAssignment {
                        id: PartitionId::new(0),
                        leader: BrokerId(3),
                        replicas: vec![BrokerId(3), BrokerId(1)],
                        in_sync_replicas: vec![BrokerId(3), BrokerId(1)],
                        epoch: Epoch(1),
                    }],
                },
            ),
        ]
        .into_iter()
        .collect(),
    }
}
