//! Cluster manifest definitions and loading.

use crate::{types::*, ClusterError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Broker specification in the cluster manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerSpec {
    /// Unique broker identifier.
    pub id: BrokerId,
    /// Hostname or IP address.
    pub host: String,
    /// Port number.
    pub port: u16,
}

/// Assignment of a single partition within a topic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    /// Partition identifier.
    pub id: PartitionId,
    /// Current leader broker.
    pub leader: BrokerId,
    /// All replica brokers.
    pub replicas: Vec<BrokerId>,
    /// In-sync replica brokers.
    pub in_sync_replicas: Vec<BrokerId>,
    /// Current leader epoch.
    pub epoch: Epoch,
}

/// Topic assignment configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicAssignment {
    /// Partition assignments for this topic.
    pub partitions: Vec<PartitionAssignment>,
    /// Replication factor for new partitions.
    pub replication_factor: u8,
}

/// Root cluster manifest structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterManifest {
    /// All brokers in the cluster.
    pub brokers: Vec<BrokerSpec>,
    /// Topic assignments by topic name.
    pub topics: HashMap<String, TopicAssignment>,
}

/// Manifest loader with file I/O operations.
pub struct ManifestLoader;

impl ManifestLoader {
    /// Load manifest from file path.
    /// Supports both JSON (.json) and YAML (.yaml/.yml) formats based on file extension.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<ClusterManifest, ClusterError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .map_err(|e| ClusterError::from_io_error(e, "manifest loading"))?;

        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");

        match extension.to_lowercase().as_str() {
            "json" => serde_json::from_str(&content)
                .map_err(|e| ClusterError::from_parse_error(e, "JSON manifest parsing")),
            "yaml" | "yml" => serde_yaml::from_str(&content)
                .map_err(|e| ClusterError::from_parse_error(e, "YAML manifest parsing")),
            _ => {
                // Try JSON first, then YAML as fallback
                serde_json::from_str(&content)
                    .or_else(|_| serde_yaml::from_str(&content))
                    .map_err(|e| ClusterError::from_parse_error(e, "manifest parsing (tried both JSON and YAML)"))
            }
        }
    }

    /// Reload manifest from the same path (placeholder for future watch functionality).
    pub fn reload<P: AsRef<Path>>(path: P) -> Result<ClusterManifest, ClusterError> {
        Self::from_path(path)
    }
}

impl ClusterManifest {
    /// Create a new empty cluster manifest.
    pub fn new() -> Self {
        Self {
            brokers: Vec::new(),
            topics: HashMap::new(),
        }
    }

    /// Get broker by ID.
    pub fn get_broker(&self, broker_id: BrokerId) -> Result<&BrokerSpec, ClusterError> {
        self.brokers
            .iter()
            .find(|broker| broker.id == broker_id)
            .ok_or(ClusterError::BrokerNotFound {
                broker_id: broker_id.into(),
            })
    }

    /// Get topic assignment.
    pub fn get_topic(&self, topic: &str) -> Result<&TopicAssignment, ClusterError> {
        self.topics.get(topic).ok_or(ClusterError::TopicNotFound {
            topic: topic.to_string(),
        })
    }

    /// Get partition assignment for a specific topic and partition.
    pub fn get_partition(
        &self,
        topic: &str,
        partition_id: PartitionId,
    ) -> Result<&PartitionAssignment, ClusterError> {
        let topic_assignment = self.get_topic(topic)?;
        topic_assignment
            .partitions
            .iter()
            .find(|partition| partition.id == partition_id)
            .ok_or(ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
            })
    }
}

impl Default for ClusterManifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

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
            ],
            topics: [(
                "orders".to_string(),
                TopicAssignment {
                    replication_factor: 3,
                    partitions: vec![PartitionAssignment {
                        id: PartitionId::new(0),
                        leader: BrokerId(1),
                        replicas: vec![BrokerId(1), BrokerId(2)],
                        in_sync_replicas: vec![BrokerId(1), BrokerId(2)],
                        epoch: Epoch(4),
                    }],
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn test_manifest_serialization() {
        let manifest = create_test_manifest();
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let deserialized: ClusterManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, deserialized);
    }

    #[test]
    fn test_manifest_loading() {
        let manifest = create_test_manifest();
        let json = serde_json::to_string_pretty(&manifest).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();

        let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();
        assert_eq!(manifest, loaded);
    }

    #[test]
    fn test_manifest_access() {
        let manifest = create_test_manifest();

        // Test broker access
        let broker = manifest.get_broker(BrokerId(1)).unwrap();
        assert_eq!(broker.host, "127.0.0.1");
        assert_eq!(broker.port, 6001);

        // Test topic access
        let topic = manifest.get_topic("orders").unwrap();
        assert_eq!(topic.replication_factor, 3);

        // Test partition access
        let partition = manifest.get_partition("orders", PartitionId::new(0)).unwrap();
        assert_eq!(partition.leader, BrokerId(1));
        assert_eq!(partition.epoch, Epoch(4));
    }

    #[test]
    fn test_manifest_errors() {
        let manifest = create_test_manifest();

        // Test broker not found
        let result = manifest.get_broker(BrokerId(999));
        assert!(matches!(result, Err(ClusterError::BrokerNotFound { .. })));

        // Test topic not found
        let result = manifest.get_topic("nonexistent");
        assert!(matches!(result, Err(ClusterError::TopicNotFound { .. })));

        // Test partition not found
        let result = manifest.get_partition("orders", PartitionId::new(999));
        assert!(matches!(result, Err(ClusterError::PartitionNotFound { .. })));
    }
}