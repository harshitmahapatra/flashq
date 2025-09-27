//! Cluster manifest data structures.

use crate::{ClusterError, types::*};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerSpec {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub id: PartitionId,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    pub in_sync_replicas: Vec<BrokerId>,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicAssignment {
    pub partitions: Vec<PartitionAssignment>,
    pub replication_factor: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterManifest {
    pub brokers: Vec<BrokerSpec>,
    pub topics: HashMap<String, TopicAssignment>,
}

impl ClusterManifest {
    pub fn new() -> Self {
        Self {
            brokers: Vec::new(),
            topics: HashMap::new(),
        }
    }

    pub fn get_broker(&self, broker_id: BrokerId) -> Result<&BrokerSpec, ClusterError> {
        self.brokers
            .iter()
            .find(|broker| broker.id == broker_id)
            .ok_or(ClusterError::BrokerNotFound {
                broker_id: broker_id.into(),
            })
    }

    pub fn get_topic(&self, topic: &str) -> Result<&TopicAssignment, ClusterError> {
        self.topics.get(topic).ok_or(ClusterError::TopicNotFound {
            topic: topic.to_string(),
        })
    }

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
        let partition = manifest
            .get_partition("orders", PartitionId::new(0))
            .unwrap();
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
        assert!(matches!(
            result,
            Err(ClusterError::PartitionNotFound { .. })
        ));
    }
}
