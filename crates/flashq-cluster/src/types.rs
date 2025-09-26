//! Core types for cluster metadata management.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a broker in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BrokerId(pub u32);

/// Unique identifier for a partition within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

/// Topic identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub String);

/// Offset within a partition log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub u64);

/// Leader epoch for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(pub u64);

// Implement From/Into traits for ergonomic conversion
impl From<u32> for BrokerId {
    fn from(id: u32) -> Self {
        BrokerId(id)
    }
}

impl From<BrokerId> for u32 {
    fn from(broker_id: BrokerId) -> Self {
        broker_id.0
    }
}

impl From<u32> for PartitionId {
    fn from(id: u32) -> Self {
        PartitionId(id)
    }
}

impl From<PartitionId> for u32 {
    fn from(partition_id: PartitionId) -> Self {
        partition_id.0
    }
}

impl From<String> for TopicId {
    fn from(topic: String) -> Self {
        TopicId(topic)
    }
}

impl From<&str> for TopicId {
    fn from(topic: &str) -> Self {
        TopicId(topic.to_string())
    }
}

impl From<TopicId> for String {
    fn from(topic_id: TopicId) -> Self {
        topic_id.0
    }
}

impl From<u64> for Offset {
    fn from(offset: u64) -> Self {
        Offset(offset)
    }
}

impl From<Offset> for u64 {
    fn from(offset: Offset) -> Self {
        offset.0
    }
}

impl From<u64> for Epoch {
    fn from(epoch: u64) -> Self {
        Epoch(epoch)
    }
}

impl From<Epoch> for u64 {
    fn from(epoch: Epoch) -> Self {
        epoch.0
    }
}

// Implement Display for better formatting
impl fmt::Display for BrokerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "broker-{}", self.0)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partition-{}", self.0)
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "offset-{}", self.0)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "epoch-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_id_conversions() {
        let id: BrokerId = 42u32.into();
        assert_eq!(id, BrokerId(42));

        let raw: u32 = id.into();
        assert_eq!(raw, 42);

        assert_eq!(id.to_string(), "broker-42");
    }

    #[test]
    fn test_partition_id_conversions() {
        let id: PartitionId = 7u32.into();
        assert_eq!(id, PartitionId(7));

        let raw: u32 = id.into();
        assert_eq!(raw, 7);

        assert_eq!(id.to_string(), "partition-7");
    }

    #[test]
    fn test_topic_id_conversions() {
        let id: TopicId = "orders".into();
        assert_eq!(id, TopicId("orders".to_string()));

        let id: TopicId = "inventory".to_string().into();
        assert_eq!(id, TopicId("inventory".to_string()));

        let raw: String = id.into();
        assert_eq!(raw, "inventory");

        let topic_id = TopicId("test".to_string());
        assert_eq!(topic_id.to_string(), "test");
    }

    #[test]
    fn test_offset_conversions() {
        let offset: Offset = 1000u64.into();
        assert_eq!(offset, Offset(1000));

        let raw: u64 = offset.into();
        assert_eq!(raw, 1000);

        assert_eq!(offset.to_string(), "offset-1000");
    }

    #[test]
    fn test_epoch_conversions() {
        let epoch: Epoch = 5u64.into();
        assert_eq!(epoch, Epoch(5));

        let raw: u64 = epoch.into();
        assert_eq!(raw, 5);

        assert_eq!(epoch.to_string(), "epoch-5");
    }

    #[test]
    fn test_epoch_ordering() {
        let epoch1 = Epoch(1);
        let epoch2 = Epoch(2);
        let epoch3 = Epoch(2);

        assert!(epoch1 < epoch2);
        assert_eq!(epoch2, epoch3);
        assert!(epoch2 >= epoch3);
    }

    #[test]
    fn test_serialization() {
        let broker_id = BrokerId(123);
        let json = serde_json::to_string(&broker_id).unwrap();
        let deserialized: BrokerId = serde_json::from_str(&json).unwrap();
        assert_eq!(broker_id, deserialized);

        let topic_id = TopicId("test-topic".to_string());
        let json = serde_json::to_string(&topic_id).unwrap();
        let deserialized: TopicId = serde_json::from_str(&json).unwrap();
        assert_eq!(topic_id, deserialized);
    }
}