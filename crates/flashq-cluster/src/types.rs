//! Core types for cluster metadata management.

use serde::{Deserialize, Serialize};
use std::fmt;

// Re-export PartitionId from flashq core
pub use flashq::storage::PartitionId;

/// Unique identifier for a broker in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BrokerId(pub u32);

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

    }

    #[test]
    fn test_partition_id_from_flashq() {
        // Test that we can use flashq's PartitionId
        let partition_id = PartitionId::new(42);
        assert_eq!(partition_id.as_u32(), 42);
        assert_eq!(partition_id.to_string(), "42");

        let partition_id2: PartitionId = 7u32.into();
        let raw: u32 = partition_id2.into();
        assert_eq!(raw, 7);
    }
}