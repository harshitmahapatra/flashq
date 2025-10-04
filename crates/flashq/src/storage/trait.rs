use crate::error::StorageError;
use crate::{Record, RecordWithOffset};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PartitionId(pub u32);

impl PartitionId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for PartitionId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<PartitionId> for u32 {
    fn from(partition_id: PartitionId) -> u32 {
        partition_id.0
    }
}

pub trait TopicLog: Send + Sync {
    fn append(&mut self, record: Record) -> Result<u64, StorageError> {
        self.append_partition(PartitionId::new(0), record)
    }

    fn append_batch(&mut self, records: Vec<Record>) -> Result<u64, StorageError> {
        self.append_batch_partition(PartitionId::new(0), records)
    }

    fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        self.read_from_partition(PartitionId::new(0), offset, count)
    }

    fn get_records_from_timestamp(
        &self,
        ts_rfc3339: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        self.read_from_partition_timestamp(PartitionId::new(0), ts_rfc3339, count)
    }

    fn len(&self) -> usize {
        self.partition_len(PartitionId::new(0))
    }

    fn is_empty(&self) -> bool {
        self.partition_is_empty(PartitionId::new(0))
    }

    fn next_offset(&self) -> u64 {
        self.partition_next_offset(PartitionId::new(0))
    }

    fn append_partition(
        &mut self,
        partition_id: PartitionId,
        record: Record,
    ) -> Result<u64, StorageError>;

    fn append_batch_partition(
        &mut self,
        partition_id: PartitionId,
        records: Vec<Record>,
    ) -> Result<u64, StorageError> {
        let mut last = 0;
        for r in records.into_iter() {
            last = self.append_partition(partition_id, r)?;
        }
        Ok(last)
    }

    fn read_from_partition(
        &self,
        partition_id: PartitionId,
        from_offset: u64,
        max_bytes: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError>;

    fn read_from_partition_timestamp(
        &self,
        partition_id: PartitionId,
        ts_rfc3339: &str,
        max_bytes: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError>;

    fn partition_len(&self, partition_id: PartitionId) -> usize;
    fn partition_is_empty(&self, partition_id: PartitionId) -> bool;
    fn partition_next_offset(&self, partition_id: PartitionId) -> u64;
}

pub trait ConsumerGroup: Send + Sync {
    fn offset_store(&self) -> &dyn ConsumerOffsetStore;

    fn get_offset_partition(&self, topic: &str, partition_id: PartitionId) -> u64 {
        self.offset_store()
            .load_snapshot(topic, partition_id)
            .unwrap_or(0)
    }

    fn set_offset_partition(&mut self, topic: String, partition_id: PartitionId, offset: u64) {
        let _ = self
            .offset_store()
            .persist_snapshot(topic, partition_id, offset);
    }

    fn get_all_offsets_partitioned(&self) -> HashMap<(String, PartitionId), u64> {
        self.offset_store().get_all_snapshots().unwrap_or_default()
    }

    fn get_offset(&self, topic: &str) -> u64 {
        self.get_offset_partition(topic, PartitionId(0))
    }

    fn set_offset(&mut self, topic: String, offset: u64) {
        self.set_offset_partition(topic, PartitionId(0), offset)
    }

    fn get_all_offsets(&self) -> HashMap<String, u64> {
        self.get_all_offsets_partitioned()
            .into_iter()
            .filter_map(|((topic, partition_id), offset)| {
                if partition_id == PartitionId(0) {
                    Some((topic, offset))
                } else {
                    None
                }
            })
            .collect()
    }

    fn group_id(&self) -> &str {
        self.offset_store().group_id()
    }
}

/// Snapshot-based consumer offset storage trait for Phase 3+
///
/// This trait replaces the mutable `ConsumerGroup` trait with an immutable
/// snapshot-based approach that supports monotonic offset updates and
/// cluster-wide coordination (Phase 4).
///
/// Key properties:
/// - **Monotonic updates**: `persist_snapshot` only persists if new_offset >= current
/// - **O(1) storage**: Single snapshot per (topic, partition, group)
/// - **Thread-safe**: Implementations must be Send + Sync
/// - **Cluster-ready**: Prepares for leader-follower snapshot replication
pub trait ConsumerOffsetStore: Send + Sync {
    /// Load the committed offset snapshot for a specific topic/partition/group.
    /// Returns 0 if no snapshot exists.
    fn load_snapshot(&self, topic: &str, partition_id: PartitionId) -> Result<u64, StorageError>;

    /// Persist an offset snapshot with monotonic enforcement.
    /// Only persists if new_offset >= current_offset to prevent regression.
    /// Returns Ok(true) if persisted, Ok(false) if rejected as stale.
    fn persist_snapshot(
        &self,
        topic: String,
        partition_id: PartitionId,
        offset: u64,
    ) -> Result<bool, StorageError>;

    /// Get all offset snapshots for this consumer group.
    /// Returns map of (topic, partition) -> offset.
    fn get_all_snapshots(&self) -> Result<HashMap<(String, PartitionId), u64>, StorageError>;

    /// Get the consumer group ID.
    fn group_id(&self) -> &str;
}
