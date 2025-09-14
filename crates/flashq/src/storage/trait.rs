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
    fn get_offset(&self, topic: &str) -> u64;
    fn set_offset(&mut self, topic: String, offset: u64);
    fn group_id(&self) -> &str;
    fn get_all_offsets(&self) -> HashMap<String, u64>;
}
