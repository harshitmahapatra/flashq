use crate::error::StorageError;
use crate::{Record, RecordWithOffset};
use std::collections::HashMap;

pub trait TopicLog: Send + Sync {
    fn append(&mut self, record: Record) -> Result<u64, StorageError>;
    /// Append a batch of records, taking ownership to avoid cloning.
    /// Default implementation appends one-by-one; implementers may override for efficiency.
    fn append_batch(&mut self, records: Vec<Record>) -> Result<u64, StorageError> {
        let mut last = 0;
        for r in records.into_iter() {
            last = self.append(r)?;
        }
        Ok(last)
    }
    fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn next_offset(&self) -> u64;
}

pub trait ConsumerGroup: Send + Sync {
    fn get_offset(&self, topic: &str) -> u64;
    fn set_offset(&mut self, topic: String, offset: u64);
    fn group_id(&self) -> &str;
    fn get_all_offsets(&self) -> HashMap<String, u64>;
}
