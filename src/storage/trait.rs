use crate::{Record, RecordWithOffset};
use crate::error::StorageError;
use std::collections::HashMap;

pub trait TopicLog: Send + Sync {
    fn append(&mut self, record: Record) -> Result<u64, StorageError>;
    fn get_records_from_offset(&self, offset: u64, count: Option<usize>) -> Result<Vec<RecordWithOffset>, StorageError>;
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
