use crate::{FlashQError, Record, RecordWithOffset};
use std::collections::HashMap;

pub trait TopicLog: Send + Sync {
    fn append(&mut self, record: Record) -> Result<u64, FlashQError>;
    fn get_records_from_offset(&self, offset: u64, count: Option<usize>) -> Vec<RecordWithOffset>;
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
