use super::{ConsumerGroup, TopicLog};
use crate::{Record, RecordWithOffset};
use crate::error::StorageError;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct InMemoryTopicLog {
    records: Vec<RecordWithOffset>,
    next_offset: u64,
}

impl Default for InMemoryTopicLog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTopicLog {
    pub fn new() -> Self {
        InMemoryTopicLog {
            records: Vec::new(),
            next_offset: 0,
        }
    }
}

impl TopicLog for InMemoryTopicLog {
    fn append(&mut self, record: Record) -> Result<u64, StorageError> {
        let current_offset = self.next_offset;
        let record_with_offset = RecordWithOffset::from_record(record, current_offset);
        self.records.push(record_with_offset);
        self.next_offset += 1;
        Ok(current_offset)
    }

    fn get_records_from_offset(&self, offset: u64, count: Option<usize>) -> Result<Vec<RecordWithOffset>, StorageError> {
        let start_index = offset
            .try_into()
            .map_err(|_| StorageError::DataCorruption { 
                context: "memory storage".to_string(),
                details: format!("offset {} too large to convert to array index", offset)
            })?;
        
        if start_index >= self.records.len() {
            return Ok(Vec::new());
        }
        
        let slice = &self.records[start_index..];
        let limited_slice = match count {
            Some(limit) => &slice[..limit.min(slice.len())],
            None => slice,
        };
        Ok(limited_slice.to_vec())
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;

    #[test]
    fn test_topic_log_creation() {
        let log = InMemoryTopicLog::new();
        assert_eq!(log.len(), 0);
        assert_eq!(log.next_offset(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_topic_log_append_single_record() {
        let mut log = InMemoryTopicLog::new();
        let record = Record::new(None, "first record".to_string(), None);
        let offset = log.append(record).unwrap();

        assert_eq!(offset, 0);
        assert_eq!(log.len(), 1);
        assert_eq!(log.next_offset(), 1);
        assert!(!log.is_empty());
    }

    #[test]
    fn test_topic_log_append_multiple_records() {
        let mut log = InMemoryTopicLog::new();
        let record1 = Record::new(None, "record 1".to_string(), None);
        let record2 = Record::new(None, "record 2".to_string(), None);
        let record3 = Record::new(None, "record 3".to_string(), None);

        let offset1 = log.append(record1).unwrap();
        let offset2 = log.append(record2).unwrap();
        let offset3 = log.append(record3).unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(log.len(), 3);
        assert_eq!(log.next_offset(), 3);
    }

    #[test]
    fn test_topic_log_get_records_from_beginning() {
        let mut log = InMemoryTopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1).unwrap();
        log.append(record2).unwrap();
        log.append(record3).unwrap();

        let records = log.get_records_from_offset(0, None).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record.value, "first");
        assert_eq!(records[1].record.value, "second");
        assert_eq!(records[2].record.value, "third");
    }

    #[test]
    fn test_topic_log_get_records_from_middle_offset() {
        let mut log = InMemoryTopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1).unwrap();
        log.append(record2).unwrap();
        log.append(record3).unwrap();

        let records = log.get_records_from_offset(1, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "second");
        assert_eq!(records[1].record.value, "third");
    }

    #[test]
    fn test_topic_log_get_records_with_count_limit() {
        let mut log = InMemoryTopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1).unwrap();
        log.append(record2).unwrap();
        log.append(record3).unwrap();

        let records = log.get_records_from_offset(0, Some(2)).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "first");
        assert_eq!(records[1].record.value, "second");
    }

    #[test]
    fn test_topic_log_get_records_beyond_log() {
        let mut log = InMemoryTopicLog::new();
        let record = Record::new(None, "only record".to_string(), None);
        log.append(record).unwrap();

        let records = log.get_records_from_offset(5, None).unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_topic_log_record_offsets_match() {
        let mut log = InMemoryTopicLog::new();
        let record1 = Record::new(None, "msg1".to_string(), None);
        let record2 = Record::new(None, "msg2".to_string(), None);

        let offset1 = log.append(record1).unwrap();
        let offset2 = log.append(record2).unwrap();

        let records = log.get_records_from_offset(0, None).unwrap();
        assert_eq!(records[0].offset, offset1);
        assert_eq!(records[1].offset, offset2);
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryConsumerGroup {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

impl InMemoryConsumerGroup {
    pub fn new(group_id: String) -> Self {
        InMemoryConsumerGroup {
            group_id,
            topic_offsets: HashMap::new(),
        }
    }
}

impl ConsumerGroup for InMemoryConsumerGroup {
    fn get_offset(&self, topic: &str) -> u64 {
        self.topic_offsets.get(topic).copied().unwrap_or(0)
    }

    fn set_offset(&mut self, topic: String, offset: u64) {
        self.topic_offsets.insert(topic, offset);
    }

    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn get_all_offsets(&self) -> HashMap<String, u64> {
        self.topic_offsets.clone()
    }
}
