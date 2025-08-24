use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::fmt;
use std::sync::{Arc, Mutex};

pub mod demo;

#[derive(Debug, Clone, PartialEq)]
pub enum MessageQueueError {
    TopicNotFound {
        topic: String,
    },
    ConsumerGroupNotFound {
        group_id: String,
    },
    ConsumerGroupAlreadyExists {
        group_id: String,
    },
    InvalidOffset {
        offset: u64,
        topic: String,
        max_offset: u64,
    },
}

impl fmt::Display for MessageQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageQueueError::TopicNotFound { topic } => write!(f, "Topic {topic} does not exist"),
            MessageQueueError::ConsumerGroupNotFound { group_id } => {
                write!(f, "Consumer group {group_id} does not exist")
            }
            MessageQueueError::ConsumerGroupAlreadyExists { group_id } => {
                write!(f, "Consumer group {group_id} already exists")
            }
            MessageQueueError::InvalidOffset {
                offset,
                topic,
                max_offset,
            } => write!(
                f,
                "Invalid offset {offset} for topic {topic} with max offset {max_offset}"
            ),
        }
    }
}

impl std::error::Error for MessageQueueError {}

impl MessageQueueError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            MessageQueueError::TopicNotFound { .. }
                | MessageQueueError::ConsumerGroupNotFound { .. }
                | MessageQueueError::InvalidOffset { .. }
        )
    }
}

// =============================================================================
// CORE DATA STRUCTURES
// =============================================================================

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RecordWithOffset {
    #[serde(flatten)]
    pub record: Record,
    pub offset: u64,
    pub timestamp: String,
}

impl Record {
    pub fn new(
        key: Option<String>,
        value: String,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
        }
    }
}

impl RecordWithOffset {
    pub fn from_record(record: Record, offset: u64) -> Self {
        let timestamp = chrono::Utc::now().to_rfc3339();
        Self {
            record,
            offset,
            timestamp,
        }
    }
}

// =============================================================================
// QUEUE COMPONENTS
// =============================================================================

#[derive(Debug, Clone)]
pub struct TopicLog {
    records: Vec<RecordWithOffset>,
    next_offset: u64,
}

impl Default for TopicLog {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicLog {
    pub fn new() -> Self {
        TopicLog {
            records: Vec::new(),
            next_offset: 0,
        }
    }

    pub fn append(&mut self, record: Record) -> u64 {
        let current_offset = self.next_offset;
        let record_with_offset = RecordWithOffset::from_record(record, current_offset);
        self.records.push(record_with_offset);
        self.next_offset += 1;
        current_offset
    }

    pub fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Vec<&RecordWithOffset> {
        let start_index = offset as usize;
        if start_index >= self.records.len() {
            return Vec::new();
        }
        let slice = &self.records[start_index..];
        let limited_slice = match count {
            Some(limit) => &slice[..limit.min(slice.len())],
            None => slice,
        };
        limited_slice.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

impl ConsumerGroup {
    pub fn new(group_id: String) -> Self {
        ConsumerGroup {
            group_id,
            topic_offsets: HashMap::new(),
        }
    }

    pub fn get_offset(&self, topic: &str) -> u64 {
        self.topic_offsets.get(topic).copied().unwrap_or(0)
    }

    pub fn set_offset(&mut self, topic: String, offset: u64) {
        self.topic_offsets.insert(topic, offset);
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

pub struct MessageQueue {
    topics: Arc<Mutex<HashMap<String, TopicLog>>>,
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
}

impl Default for MessageQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageQueue {
    pub fn new() -> Self {
        MessageQueue {
            topics: Arc::new(Mutex::new(HashMap::new())),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn post_record(&self, topic: String, record: Record) -> Result<u64, String> {
        let mut topic_log_map = self.topics.lock().unwrap();
        let topic_log = topic_log_map.entry(topic).or_default();
        Ok(topic_log.append(record))
    }

    pub fn post_records(&self, topic: String, records: Vec<Record>) -> Result<Vec<u64>, String> {
        let mut topic_log_map = self.topics.lock().unwrap();
        let topic_log = topic_log_map.entry(topic).or_default();

        let mut offsets = Vec::new();
        for record in records {
            offsets.push(topic_log.append(record));
        }
        Ok(offsets)
    }

    pub fn poll_records(
        &self,
        topic: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, MessageQueueError> {
        self.poll_records_from_offset(topic, 0, count)
    }

    pub fn poll_records_from_offset(
        &self,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, MessageQueueError> {
        let topic_log_map = self.topics.lock().unwrap();
        match topic_log_map.get(topic) {
            Some(topic_log) => Ok(topic_log
                .get_records_from_offset(offset, count)
                .into_iter()
                .cloned()
                .collect()),
            None => Err(MessageQueueError::TopicNotFound {
                topic: topic.to_string(),
            }),
        }
    }

    pub fn create_consumer_group(&self, group_id: String) -> Result<(), MessageQueueError> {
        let mut consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.entry(group_id.clone()) {
            Vacant(entry) => {
                entry.insert(ConsumerGroup::new(group_id));
                Ok(())
            }
            Occupied(_) => Err(MessageQueueError::ConsumerGroupAlreadyExists { group_id }),
        }
    }

    pub fn get_consumer_group_offset(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<u64, MessageQueueError> {
        let consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.get(group_id) {
            Some(consumer_group) => Ok(consumer_group.get_offset(topic)),
            None => Err(MessageQueueError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn update_consumer_group_offset(
        &self,
        group_id: &str,
        topic: String,
        offset: u64,
    ) -> Result<(), MessageQueueError> {
        let topic_log_map = self.topics.lock().unwrap();
        let topic_next_offset = match topic_log_map.get(&topic) {
            Some(topic_log) => topic_log.next_offset(),
            None => {
                return Err(MessageQueueError::TopicNotFound {
                    topic: topic.clone(),
                });
            }
        };

        if offset > topic_next_offset {
            return Err(MessageQueueError::InvalidOffset {
                offset,
                topic: topic.clone(),
                max_offset: topic_next_offset,
            });
        }

        let mut consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.get_mut(group_id) {
            Some(consumer_group) => {
                consumer_group.set_offset(topic, offset);
                Ok(())
            }
            None => Err(MessageQueueError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn delete_consumer_group(&self, group_id: &str) -> Result<(), MessageQueueError> {
        let mut consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.remove(group_id) {
            Some(_) => Ok(()),
            None => Err(MessageQueueError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn poll_records_for_consumer_group(
        &self,
        group_id: &str,
        topic: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, MessageQueueError> {
        let current_offset = self.get_consumer_group_offset(group_id, topic)?;
        let records = self.poll_records_from_offset(topic, current_offset, count)?;
        Ok(records)
    }

    pub fn poll_records_for_consumer_group_from_offset(
        &self,
        group_id: &str,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, MessageQueueError> {
        self.get_consumer_group_offset(group_id, topic)?;
        self.poll_records_from_offset(topic, offset, count)
    }

    pub fn get_high_water_mark(&self, topic: &str) -> u64 {
        let topics = self.topics.lock().unwrap();
        match topics.get(topic) {
            Some(topic_log) => topic_log.next_offset(),
            None => 0,
        }
    }
}

pub mod api;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Test functions updated to use new Record types
    // MessageRecord -> Record, RecordWithOffset -> RecordWithOffset
    #[test]
    fn test_message_record_creation() {
        let record = Record::new(None, "test content".to_string(), None);
        assert_eq!(record.value, "test content");
        assert!(record.key.is_none());
        assert!(record.headers.is_none());
    }

    #[test]
    fn test_message_record_with_key_and_headers() {
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());

        let record = Record::new(
            Some("user123".to_string()),
            "test message".to_string(),
            Some(headers.clone()),
        );

        assert_eq!(record.key.as_ref().unwrap(), "user123");
        assert_eq!(record.value, "test message");
        assert_eq!(record.headers.as_ref().unwrap(), &headers);
    }

    #[test]
    fn test_message_with_offset_creation() {
        let record = Record::new(None, "test content".to_string(), None);
        let message_with_offset = RecordWithOffset::from_record(record.clone(), 42);

        assert_eq!(message_with_offset.record.value, "test content");
        assert_eq!(message_with_offset.offset, 42);
        assert!(message_with_offset.timestamp.contains("T")); // ISO 8601 format
    }

    // =============================================================================
    // QUEUE TESTS
    // =============================================================================

    #[test]
    fn test_queue_creation() {
        let queue = MessageQueue::new();
        // Queue should be created successfully - no panic means success
        drop(queue);
    }

    #[test]
    fn test_post_single_message() {
        let queue = MessageQueue::new();
        let record = Record::new(None, "test message".to_string(), None);
        let result = queue.post_record("test_topic".to_string(), record);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // First message should have offset 0
    }

    #[test]
    fn test_post_multiple_messages_increment_offset() {
        let queue = MessageQueue::new();

        let record1 = Record::new(None, "msg1".to_string(), None);
        let record2 = Record::new(None, "msg2".to_string(), None);
        let record3 = Record::new(None, "msg3".to_string(), None);

        let offset1 = queue.post_record("topic".to_string(), record1).unwrap();
        let offset2 = queue.post_record("topic".to_string(), record2).unwrap();
        let offset3 = queue
            .post_record("different_topic".to_string(), record3)
            .unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 0); // Different topic starts from 0
    }

    #[test]
    fn test_poll_messages_from_existing_topic() {
        let queue = MessageQueue::new();

        let record1 = Record::new(None, "first news".to_string(), None);
        let record2 = Record::new(None, "second news".to_string(), None);

        queue.post_record("news".to_string(), record1).unwrap();
        queue.post_record("news".to_string(), record2).unwrap();

        let messages = queue.poll_records("news", None).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].record.value, "first news");
        assert_eq!(messages[1].record.value, "second news");
        assert_eq!(messages[0].offset, 0);
        assert_eq!(messages[1].offset, 1);
    }

    #[test]
    fn test_poll_messages_with_count_limit() {
        let queue = MessageQueue::new();

        let record1 = Record::new(None, "first news".to_string(), None);
        let record2 = Record::new(None, "second news".to_string(), None);

        queue.post_record("news".to_string(), record1).unwrap();
        queue.post_record("news".to_string(), record2).unwrap();

        let messages = queue.poll_records("news", Some(1)).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].record.value, "first news");
    }

    #[test]
    fn test_poll_nonexistent_topic() {
        let queue = MessageQueue::new();
        let poll_result = queue.poll_records("news", None);
        poll_result.expect_err("Expected error for non-existent topic");
    }

    #[test]
    fn test_fifo_ordering() {
        let queue = MessageQueue::new();

        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        queue.post_record("ordered".to_string(), record1).unwrap();
        queue.post_record("ordered".to_string(), record2).unwrap();
        queue.post_record("ordered".to_string(), record3).unwrap();

        let messages = queue.poll_records("ordered", None).unwrap();
        assert_eq!(messages[0].record.value, "first");
        assert_eq!(messages[1].record.value, "second");
        assert_eq!(messages[2].record.value, "third");

        // Verify timestamps are in ascending order (FIFO)
        assert!(messages[0].timestamp <= messages[1].timestamp);
        assert!(messages[1].timestamp <= messages[2].timestamp);
    }

    #[test]
    fn test_messages_persist_after_polling() {
        let queue = MessageQueue::new();
        let record = Record::new(None, "first news".to_string(), None);
        queue.post_record("news".to_string(), record).unwrap();

        let first_polling_messages = queue.poll_records("news", None).unwrap();
        let second_polling_messages = queue.poll_records("news", None).unwrap();

        assert_eq!(first_polling_messages[0], second_polling_messages[0]);
    }

    #[test]
    fn test_different_topics_isolated() {
        let queue = MessageQueue::new();

        let record_a = Record::new(None, "message for A".to_string(), None);
        let record_b = Record::new(None, "message for B".to_string(), None);

        queue.post_record("topic_a".to_string(), record_a).unwrap();
        queue.post_record("topic_b".to_string(), record_b).unwrap();

        let messages_a = queue.poll_records("topic_a", None).unwrap();
        let messages_b = queue.poll_records("topic_b", None).unwrap();

        assert_eq!(messages_a.len(), 1);
        assert_eq!(messages_b.len(), 1);
        assert_eq!(messages_a[0].record.value, "message for A");
        assert_eq!(messages_b[0].record.value, "message for B");
    }

    #[test]
    fn test_message_with_key_and_headers() {
        let queue = MessageQueue::new();
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "high".to_string());
        headers.insert("source".to_string(), "test-suite".to_string());

        let record = Record::new(
            Some("user456".to_string()),
            "message with metadata".to_string(),
            Some(headers.clone()),
        );

        queue
            .post_record("metadata_topic".to_string(), record)
            .unwrap();
        let messages = queue.poll_records("metadata_topic", None).unwrap();

        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg.record.key.as_ref().unwrap(), "user456");
        assert_eq!(msg.record.value, "message with metadata");
        assert_eq!(msg.record.headers.as_ref().unwrap(), &headers);
        assert_eq!(msg.offset, 0);
    }

    // =============================================================================
    // TOPIC LOG TESTS
    // =============================================================================

    // TopicLog Tests
    #[test]
    fn test_topic_log_creation() {
        let log = TopicLog::new();
        assert_eq!(log.len(), 0);
        assert_eq!(log.next_offset(), 0);
    }

    #[test]
    fn test_topic_log_append_single_message() {
        let mut log = TopicLog::new();
        let record = Record::new(None, "first message".to_string(), None);
        let offset = log.append(record);

        assert_eq!(offset, 0);
        assert_eq!(log.len(), 1);
        assert_eq!(log.next_offset(), 1);
    }

    #[test]
    fn test_topic_log_append_multiple_messages() {
        let mut log = TopicLog::new();
        let record1 = Record::new(None, "message 1".to_string(), None);
        let record2 = Record::new(None, "message 2".to_string(), None);
        let record3 = Record::new(None, "message 3".to_string(), None);

        let offset1 = log.append(record1);
        let offset2 = log.append(record2);
        let offset3 = log.append(record3);

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(log.len(), 3);
        assert_eq!(log.next_offset(), 3);
    }

    #[test]
    fn test_topic_log_get_messages_from_beginning() {
        let mut log = TopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1);
        log.append(record2);
        log.append(record3);

        let messages = log.get_records_from_offset(0, None);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].record.value, "first");
        assert_eq!(messages[1].record.value, "second");
        assert_eq!(messages[2].record.value, "third");
    }

    #[test]
    fn test_topic_log_get_messages_from_middle_offset() {
        let mut log = TopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1);
        log.append(record2);
        log.append(record3);

        let messages = log.get_records_from_offset(1, None);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].record.value, "second");
        assert_eq!(messages[1].record.value, "third");
    }

    #[test]
    fn test_topic_log_get_messages_with_count_limit() {
        let mut log = TopicLog::new();
        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        log.append(record1);
        log.append(record2);
        log.append(record3);

        let messages = log.get_records_from_offset(0, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].record.value, "first");
        assert_eq!(messages[1].record.value, "second");
    }

    #[test]
    fn test_topic_log_get_messages_beyond_log() {
        let mut log = TopicLog::new();
        let record = Record::new(None, "only message".to_string(), None);
        log.append(record);

        let messages = log.get_records_from_offset(5, None);
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_topic_log_message_offsets_match() {
        let mut log = TopicLog::new();
        let record1 = Record::new(None, "msg1".to_string(), None);
        let record2 = Record::new(None, "msg2".to_string(), None);

        let offset1 = log.append(record1);
        let offset2 = log.append(record2);

        let messages = log.get_records_from_offset(0, None);
        assert_eq!(messages[0].offset, offset1);
        assert_eq!(messages[1].offset, offset2);
    }

    // =============================================================================
    // CONSUMER GROUP TESTS
    // =============================================================================

    // ConsumerGroup Tests
    #[test]
    fn test_consumer_group_creation() {
        let group = ConsumerGroup::new("test-group".to_string());
        assert_eq!(group.group_id(), "test-group");
        assert_eq!(group.get_offset("any-topic"), 0);
    }

    #[test]
    fn test_consumer_group_set_and_get_offset() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.set_offset("topic1".to_string(), 5);
        group.set_offset("topic2".to_string(), 10);

        assert_eq!(group.get_offset("topic1"), 5);
        assert_eq!(group.get_offset("topic2"), 10);
        assert_eq!(group.get_offset("nonexistent"), 0);
    }

    #[test]
    fn test_consumer_group_update_offset() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.set_offset("topic".to_string(), 3);
        assert_eq!(group.get_offset("topic"), 3);

        group.set_offset("topic".to_string(), 8);
        assert_eq!(group.get_offset("topic"), 8);
    }

    #[test]
    fn test_consumer_group_multiple_topics() {
        let mut group = ConsumerGroup::new("multi-topic-group".to_string());

        group.set_offset("news".to_string(), 15);
        group.set_offset("alerts".to_string(), 7);
        group.set_offset("logs".to_string(), 42);

        assert_eq!(group.get_offset("news"), 15);
        assert_eq!(group.get_offset("alerts"), 7);
        assert_eq!(group.get_offset("logs"), 42);
        assert_eq!(group.get_offset("unknown"), 0);
    }

    // =============================================================================
    // SERIALIZATION TESTS
    // =============================================================================

    // Serialization Tests
    #[test]
    fn test_message_record_serialization() {
        let mut headers = HashMap::new();
        headers.insert("test".to_string(), "value".to_string());

        let record = Record::new(
            Some("key123".to_string()),
            "test content".to_string(),
            Some(headers),
        );

        let json = serde_json::to_string(&record).expect("Should serialize");
        let deserialized: Record = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_message_with_offset_serialization() {
        let record = Record::new(None, "test".to_string(), None);
        let message_with_offset = RecordWithOffset::from_record(record, 42);

        let json = serde_json::to_string(&message_with_offset).expect("Should serialize");
        let deserialized: RecordWithOffset =
            serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(message_with_offset.record, deserialized.record);
        assert_eq!(message_with_offset.offset, deserialized.offset);
        // Note: timestamp might differ slightly due to precision, so we just check it's present
        assert!(!deserialized.timestamp.is_empty());
    }

    // =============================================================================
    // ERROR CONDITION TESTS
    // =============================================================================

    // Error Condition Tests
    #[test]
    fn test_consumer_group_already_exists_error() {
        let queue = MessageQueue::new();

        // Create a consumer group
        queue
            .create_consumer_group("existing-group".to_string())
            .unwrap();

        // Try to create the same group again
        let result = queue.create_consumer_group("existing-group".to_string());
        assert!(result.is_err());

        if let Err(MessageQueueError::ConsumerGroupAlreadyExists { group_id }) = result {
            assert_eq!(group_id, "existing-group");
        } else {
            panic!("Expected ConsumerGroupAlreadyExists error");
        }
    }

    #[test]
    fn test_consumer_group_not_found_error() {
        let queue = MessageQueue::new();

        // Try to get offset from non-existent group
        let result = queue.get_consumer_group_offset("nonexistent-group", "topic");
        assert!(result.is_err());

        if let Err(MessageQueueError::ConsumerGroupNotFound { group_id }) = result {
            assert_eq!(group_id, "nonexistent-group");
        } else {
            panic!("Expected ConsumerGroupNotFound error");
        }
    }

    #[test]
    fn test_delete_consumer_group_success() {
        let queue = MessageQueue::new();
        let group_id = "test-group";

        // Create a consumer group
        queue.create_consumer_group(group_id.to_string()).unwrap();

        // Create topic by posting a message
        let record = Record::new(None, "test message".to_string(), None);
        queue.post_record("topic1".to_string(), record).unwrap();

        // Set an offset to verify the group exists
        queue
            .update_consumer_group_offset(group_id, "topic1".to_string(), 1)
            .unwrap();

        // Verify the group exists by getting its offset
        let offset = queue.get_consumer_group_offset(group_id, "topic1").unwrap();
        assert_eq!(offset, 1);

        // Delete the consumer group
        let result = queue.delete_consumer_group(group_id);
        assert!(result.is_ok());

        // Verify the group no longer exists
        let get_result = queue.get_consumer_group_offset(group_id, "topic1");
        assert!(get_result.is_err());

        if let Err(MessageQueueError::ConsumerGroupNotFound {
            group_id: error_group_id,
        }) = get_result
        {
            assert_eq!(error_group_id, group_id);
        } else {
            panic!("Expected ConsumerGroupNotFound error after deletion");
        }
    }

    #[test]
    fn test_delete_consumer_group_not_found() {
        let queue = MessageQueue::new();
        let nonexistent_group = "nonexistent-group";

        // Try to delete a non-existent consumer group
        let result = queue.delete_consumer_group(nonexistent_group);
        assert!(result.is_err());

        if let Err(MessageQueueError::ConsumerGroupNotFound { group_id }) = result {
            assert_eq!(group_id, nonexistent_group);
        } else {
            panic!("Expected ConsumerGroupNotFound error");
        }
    }

    #[test]
    fn test_delete_consumer_group_multiple_topics() {
        let queue = MessageQueue::new();
        let group_id = "multi-topic-group";

        // Create a consumer group
        queue.create_consumer_group(group_id.to_string()).unwrap();

        // Create topics by posting messages
        let record1 = Record::new(None, "test message 1".to_string(), None);
        let record2 = Record::new(None, "test message 2".to_string(), None);
        let record3 = Record::new(None, "test message 3".to_string(), None);

        queue.post_record("topic1".to_string(), record1).unwrap();
        queue.post_record("topic2".to_string(), record2).unwrap();
        queue.post_record("topic3".to_string(), record3).unwrap();

        // Set offsets for multiple topics
        queue
            .update_consumer_group_offset(group_id, "topic1".to_string(), 1)
            .unwrap();
        queue
            .update_consumer_group_offset(group_id, "topic2".to_string(), 1)
            .unwrap();
        queue
            .update_consumer_group_offset(group_id, "topic3".to_string(), 1)
            .unwrap();

        // Verify all offsets are set
        assert_eq!(
            queue.get_consumer_group_offset(group_id, "topic1").unwrap(),
            1
        );
        assert_eq!(
            queue.get_consumer_group_offset(group_id, "topic2").unwrap(),
            1
        );
        assert_eq!(
            queue.get_consumer_group_offset(group_id, "topic3").unwrap(),
            1
        );

        // Delete the consumer group
        let result = queue.delete_consumer_group(group_id);
        assert!(result.is_ok());

        // Verify all topic offsets for this group are gone
        assert!(queue.get_consumer_group_offset(group_id, "topic1").is_err());
        assert!(queue.get_consumer_group_offset(group_id, "topic2").is_err());
        assert!(queue.get_consumer_group_offset(group_id, "topic3").is_err());
    }

    #[test]
    fn test_invalid_offset_error() {
        let queue = MessageQueue::new();
        let record = Record::new(None, "test".to_string(), None);

        // Post a message to create topic with offset 0
        queue.post_record("test-topic".to_string(), record).unwrap();

        // Create consumer group and try to set invalid offset (beyond available messages)
        queue
            .create_consumer_group("test-group".to_string())
            .unwrap();
        let result =
            queue.update_consumer_group_offset("test-group", "test-topic".to_string(), 999);

        assert!(result.is_err());
        if let Err(MessageQueueError::InvalidOffset {
            offset,
            topic,
            max_offset,
        }) = result
        {
            assert_eq!(offset, 999);
            assert_eq!(topic, "test-topic");
            assert_eq!(max_offset, 1); // Next offset after one message
        } else {
            panic!("Expected InvalidOffset error");
        }
    }

    #[test]
    fn test_message_queue_error_display() {
        let topic_error = MessageQueueError::TopicNotFound {
            topic: "missing".to_string(),
        };
        assert_eq!(topic_error.to_string(), "Topic missing does not exist");

        let group_error = MessageQueueError::ConsumerGroupNotFound {
            group_id: "missing-group".to_string(),
        };
        assert_eq!(
            group_error.to_string(),
            "Consumer group missing-group does not exist"
        );

        let exists_error = MessageQueueError::ConsumerGroupAlreadyExists {
            group_id: "existing-group".to_string(),
        };
        assert_eq!(
            exists_error.to_string(),
            "Consumer group existing-group already exists"
        );

        let offset_error = MessageQueueError::InvalidOffset {
            offset: 10,
            topic: "test".to_string(),
            max_offset: 5,
        };
        assert_eq!(
            offset_error.to_string(),
            "Invalid offset 10 for topic test with max offset 5"
        );
    }

    #[test]
    fn test_message_queue_error_is_not_found() {
        let topic_error = MessageQueueError::TopicNotFound {
            topic: "missing".to_string(),
        };
        assert!(topic_error.is_not_found());

        let group_error = MessageQueueError::ConsumerGroupNotFound {
            group_id: "missing".to_string(),
        };
        assert!(group_error.is_not_found());

        let offset_error = MessageQueueError::InvalidOffset {
            offset: 10,
            topic: "test".to_string(),
            max_offset: 5,
        };
        assert!(offset_error.is_not_found());

        let exists_error = MessageQueueError::ConsumerGroupAlreadyExists {
            group_id: "existing".to_string(),
        };
        assert!(!exists_error.is_not_found());
    }
}
// =============================================================================
// RECORD TESTS
// =============================================================================
