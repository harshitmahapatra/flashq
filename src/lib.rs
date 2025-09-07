use dashmap::DashMap;
use dashmap::mapref::entry::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use storage::{ConsumerGroup, TopicLog};

pub mod demo;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod storage;

pub use error::FlashQError;

// Re-export logging macros for consistent usage across the crate
pub use log::{debug, error, info, trace, warn};

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

pub struct FlashQ {
    topics: Arc<DashMap<String, Arc<RwLock<dyn TopicLog>>>>,
    consumer_groups: Arc<DashMap<String, Arc<RwLock<dyn ConsumerGroup>>>>,
    storage_backend: storage::StorageBackend,
}

impl Default for FlashQ {
    fn default() -> Self {
        Self::new()
    }
}

impl FlashQ {
    pub fn new() -> Self {
        Self::with_storage_backend(storage::StorageBackend::new_memory())
    }

    pub fn with_storage_backend(storage_backend: storage::StorageBackend) -> Self {
        let queue = FlashQ {
            topics: Arc::new(DashMap::new()),
            consumer_groups: Arc::new(DashMap::new()),
            storage_backend,
        };

        // For file backends, recover existing topics and consumer groups from disk
        queue.recover_existing_topics().unwrap_or_else(|e| {
            warn!("Failed to recover existing topics: {e}");
        });

        queue
            .recover_existing_consumer_groups()
            .unwrap_or_else(|e| {
                warn!("Failed to recover existing consumer groups: {e}");
            });

        queue
    }

    pub fn post_records(&self, topic: String, records: Vec<Record>) -> Result<u64, FlashQError> {
        let topic_log = self.topics.entry(topic.clone()).or_insert_with(|| {
            self.storage_backend
                .create(&topic)
                .expect("Failed to create storage backend")
        });

        let mut offsets = Vec::new();
        let mut topic_log_locked = topic_log.value().write().unwrap();
        for record in records {
            offsets.push(topic_log_locked.append(record).map_err(FlashQError::from)?);
        }
        Ok(offsets.last().cloned().unwrap_or_default())
    }

    pub fn poll_records(
        &self,
        topic: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        self.poll_records_from_offset(topic, 0, count)
    }

    pub fn poll_records_from_offset(
        &self,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        match self.topics.get(topic) {
            Some(topic_log) => topic_log
                .value()
                .read()
                .unwrap()
                .get_records_from_offset(offset, count)
                .map_err(FlashQError::from),
            None => Err(FlashQError::TopicNotFound {
                topic: topic.to_string(),
            }),
        }
    }

    pub fn create_consumer_group(&self, group_id: String) -> Result<(), FlashQError> {
        match self.consumer_groups.entry(group_id.clone()) {
            Occupied(_) => Err(FlashQError::ConsumerGroupAlreadyExists { group_id }),
            Vacant(entry) => {
                let consumer_group = self
                    .storage_backend
                    .create_consumer_group(&group_id)
                    .map_err(|e| FlashQError::ConsumerGroupAlreadyExists {
                        group_id: format!("Failed to create consumer group: {e}"),
                    })?;
                entry.insert(consumer_group);
                Ok(())
            }
        }
    }

    pub fn get_consumer_group_offset(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<u64, FlashQError> {
        match self.consumer_groups.get(group_id) {
            Some(consumer_group) => Ok(consumer_group.value().read().unwrap().get_offset(topic)),
            None => Err(FlashQError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn update_consumer_group_offset(
        &self,
        group_id: &str,
        topic: String,
        offset: u64,
    ) -> Result<(), FlashQError> {
        let topic_next_offset = match self.topics.get(&topic) {
            Some(topic_log) => topic_log.value().write().unwrap().next_offset(),
            None => {
                return Err(FlashQError::TopicNotFound {
                    topic: topic.clone(),
                });
            }
        };

        if offset > topic_next_offset {
            return Err(FlashQError::InvalidOffset {
                offset,
                topic: topic.clone(),
                max_offset: topic_next_offset,
            });
        }

        match self.consumer_groups.get_mut(group_id) {
            Some(consumer_group) => {
                consumer_group
                    .value()
                    .write()
                    .unwrap()
                    .set_offset(topic, offset);
                Ok(())
            }
            None => Err(FlashQError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn delete_consumer_group(&self, group_id: &str) -> Result<(), FlashQError> {
        match self.consumer_groups.remove(group_id) {
            Some(_) => Ok(()),
            None => Err(FlashQError::ConsumerGroupNotFound {
                group_id: group_id.to_string(),
            }),
        }
    }

    pub fn poll_records_for_consumer_group(
        &self,
        group_id: &str,
        topic: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        let current_offset = self.get_consumer_group_offset(group_id, topic)?;
        let records = self.poll_records_from_offset(topic, current_offset, count)?;

        // Record that this consumer group has accessed this topic (with offset 0 if first time)
        // This ensures the topic appears in the consumer group's JSON file
        if current_offset == 0 {
            self.update_consumer_group_offset(group_id, topic.to_string(), 0)?;
        }

        Ok(records)
    }

    pub fn poll_records_for_consumer_group_from_offset(
        &self,
        group_id: &str,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        self.get_consumer_group_offset(group_id, topic)?;
        self.poll_records_from_offset(topic, offset, count)
    }

    pub fn get_high_water_mark(&self, topic: &str) -> u64 {
        match self.topics.get(topic) {
            Some(topic_log) => topic_log.value().read().unwrap().next_offset(),
            None => 0,
        }
    }

    /// Recover existing topics from disk for file storage backends
    fn recover_existing_topics(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Discover existing topics using the storage backend
        let topic_names = self
            .storage_backend
            .discover_topics()
            .map_err(|e| format!("Failed to discover topics: {e}"))?;

        // Create TopicLog instances for discovered topics
        for topic_name in topic_names {
            if let Ok(topic_log) = self.storage_backend.create(&topic_name) {
                self.topics.insert(topic_name, topic_log);
            }
        }

        Ok(())
    }

    /// Recover existing consumer groups from disk for file storage backends  
    fn recover_existing_consumer_groups(&self) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        // Only recover for file storage backends
        let data_dir = match &self.storage_backend {
            storage::StorageBackend::File { data_dir, .. } => data_dir.clone(),
            storage::StorageBackend::Memory => return Ok(()), // No recovery needed for memory
        };

        let consumer_groups_dir = data_dir.join("consumer_groups");

        // Check if consumer groups directory exists
        if !consumer_groups_dir.exists() {
            return Ok(());
        }

        // Scan for .json files (consumer group files)
        let entries = fs::read_dir(&consumer_groups_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if let Some(extension) = path.extension() {
                if extension == "json" {
                    if let Some(file_name) = path.file_stem() {
                        if let Some(group_id) = file_name.to_str() {
                            // Create ConsumerGroup for this group
                            if let Ok(consumer_group) =
                                self.storage_backend.create_consumer_group(group_id)
                            {
                                self.consumer_groups
                                    .insert(group_id.to_string(), consumer_group);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Test functions updated to use new Record types
    // Using Record and RecordWithOffset types
    #[test]
    fn test_record_creation() {
        let record = Record::new(None, "test content".to_string(), None);
        assert_eq!(record.value, "test content");
        assert!(record.key.is_none());
        assert!(record.headers.is_none());
    }

    #[test]
    fn test_record_with_key_and_headers() {
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());

        let record = Record::new(
            Some("user123".to_string()),
            "test record".to_string(),
            Some(headers.clone()),
        );

        assert_eq!(
            record.key.as_ref().expect("record should have key"),
            "user123"
        );
        assert_eq!(record.value, "test record");
        assert_eq!(
            record.headers.as_ref().expect("record should have headers"),
            &headers
        );
    }

    #[test]
    fn test_record_with_offset_creation() {
        let record = Record::new(None, "test content".to_string(), None);
        let record_with_offset = RecordWithOffset::from_record(record.clone(), 42);

        assert_eq!(record_with_offset.record.value, "test content");
        assert_eq!(record_with_offset.offset, 42);
        assert!(record_with_offset.timestamp.contains("T")); // ISO 8601 format
    }

    // =============================================================================
    // QUEUE TESTS
    // =============================================================================

    #[test]
    fn test_queue_creation() {
        let queue = FlashQ::new();
        // Queue should be created successfully - no panic means success
        drop(queue);
    }

    #[test]
    fn test_post_single_record() {
        let queue = FlashQ::new();
        let record = Record::new(None, "test record".to_string(), None);
        let result = queue.post_records("test_topic".to_string(), vec![record]);

        assert!(result.is_ok());
        assert_eq!(result.expect("posting record should succeed"), 0); // First record should have offset 0
    }

    #[test]
    fn test_post_multiple_records_increment_offset() {
        let queue = FlashQ::new();

        let record1 = Record::new(None, "msg1".to_string(), None);
        let record2 = Record::new(None, "msg2".to_string(), None);
        let record3 = Record::new(None, "msg3".to_string(), None);

        let offset1 = queue
            .post_records("topic".to_string(), vec![record1])
            .expect("posting first record should succeed");
        let offset2 = queue
            .post_records("topic".to_string(), vec![record2])
            .expect("posting second record should succeed");
        let offset3 = queue
            .post_records("different_topic".to_string(), vec![record3])
            .expect("posting record to different topic should succeed");

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 0); // Different topic starts from 0
    }

    #[test]
    fn test_poll_records_from_existing_topic() {
        let queue = FlashQ::new();

        let record1 = Record::new(None, "first news".to_string(), None);
        let record2 = Record::new(None, "second news".to_string(), None);

        queue
            .post_records("news".to_string(), vec![record1])
            .expect("posting first record should succeed");
        queue
            .post_records("news".to_string(), vec![record2])
            .expect("posting second record should succeed");

        let records = queue
            .poll_records("news", None)
            .expect("polling records from existing topic should succeed");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "first news");
        assert_eq!(records[1].record.value, "second news");
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
    }

    #[test]
    fn test_poll_records_with_count_limit() {
        let queue = FlashQ::new();

        let record1 = Record::new(None, "first news".to_string(), None);
        let record2 = Record::new(None, "second news".to_string(), None);

        queue
            .post_records("news".to_string(), vec![record1])
            .unwrap();
        queue
            .post_records("news".to_string(), vec![record2])
            .unwrap();

        let records = queue.poll_records("news", Some(1)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.value, "first news");
    }

    #[test]
    fn test_poll_nonexistent_topic() {
        let queue = FlashQ::new();
        let poll_result = queue.poll_records("news", None);
        poll_result.expect_err("Expected error for non-existent topic");
    }

    #[test]
    fn test_fifo_ordering() {
        let queue = FlashQ::new();

        let record1 = Record::new(None, "first".to_string(), None);
        let record2 = Record::new(None, "second".to_string(), None);
        let record3 = Record::new(None, "third".to_string(), None);

        queue
            .post_records("ordered".to_string(), vec![record1])
            .unwrap();
        queue
            .post_records("ordered".to_string(), vec![record2])
            .unwrap();
        queue
            .post_records("ordered".to_string(), vec![record3])
            .unwrap();

        let records = queue.poll_records("ordered", None).unwrap();
        assert_eq!(records[0].record.value, "first");
        assert_eq!(records[1].record.value, "second");
        assert_eq!(records[2].record.value, "third");

        // Verify timestamps are in ascending order (FIFO)
        assert!(records[0].timestamp <= records[1].timestamp);
        assert!(records[1].timestamp <= records[2].timestamp);
    }

    #[test]
    fn test_records_persist_after_polling() {
        let queue = FlashQ::new();
        let record = Record::new(None, "first news".to_string(), None);
        queue
            .post_records("news".to_string(), vec![record])
            .unwrap();

        let first_polling_records = queue.poll_records("news", None).unwrap();
        let second_polling_records = queue.poll_records("news", None).unwrap();

        assert_eq!(first_polling_records[0], second_polling_records[0]);
    }

    #[test]
    fn test_different_topics_isolated() {
        let queue = FlashQ::new();

        let record_a = Record::new(None, "record for A".to_string(), None);
        let record_b = Record::new(None, "record for B".to_string(), None);

        queue
            .post_records("topic_a".to_string(), vec![record_a])
            .unwrap();
        queue
            .post_records("topic_b".to_string(), vec![record_b])
            .unwrap();

        let records_in_topic_a = queue.poll_records("topic_a", None).unwrap();
        let records_in_topic_b = queue.poll_records("topic_b", None).unwrap();

        assert_eq!(records_in_topic_a.len(), 1);
        assert_eq!(records_in_topic_b.len(), 1);
        assert_eq!(records_in_topic_a[0].record.value, "record for A");
        assert_eq!(records_in_topic_b[0].record.value, "record for B");
    }

    #[test]
    fn test_queue_record_with_key_and_headers() {
        let queue = FlashQ::new();
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "high".to_string());
        headers.insert("source".to_string(), "test-suite".to_string());

        let record = Record::new(
            Some("user456".to_string()),
            "record with metadata".to_string(),
            Some(headers.clone()),
        );

        queue
            .post_records("metadata_topic".to_string(), vec![record])
            .expect("posting record with metadata should succeed");
        let records = queue
            .poll_records("metadata_topic", None)
            .expect("polling records should succeed");

        assert_eq!(records.len(), 1);
        let msg = &records[0];
        assert_eq!(
            msg.record.key.as_ref().expect("record should have key"),
            "user456"
        );
        assert_eq!(msg.record.value, "record with metadata");
        assert_eq!(
            msg.record
                .headers
                .as_ref()
                .expect("record should have headers"),
            &headers
        );
        assert_eq!(msg.offset, 0);
    }

    // =============================================================================
    // CONSUMER GROUP TESTS
    // =============================================================================

    // ConsumerGroup Tests
    #[test]
    fn test_consumer_group_creation() {
        let group = storage::InMemoryConsumerGroup::new("test-group".to_string());
        assert_eq!(group.group_id(), "test-group");
        assert_eq!(group.get_offset("any-topic"), 0);
    }

    #[test]
    fn test_consumer_group_set_and_get_offset() {
        let mut group = storage::InMemoryConsumerGroup::new("test-group".to_string());

        group.set_offset("topic1".to_string(), 5);
        group.set_offset("topic2".to_string(), 10);

        assert_eq!(group.get_offset("topic1"), 5);
        assert_eq!(group.get_offset("topic2"), 10);
        assert_eq!(group.get_offset("nonexistent"), 0);
    }

    #[test]
    fn test_consumer_group_update_offset() {
        let mut group = storage::InMemoryConsumerGroup::new("test-group".to_string());

        group.set_offset("topic".to_string(), 3);
        assert_eq!(group.get_offset("topic"), 3);

        group.set_offset("topic".to_string(), 8);
        assert_eq!(group.get_offset("topic"), 8);
    }

    #[test]
    fn test_consumer_group_multiple_topics() {
        let mut group = storage::InMemoryConsumerGroup::new("multi-topic-group".to_string());

        group.set_offset("news".to_string(), 15);
        group.set_offset("alerts".to_string(), 7);
        group.set_offset("logs".to_string(), 42);

        assert_eq!(group.get_offset("news"), 15);
        assert_eq!(group.get_offset("alerts"), 7);
        assert_eq!(group.get_offset("logs"), 42);
        assert_eq!(group.get_offset("unknown"), 0);
    }

    #[test]
    fn test_consumer_group_polling_records_offset_behavior() {
        let queue = FlashQ::new();
        let group_id = "test-group";
        let topic = "test-topic";

        // Create consumer group
        queue.create_consumer_group(group_id.to_string()).unwrap();

        // Post some records
        let record1 = Record::new(None, "msg1".to_string(), None);
        let record2 = Record::new(None, "msg2".to_string(), None);
        let record3 = Record::new(None, "msg3".to_string(), None);

        queue
            .post_records(topic.to_string(), vec![record1])
            .unwrap();
        queue
            .post_records(topic.to_string(), vec![record2])
            .unwrap();
        queue
            .post_records(topic.to_string(), vec![record3])
            .unwrap();

        // Initial offset should be 0
        assert_eq!(queue.get_consumer_group_offset(group_id, topic).unwrap(), 0);

        // Poll records - offset should remain 0 (polling doesn't advance offset)
        let records = queue
            .poll_records_for_consumer_group(group_id, topic, Some(2))
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "msg1");
        assert_eq!(records[1].record.value, "msg2");
        assert_eq!(queue.get_consumer_group_offset(group_id, topic).unwrap(), 0);

        // Poll again - should return same records since offset hasn't advanced
        let records = queue
            .poll_records_for_consumer_group(group_id, topic, Some(2))
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "msg1");
        assert_eq!(records[1].record.value, "msg2");
        assert_eq!(queue.get_consumer_group_offset(group_id, topic).unwrap(), 0);

        // Now commit offset to 2 (after consuming first 2 records)
        queue
            .update_consumer_group_offset(group_id, topic.to_string(), 2)
            .unwrap();
        assert_eq!(queue.get_consumer_group_offset(group_id, topic).unwrap(), 2);

        // Poll again - should now return the remaining record
        let records = queue
            .poll_records_for_consumer_group(group_id, topic, None)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.value, "msg3");
        assert_eq!(queue.get_consumer_group_offset(group_id, topic).unwrap(), 2); // Still 2 until explicitly committed
    }

    // =============================================================================
    // SERIALIZATION TESTS
    // =============================================================================

    // Serialization Tests
    #[test]
    fn test_record_record_serialization() {
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
    fn test_record_with_offset_serialization() {
        let record = Record::new(None, "test".to_string(), None);
        let record_with_offset = RecordWithOffset::from_record(record, 42);

        let json = serde_json::to_string(&record_with_offset).expect("Should serialize");
        let deserialized: RecordWithOffset =
            serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(record_with_offset.record, deserialized.record);
        assert_eq!(record_with_offset.offset, deserialized.offset);
        // Note: timestamp might differ slightly due to precision, so we just check it's present
        assert!(!deserialized.timestamp.is_empty());
    }

    // =============================================================================
    // ERROR CONDITION TESTS
    // =============================================================================

    // Error Condition Tests
    #[test]
    fn test_consumer_group_already_exists_error() {
        let queue = FlashQ::new();

        // Create a consumer group
        queue
            .create_consumer_group("existing-group".to_string())
            .unwrap();

        // Try to create the same group again
        let result = queue.create_consumer_group("existing-group".to_string());
        assert!(result.is_err());

        if let Err(FlashQError::ConsumerGroupAlreadyExists { group_id }) = result {
            assert_eq!(group_id, "existing-group");
        } else {
            panic!("Expected ConsumerGroupAlreadyExists error");
        }
    }

    #[test]
    fn test_consumer_group_not_found_error() {
        let queue = FlashQ::new();

        // Try to get offset from non-existent group
        let result = queue.get_consumer_group_offset("nonexistent-group", "topic");
        assert!(result.is_err());

        if let Err(FlashQError::ConsumerGroupNotFound { group_id }) = result {
            assert_eq!(group_id, "nonexistent-group");
        } else {
            panic!("Expected ConsumerGroupNotFound error");
        }
    }

    #[test]
    fn test_delete_consumer_group_success() {
        let queue = FlashQ::new();
        let group_id = "test-group";

        // Create a consumer group
        queue.create_consumer_group(group_id.to_string()).unwrap();

        // Create topic by posting a record
        let record = Record::new(None, "test record".to_string(), None);
        queue
            .post_records("topic1".to_string(), vec![record])
            .unwrap();

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

        if let Err(FlashQError::ConsumerGroupNotFound {
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
        let queue = FlashQ::new();
        let nonexistent_group = "nonexistent-group";

        // Try to delete a non-existent consumer group
        let result = queue.delete_consumer_group(nonexistent_group);
        assert!(result.is_err());

        if let Err(FlashQError::ConsumerGroupNotFound { group_id }) = result {
            assert_eq!(group_id, nonexistent_group);
        } else {
            panic!("Expected ConsumerGroupNotFound error");
        }
    }

    #[test]
    fn test_delete_consumer_group_multiple_topics() {
        let queue = FlashQ::new();
        let group_id = "multi-topic-group";

        // Create a consumer group
        queue.create_consumer_group(group_id.to_string()).unwrap();

        // Create topics by posting records
        let record1 = Record::new(None, "test record 1".to_string(), None);
        let record2 = Record::new(None, "test record 2".to_string(), None);
        let record3 = Record::new(None, "test record 3".to_string(), None);

        queue
            .post_records("topic1".to_string(), vec![record1])
            .unwrap();
        queue
            .post_records("topic2".to_string(), vec![record2])
            .unwrap();
        queue
            .post_records("topic3".to_string(), vec![record3])
            .unwrap();

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
        let queue = FlashQ::new();
        let record = Record::new(None, "test".to_string(), None);

        // Post a record to create topic with offset 0
        queue
            .post_records("test-topic".to_string(), vec![record])
            .unwrap();

        // Create consumer group and try to set invalid offset (beyond available records)
        queue
            .create_consumer_group("test-group".to_string())
            .unwrap();
        let result =
            queue.update_consumer_group_offset("test-group", "test-topic".to_string(), 999);

        assert!(result.is_err());
        if let Err(FlashQError::InvalidOffset {
            offset,
            topic,
            max_offset,
        }) = result
        {
            assert_eq!(offset, 999);
            assert_eq!(topic, "test-topic");
            assert_eq!(max_offset, 1); // Next offset after one record
        } else {
            panic!("Expected InvalidOffset error");
        }
    }

    #[test]
    fn test_record_queue_error_display() {
        let topic_error = FlashQError::TopicNotFound {
            topic: "missing".to_string(),
        };
        assert_eq!(topic_error.to_string(), "Topic 'missing' not found");

        let group_error = FlashQError::ConsumerGroupNotFound {
            group_id: "missing-group".to_string(),
        };
        assert_eq!(
            group_error.to_string(),
            "Consumer group 'missing-group' not found"
        );

        let exists_error = FlashQError::ConsumerGroupAlreadyExists {
            group_id: "existing-group".to_string(),
        };
        assert_eq!(
            exists_error.to_string(),
            "Consumer group 'existing-group' already exists"
        );

        let offset_error = FlashQError::InvalidOffset {
            offset: 10,
            topic: "test".to_string(),
            max_offset: 5,
        };
        assert_eq!(
            offset_error.to_string(),
            "Invalid offset 10 for topic 'test', max offset is 5"
        );
    }

    #[test]
    fn test_record_queue_error_is_not_found() {
        let topic_error = FlashQError::TopicNotFound {
            topic: "missing".to_string(),
        };
        assert!(topic_error.is_not_found());

        let group_error = FlashQError::ConsumerGroupNotFound {
            group_id: "missing".to_string(),
        };
        assert!(group_error.is_not_found());

        let offset_error = FlashQError::InvalidOffset {
            offset: 10,
            topic: "test".to_string(),
            max_offset: 5,
        };
        assert!(offset_error.is_not_found());

        let exists_error = FlashQError::ConsumerGroupAlreadyExists {
            group_id: "existing".to_string(),
        };
        assert!(!exists_error.is_not_found());
    }

    // File-based test moved to tests/storage_integration_tests.rs

    // File-based test moved to tests/storage_integration_tests.rs

    // File-based test moved to tests/storage_integration_tests.rs
}
// =============================================================================
// RECORD TESTS
// =============================================================================
