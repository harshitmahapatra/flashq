use dashmap::DashMap;
use dashmap::mapref::entry::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use storage::{ConsumerGroup, TopicLog};

pub mod demo;
pub mod error;
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

        let mut topic_log_locked = topic_log.value().write().unwrap();
        let last = topic_log_locked
            .append_batch(records)
            .map_err(FlashQError::from)?;
        Ok(last)
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

    pub fn poll_records_from_time(
        &self,
        topic: &str,
        from_time: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        match self.topics.get(topic) {
            Some(topic_log) => topic_log
                .value()
                .read()
                .unwrap()
                .get_records_from_timestamp(from_time, count)
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
                    .map_err(|e| FlashQError::ConsumerGroupCreationFailed {
                        group_id: group_id.clone(),
                        reason: e.to_string(),
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

    pub fn poll_records_for_consumer_group_from_offset(
        &self,
        group_id: &str,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        let current_offset = self.get_consumer_group_offset(group_id, topic)?;
        let records = self.poll_records_from_offset(topic, offset, count)?;

        // Record that this consumer group has accessed this topic (with offset 0 if first time)
        // This ensures the topic appears in the consumer group's JSON file
        if current_offset == 0 {
            self.update_consumer_group_offset(group_id, topic.to_string(), 0)?;
        }

        Ok(records)
    }

    pub fn poll_records_for_consumer_group_from_time(
        &self,
        group_id: &str,
        topic: &str,
        from_time: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, FlashQError> {
        // Ensure consumer group exists and is tracked for this topic
        let _ = self.get_consumer_group_offset(group_id, topic)?;
        self.poll_records_from_time(topic, from_time, count)
    }

    pub fn get_high_water_mark(&self, topic: &str) -> u64 {
        match self.topics.get(topic) {
            Some(topic_log) => topic_log.value().read().unwrap().next_offset(),
            None => 0,
        }
    }

    pub fn get_topics(&self) -> Vec<String> {
        self.topics
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
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
            storage::StorageBackend::Memory { .. } => return Ok(()), // No recovery needed for memory
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
