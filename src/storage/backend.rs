use crate::storage::{InMemoryTopicLog, TopicLog};

/// Storage backend configuration
///
/// Specifies which storage implementation to use for topic logs.
/// Currently supports in-memory storage and file-based storage with
/// future extensibility for database storage backends.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// In-memory storage - records persist only during process lifetime
    Memory,
    /// File-based storage with configurable sync mode - records persist to disk
    File(crate::storage::file::SyncMode),
    /// File-based storage with custom data directory and sync mode
    FileWithPath {
        sync_mode: crate::storage::file::SyncMode,
        data_dir: std::path::PathBuf,
    },
}

impl StorageBackend {
    /// Create a new storage backend instance
    ///
    /// Returns a boxed trait object implementing TopicLog, allowing different
    /// storage implementations to be used interchangeably. The concrete type
    /// is determined by the StorageBackend variant:
    ///
    /// - `Memory`: Creates an InMemoryTopicLog that stores records in a Vec
    /// - `File(sync_mode)`: Creates a FileTopicLog with default data directory (./data)
    /// - `FileWithPath`: Creates a FileTopicLog with custom data directory
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name for file-based storage (ignored for memory)
    ///
    /// # Returns
    ///
    /// A boxed TopicLog trait object ready for use. The returned instance
    /// starts empty with offset 0 (or recovers existing records for file storage)
    /// and can immediately accept record operations.
    ///
    /// # Example
    ///
    /// ```
    /// use flashq::storage::{StorageBackend, TopicLog, file::SyncMode};
    /// use flashq::Record;
    ///
    /// let backend = StorageBackend::Memory;
    /// let mut storage = backend.create("test_topic").unwrap();
    ///
    /// let record = Record::new(None, "test".to_string(), None);
    /// let offset = storage.append(record);
    /// assert_eq!(offset, 0);
    /// ```
    pub fn create(&self, topic: &str) -> Result<Box<dyn TopicLog>, Box<dyn std::error::Error>> {
        match self {
            StorageBackend::Memory => Ok(Box::new(InMemoryTopicLog::new())),
            StorageBackend::File(sync_mode) => {
                let file_log = crate::storage::file::FileTopicLog::new_default(topic, *sync_mode)?;
                Ok(Box::new(file_log))
            }
            StorageBackend::FileWithPath { sync_mode, data_dir } => {
                let file_log = crate::storage::file::FileTopicLog::new(topic, *sync_mode, data_dir)?;
                Ok(Box::new(file_log))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use std::collections::HashMap;

    #[test]
    fn test_storage_backend_memory() {
        let backend = StorageBackend::Memory;
        let mut storage = backend.create("test_topic").unwrap();

        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
        assert_eq!(storage.next_offset(), 0);

        let record = Record::new(None, "test".to_string(), None);
        let offset = storage.append(record);
        assert_eq!(offset, 0);
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.next_offset(), 1);
    }

    #[test]
    fn test_storage_backend_enum_equality() {
        let backend1 = StorageBackend::Memory;
        let backend2 = StorageBackend::Memory;
        assert_eq!(backend1, backend2);

        use crate::storage::file::SyncMode;
        let backend3 = StorageBackend::File(SyncMode::Immediate);
        let backend4 = StorageBackend::File(SyncMode::Immediate);
        assert_eq!(backend3, backend4);

        assert_ne!(backend1, backend3);
    }

    #[test]
    fn test_trait_object_usage() {
        let mut storage: Box<dyn TopicLog> = Box::new(InMemoryTopicLog::new());

        let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
        let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);

        let offset1 = storage.append(record1);
        let offset2 = storage.append(record2);

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(storage.len(), 2);

        let records = storage.get_records_from_offset(0, None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "value1");
        assert_eq!(records[1].record.value, "value2");
    }

    #[test]
    fn test_storage_abstraction_with_headers() {
        let mut storage = InMemoryTopicLog::new();
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());
        headers.insert("priority".to_string(), "high".to_string());

        let record = Record::new(
            Some("user123".to_string()),
            "record with headers".to_string(),
            Some(headers.clone()),
        );

        let offset = storage.append(record);
        assert_eq!(offset, 0);

        let records = storage.get_records_from_offset(0, None);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]
                .record
                .key
                .as_ref()
                .expect("record should have key"),
            "user123"
        );
        assert_eq!(records[0].record.value, "record with headers");
        assert_eq!(
            records[0]
                .record
                .headers
                .as_ref()
                .expect("record should have headers"),
            &headers
        );
    }
}
