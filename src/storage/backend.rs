use crate::storage::{InMemoryTopicLog, TopicLog};

/// Storage backend configuration
///
/// Specifies which storage implementation to use for topic logs.
/// Currently only supports in-memory storage, with future extensibility
/// for file-based and database storage backends.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// In-memory storage - records persist only during process lifetime
    Memory,
}

impl StorageBackend {
    /// Create a new storage backend instance
    pub fn create(&self) -> Box<dyn TopicLog> {
        match self {
            StorageBackend::Memory => Box::new(InMemoryTopicLog::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use std::collections::HashMap;

    #[test]
    fn test_storage_backend_memory_creation() {
        let backend = StorageBackend::Memory;
        let storage = backend.create();
        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
        assert_eq!(storage.next_offset(), 0);
    }

    #[test]
    fn test_storage_backend_enum_equality() {
        let backend1 = StorageBackend::Memory;
        let backend2 = StorageBackend::Memory;
        assert_eq!(backend1, backend2);
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
        assert_eq!(records[0].record.key.as_ref().unwrap(), "user123");
        assert_eq!(records[0].record.value, "record with headers");
        assert_eq!(records[0].record.headers.as_ref().unwrap(), &headers);
    }
}
