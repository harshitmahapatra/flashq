use flashq::storage::file::{FileTopicLog, SyncMode};
use flashq::storage::{StorageBackend, TopicLog};
use flashq::{FlashQ, Record};
use std::time::{SystemTime, UNIX_EPOCH};

fn generate_test_id() -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let thread_info = std::thread::current().name().unwrap_or("unknown").to_string();
    format!("{}_{}_{}", std::process::id(), timestamp, thread_info.replace("::", "_"))
}

#[test]
fn test_file_topic_log_basic_operations() {
    let test_id = generate_test_id();
    let temp_dir = std::env::temp_dir().join(format!("flashq_test_basic_{}", test_id));
    let topic_name = format!("test_topic_{}", test_id);

    // Use the configurable data directory
    let mut log = FileTopicLog::new(&topic_name, SyncMode::Immediate, &temp_dir).unwrap();

    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
    assert_eq!(log.next_offset(), 0);

    // Add some records
    let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let offset1 = log.append(record1);
    assert_eq!(offset1, 0);
    assert_eq!(log.len(), 1);
    assert_eq!(log.next_offset(), 1);

    let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);
    let offset2 = log.append(record2);
    assert_eq!(offset2, 1);
    assert_eq!(log.len(), 2);

    // Test retrieval
    let records = log.get_records_from_offset(0, None);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].record.value, "value1");
    assert_eq!(records[1].offset, 1);
    assert_eq!(records[1].record.value, "value2");

    // Test with count limit
    let records = log.get_records_from_offset(0, Some(1));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);

    // Test from specific offset
    let records = log.get_records_from_offset(1, None);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 1);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_file_recovery() {
    let test_id = generate_test_id();
    let temp_dir = std::env::temp_dir().join(format!("flashq_test_recovery_{}", test_id));
    let topic_name = format!("recovery_test_{}", test_id);

    // Create initial log and add records
    {
        let mut log = FileTopicLog::new(&topic_name, SyncMode::Immediate, &temp_dir).unwrap();
        log.append(Record::new(None, "first".to_string(), None));
        log.append(Record::new(None, "second".to_string(), None));
        // Explicitly sync to ensure data is written
        log.sync().unwrap();
    } // Log goes out of scope, file is closed

    // Create new log instance - should recover existing records
    {
        let log = FileTopicLog::new(&topic_name, SyncMode::Immediate, &temp_dir).unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log.next_offset(), 2);

        let records = log.get_records_from_offset(0, None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "first");
        assert_eq!(records[1].record.value, "second");
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_storage_backend_file() {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_backend_test_{}", test_id));
    let topic_name = format!("test_topic_{}", test_id);

    let backend = StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    };
    let mut storage = backend.create(&topic_name).unwrap();

    assert_eq!(storage.len(), 0);
    assert!(storage.is_empty());
    assert_eq!(storage.next_offset(), 0);

    let record = Record::new(None, "test".to_string(), None);
    let offset = storage.append(record);
    assert_eq!(offset, 0);
    assert_eq!(storage.len(), 1);
    assert_eq!(storage.next_offset(), 1);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_file_storage_persistence() {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_persistence_test_{}", test_id));
    let topic_name = format!("persistent_topic_{}", test_id);

    // Create queue and add records
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });
        let record1 = Record::new(None, "persistent_value1".to_string(), None);
        let record2 = Record::new(None, "persistent_value2".to_string(), None);
        
        queue.post_record(topic_name.clone(), record1).unwrap();
        queue.post_record(topic_name.clone(), record2).unwrap();
    } // Queue goes out of scope

    // Create new queue instance - should have persistent data
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });
        
        // Post a new record to trigger topic creation and recovery
        let record3 = Record::new(None, "new_value".to_string(), None);
        let offset = queue.post_record(topic_name.clone(), record3).unwrap();
        
        // Should start from offset 2 (after the recovered records)
        assert_eq!(offset, 2);
        
        let records = queue.poll_records(&topic_name, None).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record.value, "persistent_value1");
        assert_eq!(records[1].record.value, "persistent_value2");
        assert_eq!(records[2].record.value, "new_value");
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_file_storage_vs_memory_storage_compatibility() {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let topic_name = format!("compatibility_test_{}", test_id);
    let temp_dir = std::env::temp_dir().join(format!("flashq_compatibility_test_{}", test_id));

    // Test that both backends provide the same interface and behavior
    let memory_queue = FlashQ::with_storage_backend(StorageBackend::Memory);
    let record = Record::new(Some("test_key".to_string()), "test_value".to_string(), None);
    
    let memory_offset = memory_queue.post_record(topic_name.clone(), record.clone()).unwrap();
    let memory_records = memory_queue.poll_records(&topic_name, None).unwrap();

    // File storage should behave identically
    let file_queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    });
    let file_offset = file_queue.post_record(topic_name.clone(), record).unwrap();
    let file_records = file_queue.poll_records(&topic_name, None).unwrap();

    // Both should give same results
    assert_eq!(memory_offset, file_offset);
    assert_eq!(memory_records.len(), file_records.len());
    assert_eq!(memory_records[0].record.value, file_records[0].record.value);
    assert_eq!(memory_records[0].record.key, file_records[0].record.key);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_flashq_with_file_storage() {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_file_test_{}", test_id));
    let topic_name = format!("file_test_{}", test_id);

    let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    });
    
    let record = Record::new(Some("file_key".to_string()), "file_value".to_string(), None);
    let offset = queue.post_record(topic_name.clone(), record).unwrap();
    assert_eq!(offset, 0);
    
    let records = queue.poll_records(&topic_name, None).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "file_value");
    assert_eq!(records[0].record.key, Some("file_key".to_string()));
    assert_eq!(records[0].offset, 0);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}