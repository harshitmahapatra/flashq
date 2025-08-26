use flashq::Record;
use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_storage_backend_file() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_backend_test_{test_id}"));
    let topic_name = format!("test_topic_{test_id}");

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
fn test_file_storage_vs_memory_storage_compatibility() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let topic_name = format!("compatibility_test_{test_id}");
    let temp_dir = std::env::temp_dir().join(format!("flashq_compatibility_test_{test_id}"));

    // Test that both backends provide the same interface and behavior
    let memory_backend = StorageBackend::Memory;
    let mut memory_storage = memory_backend.create(&topic_name).unwrap();

    let file_backend = StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    };
    let mut file_storage = file_backend.create(&topic_name).unwrap();

    let record = Record::new(Some("test_key".to_string()), "test_value".to_string(), None);

    let memory_offset = memory_storage.append(record.clone());
    let file_offset = file_storage.append(record);

    let memory_records = memory_storage.get_records_from_offset(0, None);
    let file_records = file_storage.get_records_from_offset(0, None);

    // Both should give same results
    assert_eq!(memory_offset, file_offset);
    assert_eq!(memory_records.len(), file_records.len());
    assert_eq!(memory_records[0].record.value, file_records[0].record.value);
    assert_eq!(memory_records[0].record.key, file_records[0].record.key);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}
