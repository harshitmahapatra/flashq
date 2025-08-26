use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{FlashQ, Record};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_file_storage_persistence() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_persistence_test_{test_id}"));
    let topic_name = format!("persistent_topic_{test_id}");

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
fn test_flashq_with_file_storage() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_file_test_{test_id}"));
    let topic_name = format!("file_test_{test_id}");

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
