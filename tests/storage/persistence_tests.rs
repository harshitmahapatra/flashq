use super::test_utilities::*;
use flashq::storage::file::FileTopicLog;
use flashq::storage::{StorageBackend, TopicLog};
use flashq::{FlashQ, Record};

#[test]
fn test_file_topic_log_recovery() {
    // Setup
    let config = TestConfig::new("log_recovery");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir.clone();

    // Action: Create log, add records, and drop it
    {
        let mut log = FileTopicLog::new(&topic_name, config.sync_mode, &temp_dir).unwrap();
        log.append(Record::new(None, "first".to_string(), None)).unwrap();
        log.append(Record::new(None, "second".to_string(), None)).unwrap();
        log.sync().unwrap();
    } // Log goes out of scope

    // Action: Create new log instance
    let recovered_log = FileTopicLog::new(&topic_name, config.sync_mode, &temp_dir).unwrap();

    // Expectation: Should recover existing records
    assert_eq!(recovered_log.len(), 2);
    assert_eq!(recovered_log.next_offset(), 2);

    let records = recovered_log.get_records_from_offset(0, None);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record.value, "first");
    assert_eq!(records[1].record.value, "second");
}

#[test]
fn test_flashq_persistence_across_instances() {
    // Setup
    let config = TestConfig::new("flashq_persistence");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir.clone();

    // Action: Create FlashQ, add records, and drop it
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: config.sync_mode,
            data_dir: temp_dir.clone(),
        });

        queue
            .post_record(
                topic_name.clone(),
                Record::new(None, "persistent1".to_string(), None),
            )
            .unwrap();
        queue
            .post_record(
                topic_name.clone(),
                Record::new(None, "persistent2".to_string(), None),
            )
            .unwrap();
    } // Queue goes out of scope

    // Action: Create new FlashQ instance
    let new_queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: temp_dir.clone(),
    });

    // Expectation: Should recover existing records
    let records = new_queue.poll_records(&topic_name, None).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record.value, "persistent1");
    assert_eq!(records[1].record.value, "persistent2");
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
}

#[test]
fn test_offset_continuation_after_recovery() {
    // Setup
    let config = TestConfig::new("offset_continuation");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir.clone();

    // Action: Create queue, add records, drop it
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: config.sync_mode,
            data_dir: temp_dir.clone(),
        });
        queue
            .post_record(
                topic_name.clone(),
                Record::new(None, "before_restart".to_string(), None),
            )
            .unwrap();
    }

    // Action: Create new instance and add more records
    let new_queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: temp_dir.clone(),
    });

    let new_offset = new_queue
        .post_record(
            topic_name.clone(),
            Record::new(None, "after_restart".to_string(), None),
        )
        .unwrap();

    // Expectation: Offset should continue from where it left off
    assert_eq!(new_offset, 1);

    let all_records = new_queue.poll_records(&topic_name, None).unwrap();
    assert_eq!(all_records.len(), 2);
    assert_eq!(all_records[0].record.value, "before_restart");
    assert_eq!(all_records[1].record.value, "after_restart");
}

#[test]
fn test_empty_data_directory_recovery() {
    // Setup
    let config = TestConfig::new("empty_recovery");

    // Action: Create FlashQ with empty data directory
    let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: config.temp_dir.clone(),
    });

    // Expectation: Should handle empty directory gracefully - polling non-existent topic should return error
    let poll_result = queue.poll_records(&config.topic_name, None);
    assert!(poll_result.is_err()); // Topic doesn't exist yet

    // Should be able to add new records
    let offset = queue
        .post_record(
            config.topic_name.clone(),
            Record::new(None, "first_record".to_string(), None),
        )
        .unwrap();
    assert_eq!(offset, 0);
}
