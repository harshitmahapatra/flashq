use flashq::Record;
use flashq::storage::TopicLog;
use flashq::storage::file::{FileTopicLog, SyncMode};
use std::time::{SystemTime, UNIX_EPOCH};

fn generate_test_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_info = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string();
    format!(
        "{}_{}_{}",
        std::process::id(),
        timestamp,
        thread_info.replace("::", "_")
    )
}

#[test]
fn test_file_topic_log_basic_operations() {
    let test_id = generate_test_id();
    let temp_dir = std::env::temp_dir().join(format!("flashq_test_basic_{test_id}"));
    let topic_name = format!("test_topic_{test_id}");

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
    let temp_dir = std::env::temp_dir().join(format!("flashq_test_recovery_{test_id}"));
    let topic_name = format!("recovery_test_{test_id}");

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
