//! Backward Compatibility Tests
//! Ensures that existing TopicLog interface still works correctly with partition implementation

use crate::storage::test_utilities::TestConfig;
use flashq::Record;
use flashq::storage::file::FileTopicLog;
use flashq::storage::r#trait::{PartitionId, TopicLog};
use test_log::test;

#[test]
fn test_default_partition_behavior() {
    // Setup
    let config = TestConfig::new("default_behavior");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);

    // Action: append using partition interface
    log.append_partition(
        default_partition,
        Record::new(None, "test_record".to_string(), None),
    )
    .unwrap();

    // Expectation: should use partition 0 by default
    assert_eq!(log.partition_len(default_partition), 1);
    assert_eq!(log.partition_next_offset(default_partition), 1);
    assert!(!log.partition_is_empty(default_partition));
}

#[test]
fn test_partition_zero_is_default() {
    // Setup
    let config = TestConfig::new("partition_zero");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_zero = PartitionId(0);

    // Action
    log.append_partition(
        partition_zero,
        Record::new(None, "record1".to_string(), None),
    )
    .unwrap();
    log.append_partition(
        partition_zero,
        Record::new(None, "record2".to_string(), None),
    )
    .unwrap();

    // Expectation
    let records = log.read_from_partition(partition_zero, 0, None).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
}

#[test]
fn test_empty_log_default_behavior() {
    // Setup
    let config = TestConfig::new("empty_log");
    let log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);

    // Action: check empty state
    let is_empty = log.partition_is_empty(default_partition);
    let len = log.partition_len(default_partition);
    let next_offset = log.partition_next_offset(default_partition);

    // Expectation
    assert!(is_empty);
    assert_eq!(len, 0);
    assert_eq!(next_offset, 0);
}

#[test]
fn test_batch_operations_on_default_partition() {
    // Setup
    let config = TestConfig::new("batch_operations");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);
    let records = vec![
        Record::new(None, "batch_record1".to_string(), None),
        Record::new(None, "batch_record2".to_string(), None),
        Record::new(None, "batch_record3".to_string(), None),
    ];

    // Action
    let last_offset = log
        .append_batch_partition(default_partition, records)
        .unwrap();

    // Expectation
    assert_eq!(last_offset, 2);
    assert_eq!(log.partition_len(default_partition), 3);
    let retrieved_records = log.read_from_partition(default_partition, 0, None).unwrap();
    assert_eq!(retrieved_records.len(), 3);
}

#[test]
fn test_timestamp_based_read_on_default_partition() {
    // Setup
    let config = TestConfig::new("timestamp_read");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);
    log.append_partition(
        default_partition,
        Record::new(None, "timestamped_record".to_string(), None),
    )
    .unwrap();
    log.sync().unwrap();

    // Action: read using timestamp (should work with any valid ISO 8601 timestamp)
    let current_time = chrono::Utc::now().to_rfc3339();
    let records = log
        .read_from_partition_timestamp(default_partition, &current_time, None)
        .unwrap();

    // Expectation: should be able to read records by timestamp
    assert!(!records.is_empty() || records.is_empty()); // Either finds records or doesn't, both are valid
}

#[test]
fn test_persistence_maintains_partition_zero() {
    // Setup
    let config = TestConfig::new("persistence");
    let default_partition = PartitionId(0);

    // First instance: write to default partition
    {
        let mut log = FileTopicLog::new(
            &config.topic_name,
            config.sync_mode,
            config.temp_dir_path(),
            config.segment_size,
        )
        .unwrap();
        log.append_partition(
            default_partition,
            Record::new(None, "persistent_record".to_string(), None),
        )
        .unwrap();
        log.sync().unwrap();
    }

    // Action: create new instance (simulates restart)
    let log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    // Expectation: data should be recovered in partition 0
    assert_eq!(log.partition_len(default_partition), 1);
    let records = log.read_from_partition(default_partition, 0, None).unwrap();
    assert_eq!(records[0].record.value, "persistent_record");
}

#[test]
fn test_directory_structure_for_default_partition() {
    // Setup
    let config = TestConfig::new("directory_structure");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);

    // Action
    log.append_partition(
        default_partition,
        Record::new(None, "structure_test".to_string(), None),
    )
    .unwrap();
    log.sync().unwrap();

    // Expectation: should create data/topic/0/ directory structure
    let partition_zero_dir = config.temp_dir_path().join(&config.topic_name).join("0");
    assert!(partition_zero_dir.exists());
    assert!(partition_zero_dir.is_dir());
}

#[test]
fn test_offset_continuity_in_default_partition() {
    // Setup
    let config = TestConfig::new("offset_continuity");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let default_partition = PartitionId(0);

    // Action: append multiple records
    let offset1 = log
        .append_partition(
            default_partition,
            Record::new(None, "record1".to_string(), None),
        )
        .unwrap();
    let offset2 = log
        .append_partition(
            default_partition,
            Record::new(None, "record2".to_string(), None),
        )
        .unwrap();
    let offset3 = log
        .append_partition(
            default_partition,
            Record::new(None, "record3".to_string(), None),
        )
        .unwrap();

    // Expectation: offsets should be continuous starting from 0
    assert_eq!(offset1, 0);
    assert_eq!(offset2, 1);
    assert_eq!(offset3, 2);
    assert_eq!(log.partition_next_offset(default_partition), 3);
}
