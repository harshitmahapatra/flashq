//! Partition Integration Tests
//! Tests the partition-aware storage functionality and backward compatibility

use crate::storage::test_utilities::TestConfig;
use flashq::Record;
use flashq::storage::file::FileTopicLog;
use flashq::storage::r#trait::{PartitionId, TopicLog};

#[test]
fn test_append_to_single_partition() {
    // Setup
    let config = TestConfig::new("single_partition");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(0);
    let record = Record::new(None, "test_value".to_string(), None);

    // Action
    let offset = log.append_partition(partition_id, record).unwrap();

    // Expectation
    assert_eq!(offset, 0);
    assert_eq!(log.partition_len(partition_id), 1);
    assert!(!log.partition_is_empty(partition_id));
}

#[test]
fn test_append_to_multiple_partitions() {
    // Setup
    let config = TestConfig::new("multiple_partitions");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);
    let record = Record::new(None, "test_value".to_string(), None);

    // Action
    let offset0 = log.append_partition(partition0, record.clone()).unwrap();
    let offset1 = log.append_partition(partition1, record).unwrap();

    // Expectation
    assert_eq!(offset0, 0);
    assert_eq!(offset1, 0);
    assert_eq!(log.partition_len(partition0), 1);
    assert_eq!(log.partition_len(partition1), 1);
}

#[test]
fn test_partitions_are_isolated() {
    // Setup
    let config = TestConfig::new("partitions_isolated");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);

    // Action
    log.append_partition(
        partition0,
        Record::new(None, "p0_record1".to_string(), None),
    )
    .unwrap();
    log.append_partition(
        partition0,
        Record::new(None, "p0_record2".to_string(), None),
    )
    .unwrap();
    log.append_partition(
        partition1,
        Record::new(None, "p1_record1".to_string(), None),
    )
    .unwrap();

    // Expectation
    assert_eq!(log.partition_len(partition0), 2);
    assert_eq!(log.partition_len(partition1), 1);
    assert_eq!(log.partition_next_offset(partition0), 2);
    assert_eq!(log.partition_next_offset(partition1), 1);
}

#[test]
fn test_read_from_specific_partition() {
    // Setup
    let config = TestConfig::new("read_specific");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(0);
    log.append_partition(partition_id, Record::new(None, "record1".to_string(), None))
        .unwrap();
    log.append_partition(partition_id, Record::new(None, "record2".to_string(), None))
        .unwrap();

    // Action
    let records = log.read_from_partition(partition_id, 0, None).unwrap();

    // Expectation
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
    assert_eq!(records[0].record.value, "record1");
    assert_eq!(records[1].record.value, "record2");
}

#[test]
fn test_read_from_nonexistent_partition() {
    // Setup
    let config = TestConfig::new("nonexistent");
    let log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(999);

    // Action
    let records = log.read_from_partition(partition_id, 0, None).unwrap();

    // Expectation
    assert!(records.is_empty());
}

#[test]
fn test_partition_offsets_are_independent() {
    // Setup
    let config = TestConfig::new("independent_offsets");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);

    // Action
    let offset0_1 = log
        .append_partition(partition0, Record::new(None, "p0_r1".to_string(), None))
        .unwrap();
    let offset1_1 = log
        .append_partition(partition1, Record::new(None, "p1_r1".to_string(), None))
        .unwrap();
    let offset0_2 = log
        .append_partition(partition0, Record::new(None, "p0_r2".to_string(), None))
        .unwrap();
    let offset1_2 = log
        .append_partition(partition1, Record::new(None, "p1_r2".to_string(), None))
        .unwrap();

    // Expectation
    assert_eq!(offset0_1, 0);
    assert_eq!(offset1_1, 0);
    assert_eq!(offset0_2, 1);
    assert_eq!(offset1_2, 1);
}

#[test]
fn test_batch_append_to_partition() {
    // Setup
    let config = TestConfig::new("batch_append");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(0);
    let records = vec![
        Record::new(None, "record1".to_string(), None),
        Record::new(None, "record2".to_string(), None),
        Record::new(None, "record3".to_string(), None),
    ];

    // Action
    let last_offset = log.append_batch_partition(partition_id, records).unwrap();

    // Expectation
    assert_eq!(last_offset, 2);
    assert_eq!(log.partition_len(partition_id), 3);
    assert_eq!(log.partition_next_offset(partition_id), 3);
}

#[test]
fn test_batch_append_to_multiple_partitions() {
    // Setup
    let config = TestConfig::new("batch_multiple");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);

    // Action
    let last_offset0 = log
        .append_batch_partition(
            partition0,
            vec![
                Record::new(None, "p0_r1".to_string(), None),
                Record::new(None, "p0_r2".to_string(), None),
            ],
        )
        .unwrap();
    let last_offset1 = log
        .append_batch_partition(
            partition1,
            vec![Record::new(None, "p1_r1".to_string(), None)],
        )
        .unwrap();

    // Expectation
    assert_eq!(last_offset0, 1);
    assert_eq!(last_offset1, 0);
    assert_eq!(log.partition_len(partition0), 2);
    assert_eq!(log.partition_len(partition1), 1);
}

#[test]
fn test_empty_batch_append() {
    // Setup
    let config = TestConfig::new("empty_batch");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(0);

    // Action
    let last_offset = log.append_batch_partition(partition_id, vec![]).unwrap();

    // Expectation
    assert_eq!(last_offset, 0);
    assert_eq!(log.partition_len(partition_id), 0);
    assert!(log.partition_is_empty(partition_id));
}

#[test]
fn test_partition_directory_structure() {
    // Setup
    let config = TestConfig::new("directory_structure");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();
    let partition_id = PartitionId(5);

    // Action
    log.append_partition(partition_id, Record::new(None, "test".to_string(), None))
        .unwrap();
    log.sync().unwrap();

    // Expectation
    let partition_dir = config.temp_dir_path().join(&config.topic_name).join("5");
    assert!(partition_dir.exists());
    assert!(partition_dir.is_dir());
}

#[test]
fn test_partition_recovery_after_restart() {
    // Setup
    let config = TestConfig::new("recovery");
    let partition_id = PartitionId(0);

    // First instance: write data
    {
        let mut log = FileTopicLog::new(
            &config.topic_name,
            config.sync_mode,
            config.temp_dir_path(),
            config.segment_size,
        )
        .unwrap();
        log.append_partition(
            partition_id,
            Record::new(None, "persisted_record".to_string(), None),
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

    // Expectation
    assert_eq!(log.partition_len(partition_id), 1);
    assert_eq!(log.partition_next_offset(partition_id), 1);
    let records = log.read_from_partition(partition_id, 0, None).unwrap();
    assert_eq!(records[0].record.value, "persisted_record");
}
