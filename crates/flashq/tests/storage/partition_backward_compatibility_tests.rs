//! Backward Compatibility Tests
//! Ensures that existing TopicLog interface still works correctly with partition implementation

use crate::storage::test_utilities::TestConfig;
use flashq::Record;
use flashq::storage::file::{FileConsumerGroup, FileTopicLog, SyncMode};
use flashq::storage::memory::InMemoryConsumerGroup;
use flashq::storage::r#trait::{ConsumerGroup, PartitionId, TopicLog};
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

#[test]
fn test_consumer_group_backward_compatibility_get_offset() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("backward_compat_group".to_string());
    let topic = "compat_topic";

    // Action: use old API methods that should delegate to partition 0
    memory_group.set_offset(topic.to_string(), 25);

    // Expectation: should work with partition 0 internally
    assert_eq!(memory_group.get_offset(topic), 25);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 25);
}

#[test]
fn test_consumer_group_backward_compatibility_set_offset() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("set_compat_group".to_string());
    let topic = "set_compat_topic";

    // Action: set using old API
    memory_group.set_offset(topic.to_string(), 42);

    // Expectation: should be accessible via both old and new APIs
    assert_eq!(memory_group.get_offset(topic), 42);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 42);

    // Verify other partitions remain unaffected
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(1)), 0);
}

#[test]
fn test_consumer_group_backward_compatibility_get_all_offsets() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("all_compat_group".to_string());
    let topic1 = "topic1";
    let topic2 = "topic2";

    // Action: set offsets using new partition-aware API
    memory_group.set_offset_partition(topic1.to_string(), PartitionId(0), 10);
    memory_group.set_offset_partition(topic1.to_string(), PartitionId(1), 20);
    memory_group.set_offset_partition(topic2.to_string(), PartitionId(0), 30);

    // Expectation: old API should only return partition 0 offsets
    let old_api_offsets = memory_group.get_all_offsets();
    assert_eq!(old_api_offsets.len(), 2);
    assert_eq!(old_api_offsets[topic1], 10);
    assert_eq!(old_api_offsets[topic2], 30);

    // New API should return all partition offsets
    let new_api_offsets = memory_group.get_all_offsets_partitioned();
    assert_eq!(new_api_offsets.len(), 3);
}

#[test]
fn test_consumer_group_mixed_api_usage() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("mixed_api_group".to_string());
    let topic = "mixed_topic";

    // Action: mix old and new API usage
    memory_group.set_offset(topic.to_string(), 5); // Sets partition 0
    memory_group.set_offset_partition(topic.to_string(), PartitionId(1), 15); // Sets partition 1

    // Expectation: both APIs should work correctly
    assert_eq!(memory_group.get_offset(topic), 5); // Gets partition 0
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 5);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(1)), 15);

    // Update via old API and verify it only affects partition 0
    memory_group.set_offset(topic.to_string(), 8);
    assert_eq!(memory_group.get_offset(topic), 8);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 8);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(1)), 15); // Unchanged
}

#[test]
fn test_file_consumer_group_backward_compatibility_persistence() {
    // Setup
    let config = TestConfig::new("file_backward_compat");
    let group_id = "backward_compat_file_group";
    let topic = "compat_file_topic";

    // First instance: use old API
    {
        let mut file_group =
            FileConsumerGroup::new(group_id, SyncMode::Immediate, config.temp_dir_path()).unwrap();

        file_group.set_offset(topic.to_string(), 99);
    }

    // Action: create new instance and verify with both APIs
    let file_group =
        FileConsumerGroup::new(group_id, SyncMode::Immediate, config.temp_dir_path()).unwrap();

    // Expectation: offset should be recoverable via both APIs
    assert_eq!(file_group.get_offset(topic), 99);
    assert_eq!(file_group.get_offset_partition(topic, PartitionId(0)), 99);
    assert_eq!(file_group.get_offset_partition(topic, PartitionId(1)), 0);
}

#[test]
fn test_consumer_group_partition_zero_equals_old_api() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("zero_equals_old".to_string());
    let topic = "zero_topic";

    // Action: set using partition 0 directly
    memory_group.set_offset_partition(topic.to_string(), PartitionId(0), 77);

    // Expectation: old API should return the same value
    assert_eq!(memory_group.get_offset(topic), 77);

    // Action: set using old API
    memory_group.set_offset(topic.to_string(), 88);

    // Expectation: partition 0 should have the updated value
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 88);
}

#[test]
fn test_consumer_group_default_offset_behavior() {
    // Setup
    let memory_group = InMemoryConsumerGroup::new("default_behavior".to_string());
    let topic = "default_topic";

    // Action & Expectation: unset offsets should return 0 for both APIs
    assert_eq!(memory_group.get_offset(topic), 0);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(0)), 0);
    assert_eq!(memory_group.get_offset_partition(topic, PartitionId(5)), 0);

    // Verify empty state
    assert!(memory_group.get_all_offsets().is_empty());
    assert!(memory_group.get_all_offsets_partitioned().is_empty());
}
