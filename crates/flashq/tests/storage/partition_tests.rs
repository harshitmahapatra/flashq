//! Partition Integration Tests
//! Tests the partition-aware storage functionality and backward compatibility

use crate::storage::test_utilities::TestConfig;
use flashq::Record;
use flashq::storage::file::{FileConsumerGroup, FileTopicLog, SyncMode};
use flashq::storage::memory::InMemoryConsumerGroup;
use flashq::storage::r#trait::{ConsumerGroup, PartitionId, TopicLog};
use test_log::test;

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

#[test]
fn test_consumer_group_partition_aware_offset_tracking() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("test_group".to_string());
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);
    let topic = "test_topic";

    // Action
    memory_group.set_offset_partition(topic.to_string(), partition0, 5);
    memory_group.set_offset_partition(topic.to_string(), partition1, 10);

    // Expectation
    assert_eq!(memory_group.get_offset_partition(topic, partition0), 5);
    assert_eq!(memory_group.get_offset_partition(topic, partition1), 10);
    assert_eq!(
        memory_group.get_offset_partition("other_topic", partition0),
        0
    );
}

#[test]
fn test_consumer_group_partition_isolation() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("isolation_group".to_string());
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);
    let partition2 = PartitionId(2);
    let topic = "isolation_topic";

    // Action
    memory_group.set_offset_partition(topic.to_string(), partition0, 100);
    memory_group.set_offset_partition(topic.to_string(), partition1, 200);
    memory_group.set_offset_partition(topic.to_string(), partition2, 300);

    // Expectation: each partition maintains independent offsets
    assert_eq!(memory_group.get_offset_partition(topic, partition0), 100);
    assert_eq!(memory_group.get_offset_partition(topic, partition1), 200);
    assert_eq!(memory_group.get_offset_partition(topic, partition2), 300);

    // Verify all partition offsets are stored
    let all_offsets = memory_group.get_all_offsets_partitioned();
    assert_eq!(all_offsets.len(), 3);
    assert_eq!(all_offsets[&(topic.to_string(), partition0)], 100);
    assert_eq!(all_offsets[&(topic.to_string(), partition1)], 200);
    assert_eq!(all_offsets[&(topic.to_string(), partition2)], 300);
}

#[test]
fn test_consumer_group_multi_topic_partition_tracking() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("multi_topic_group".to_string());
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);
    let topic1 = "topic1";
    let topic2 = "topic2";

    // Action
    memory_group.set_offset_partition(topic1.to_string(), partition0, 50);
    memory_group.set_offset_partition(topic1.to_string(), partition1, 75);
    memory_group.set_offset_partition(topic2.to_string(), partition0, 25);
    memory_group.set_offset_partition(topic2.to_string(), partition1, 30);

    // Expectation: topics and partitions are independently tracked
    assert_eq!(memory_group.get_offset_partition(topic1, partition0), 50);
    assert_eq!(memory_group.get_offset_partition(topic1, partition1), 75);
    assert_eq!(memory_group.get_offset_partition(topic2, partition0), 25);
    assert_eq!(memory_group.get_offset_partition(topic2, partition1), 30);

    // Verify all combinations are stored
    let all_offsets = memory_group.get_all_offsets_partitioned();
    assert_eq!(all_offsets.len(), 4);
}

#[test]
fn test_file_consumer_group_partition_persistence() {
    // Setup
    let config = TestConfig::new("file_consumer_partition");
    let group_id = "persistent_partition_group";
    let topic = "persistent_topic";
    let partition0 = PartitionId(0);
    let partition1 = PartitionId(1);

    // First instance: set partition offsets
    {
        let mut file_group =
            FileConsumerGroup::new(group_id, SyncMode::Immediate, config.temp_dir_path()).unwrap();

        file_group.set_offset_partition(topic.to_string(), partition0, 42);
        file_group.set_offset_partition(topic.to_string(), partition1, 84);
    }

    // Action: create new instance (simulates restart)
    let file_group =
        FileConsumerGroup::new(group_id, SyncMode::Immediate, config.temp_dir_path()).unwrap();

    // Expectation: partition offsets should be recovered
    assert_eq!(file_group.get_offset_partition(topic, partition0), 42);
    assert_eq!(file_group.get_offset_partition(topic, partition1), 84);

    let all_offsets = file_group.get_all_offsets_partitioned();
    assert_eq!(all_offsets.len(), 2);
}

#[test]
fn test_consumer_group_partition_zero_independence() {
    // Setup
    let mut memory_group = InMemoryConsumerGroup::new("zero_independence".to_string());
    let partition0 = PartitionId(0);
    let partition5 = PartitionId(5);
    let topic = "independence_topic";

    // Action: set different offsets for partition 0 and other partitions
    memory_group.set_offset_partition(topic.to_string(), partition0, 10);
    memory_group.set_offset_partition(topic.to_string(), partition5, 20);

    // Expectation: partition 0 doesn't interfere with other partitions
    assert_eq!(memory_group.get_offset_partition(topic, partition0), 10);
    assert_eq!(memory_group.get_offset_partition(topic, partition5), 20);

    // Verify independence - updating one doesn't affect the other
    memory_group.set_offset_partition(topic.to_string(), partition0, 15);
    assert_eq!(memory_group.get_offset_partition(topic, partition0), 15);
    assert_eq!(memory_group.get_offset_partition(topic, partition5), 20);
}
