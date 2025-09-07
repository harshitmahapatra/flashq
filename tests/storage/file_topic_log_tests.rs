use super::test_utilities::*;
use flashq::Record;
use flashq::storage::TopicLog;
use flashq::storage::file::FileTopicLog;

#[test]
fn test_basic_append_and_retrieval() {
    let config = TestConfig::new("basic_ops");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);

    let offset1 = log.append(record1).unwrap();
    let offset2 = log.append(record2).unwrap();

    assert_eq!(offset1, 0);
    assert_eq!(offset2, 1);
    assert_eq!(log.len(), 2);
    assert_eq!(log.next_offset(), 2);
}

#[test]
fn test_empty_log_properties() {
    let config = TestConfig::new("empty_log");
    let log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        &config.temp_dir,
        config.segment_size,
    )
    .unwrap();

    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
    assert_eq!(log.next_offset(), 0);
}

#[test]
fn test_record_retrieval_from_offset() {
    let config = TestConfig::new("retrieval");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(
        Some("key1".to_string()),
        "value1".to_string(),
        None,
    ))
    .unwrap();
    log.append(Record::new(
        Some("key2".to_string()),
        "value2".to_string(),
        None,
    ))
    .unwrap();
    log.append(Record::new(
        Some("key3".to_string()),
        "value3".to_string(),
        None,
    ))
    .unwrap();

    let all_records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all_records.len(), 3);
    assert_eq!(all_records[0].record.value, "value1");
    assert_eq!(all_records[2].record.value, "value3");

    let partial_records = log.get_records_from_offset(1, None).unwrap();
    assert_eq!(partial_records.len(), 2);
    assert_eq!(partial_records[0].record.value, "value2");
}

#[test]
fn test_record_retrieval_with_count_limit() {
    let config = TestConfig::new("count_limit");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(None, "value1".to_string(), None))
        .unwrap();
    log.append(Record::new(None, "value2".to_string(), None))
        .unwrap();
    log.append(Record::new(None, "value3".to_string(), None))
        .unwrap();

    let limited_records = log.get_records_from_offset(0, Some(2)).unwrap();
    assert_eq!(limited_records.len(), 2);
    assert_eq!(limited_records[0].record.value, "value1");
    assert_eq!(limited_records[1].record.value, "value2");
}

#[test]
fn test_offset_consistency() {
    let config = TestConfig::new("offset_consistency");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    for i in 0..5 {
        let offset = log
            .append(Record::new(None, format!("value{i}"), None))
            .unwrap();

        assert_eq!(offset, i);
        assert_eq!(log.next_offset(), i + 1);
    }

    let records = log.get_records_from_offset(0, None).unwrap();
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
    }
}

#[test]
fn test_segment_rolling() {
    // Setup
    let config = TestConfig::new("segment_rolling");
    let small_segment_size = 10 * 1024; // 10KB - forces rolling after ~10 records
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        small_segment_size,
    )
    .unwrap();

    // Action - Write records across multiple segments
    for i in 0..50 {
        let payload = "x".repeat(1024); // 1KB payload
        let record = Record::new(Some(format!("key_{i}")), payload, None);
        let offset = log.append(record).unwrap();
        assert_eq!(offset, i);
    }

    // Expectation - All records should be readable across segment boundaries
    let all_records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all_records.len(), 50);
    assert_eq!(log.len(), 50);
    assert_eq!(log.next_offset(), 50);

    // Expectation - Reading from middle offset with limit should work across segments
    let middle_records = log.get_records_from_offset(25, Some(20)).unwrap();
    assert_eq!(middle_records.len(), 20);
    assert_eq!(middle_records[0].offset, 25);
    assert_eq!(middle_records[19].offset, 44);
}

#[test]
fn test_segment_boundary_crossing() {
    // Setup - Use very small segments to force frequent rolling
    let config = TestConfig::new("segment_boundary_crossing");
    let small_segment_size = 5 * 1024; // 5KB - forces rolling after ~5 records
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        small_segment_size,
    )
    .unwrap();

    // Action - Write records that will span exactly across segment boundaries
    for i in 0..20 {
        let payload = "x".repeat(1024); // 1KB payload
        let record = Record::new(Some(format!("key_{i}")), payload, None);
        let offset = log.append(record).unwrap();
        assert_eq!(offset, i);
    }

    // Expectations - All records should be readable despite crossing boundaries
    assert_eq!(log.len(), 20);
    assert_eq!(log.next_offset(), 20);

    let all_records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all_records.len(), 20);

    // Verify records are in correct order
    for (i, record) in all_records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
        assert_eq!(record.record.key, Some(format!("key_{i}")));
    }
}

#[test]
fn test_flashq_large_file_benchmark_scenario() {
    use flashq::FlashQ;
    use flashq::storage::{StorageBackend, file::SyncMode};
    use std::collections::HashMap;

    let _ = env_logger::try_init();

    // Setup: Create FlashQ with file storage and helper function
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let storage_backend = StorageBackend::new_file_with_path(SyncMode::None, temp_dir.path())
        .expect("Failed to create file storage backend");
    let queue = FlashQ::with_storage_backend(storage_backend);
    let topic = "benchmark".to_string();

    let create_1kb_record = |index: usize| {
        let payload = "x".repeat(1024);
        let mut headers = HashMap::new();
        headers.insert("index".to_string(), index.to_string());
        flashq::Record::new(Some(format!("key_{index}")), payload, Some(headers))
    };

    // Action: Write 1000 records (~1MB total, simulates benchmark scenario)
    for i in 0..1000 {
        queue
            .post_records(topic.clone(), vec![create_1kb_record(i)])
            .unwrap();
    }

    // Expectation: Should be able to read records from various offsets
    // This tests the sparse index functionality under single-segment conditions
    let all_records = queue.poll_records(&topic, None).unwrap();
    assert_eq!(all_records.len(), 1000, "Should retrieve all 1000 records");

    let mid_records = queue
        .poll_records_from_offset(&topic, 500, Some(100))
        .unwrap();
    assert_eq!(
        mid_records.len(),
        100,
        "Should retrieve 100 records from middle"
    );
    assert_eq!(
        mid_records[0].offset, 500,
        "First record should be at offset 500"
    );

    let end_records = queue
        .poll_records_from_offset(&topic, 900, Some(100))
        .unwrap();
    assert_eq!(
        end_records.len(),
        100,
        "Should retrieve 100 records from end"
    );
    assert_eq!(
        end_records[0].offset, 900,
        "First record should be at offset 900"
    );
}
