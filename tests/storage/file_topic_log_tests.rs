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
        10,
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
    let log =
        FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir, 10).unwrap();

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
        10,
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
        10,
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
        10,
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
fn test_wal_file_creation() {
    let config = TestConfig::new("wal_creation");
    let _log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        10,
    )
    .unwrap();

    let wal_path = config
        .temp_dir_path()
        .join(format!("{}.wal", config.topic_name));
    assert!(wal_path.exists());
}

#[test]
fn test_wal_basic_functionality() {
    let config = TestConfig::new("wal_basic");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        10,
    )
    .unwrap();

    let record = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    log.append(record).unwrap();

    let wal_path = config
        .temp_dir_path()
        .join(format!("{}.wal", config.topic_name));
    let wal_content = std::fs::read(&wal_path).unwrap();
    assert!(!wal_content.is_empty());
}

#[test]
fn test_wal_recovery() {
    let config = TestConfig::new("wal_recovery");

    let wal_path = config
        .temp_dir_path()
        .join(format!("{}.wal", config.topic_name));

    {
        let mut log = FileTopicLog::new(
            &config.topic_name,
            config.sync_mode,
            config.temp_dir_path(),
            10,
        )
        .unwrap();
        let test_record = Record::new(
            Some("recovery_key".to_string()),
            "recovery_value".to_string(),
            None,
        );
        log.append(test_record).unwrap();
        log.sync().unwrap();

        let wal_content = std::fs::read(&wal_path).unwrap();
        assert!(
            !wal_content.is_empty(),
            "WAL should contain data before recovery"
        );
    }

    let wal_content_before = std::fs::read(&wal_path).unwrap();
    assert!(
        !wal_content_before.is_empty(),
        "WAL should still have content after drop"
    );

    let recovered_log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        10,
    )
    .unwrap();

    assert_eq!(recovered_log.len(), 1);
    let wal_content_after = std::fs::read(&wal_path).unwrap();
    assert!(
        wal_content_after.is_empty(),
        "WAL should be empty after recovery"
    );
}

#[test]
fn test_wal_commit_threshold_behavior() {
    let config = TestConfig::new("wal_threshold");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        2,
    )
    .unwrap();

    let wal_path = config
        .temp_dir_path()
        .join(format!("{}.wal", config.topic_name));

    log.append(Record::new(None, "value1".to_string(), None))
        .unwrap();
    let wal_content = std::fs::read(&wal_path).unwrap();
    assert!(
        !wal_content.is_empty(),
        "WAL should contain uncommitted record"
    );

    log.append(Record::new(None, "value2".to_string(), None))
        .unwrap();
    let wal_content_after = std::fs::read(&wal_path).unwrap();
    assert!(
        wal_content_after.is_empty(),
        "WAL should be empty after reaching threshold"
    );
}

#[test]
fn test_wal_recovery_with_uncommitted_records() {
    let config = TestConfig::new("wal_uncommitted");
    let wal_path = config
        .temp_dir_path()
        .join(format!("{}.wal", config.topic_name));

    {
        let mut log = FileTopicLog::new(
            &config.topic_name,
            config.sync_mode,
            config.temp_dir_path(),
            10,
        )
        .unwrap();
        log.append(Record::new(None, "uncommitted_value".to_string(), None))
            .unwrap();

        let wal_content = std::fs::read(&wal_path).unwrap();
        assert!(
            !wal_content.is_empty(),
            "WAL should have uncommitted record"
        );
    }

    let recovered_log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        10,
    )
    .unwrap();
    assert_eq!(recovered_log.len(), 1);
}
