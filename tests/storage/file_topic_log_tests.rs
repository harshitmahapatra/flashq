use super::test_utilities::*;
use flashq::Record;
use flashq::storage::TopicLog;
use flashq::storage::file::FileTopicLog;

#[test]
fn test_empty_log_properties() {
    let config = TestConfig::new("empty_log");
    let log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir, 10, 1000).unwrap();

    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
    assert_eq!(log.next_offset(), 0);
}

#[test]
fn test_append_single_record() {
    let config = TestConfig::new("append_single");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    let offset = log.append(Record::new(Some("key1".to_string()), "value1".to_string(), None)).unwrap();

    assert_eq!(offset, 0);
    assert_eq!(log.len(), 1);
    assert_eq!(log.next_offset(), 1);
}

#[test]
fn test_append_multiple_records() {
    let config = TestConfig::new("append_multiple");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    let offset1 = log.append(Record::new(Some("key1".to_string()), "value1".to_string(), None)).unwrap();
    let offset2 = log.append(Record::new(Some("key2".to_string()), "value2".to_string(), None)).unwrap();

    assert_eq!(offset1, 0);
    assert_eq!(offset2, 1);
    assert_eq!(log.len(), 2);
    assert_eq!(log.next_offset(), 2);
}

#[test]
fn test_offset_consistency() {
    let config = TestConfig::new("offset_consistency");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    for i in 0..5 {
        let offset = log.append(Record::new(None, format!("value{i}"), None)).unwrap();
        assert_eq!(offset, i);
    }

    let records = log.get_records_from_offset(0, None).unwrap();
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
    }
}

#[test]
fn test_retrieve_all_records() {
    let config = TestConfig::new("retrieve_all");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    log.append(Record::new(None, "value1".to_string(), None)).unwrap();
    log.append(Record::new(None, "value2".to_string(), None)).unwrap();
    log.append(Record::new(None, "value3".to_string(), None)).unwrap();

    let records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].record.value, "value1");
    assert_eq!(records[2].record.value, "value3");
}

#[test]
fn test_retrieve_from_offset() {
    let config = TestConfig::new("retrieve_offset");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    log.append(Record::new(None, "value1".to_string(), None)).unwrap();
    log.append(Record::new(None, "value2".to_string(), None)).unwrap();
    log.append(Record::new(None, "value3".to_string(), None)).unwrap();

    let records = log.get_records_from_offset(1, None).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record.value, "value2");
}

#[test]
fn test_retrieve_with_count_limit() {
    let config = TestConfig::new("count_limit");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    log.append(Record::new(None, "value1".to_string(), None)).unwrap();
    log.append(Record::new(None, "value2".to_string(), None)).unwrap();
    log.append(Record::new(None, "value3".to_string(), None)).unwrap();

    let records = log.get_records_from_offset(0, Some(2)).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record.value, "value1");
    assert_eq!(records[1].record.value, "value2");
}

#[test]
fn test_wal_file_creation() {
    let config = TestConfig::new("wal_creation");
    let _log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    let wal_path = config.temp_dir_path().join(format!("{}.wal", config.topic_name));
    assert!(wal_path.exists());
}

#[test]
fn test_wal_contains_data() {
    let config = TestConfig::new("wal_data");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    log.append(Record::new(Some("key1".to_string()), "value1".to_string(), None)).unwrap();

    let wal_path = config.temp_dir_path().join(format!("{}.wal", config.topic_name));
    let wal_content = std::fs::read(&wal_path).unwrap();
    assert!(!wal_content.is_empty());
}

#[test]
fn test_wal_recovery() {
    let config = TestConfig::new("wal_recovery");
    let wal_path = config.temp_dir_path().join(format!("{}.wal", config.topic_name));

    {
        let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();
        log.append(Record::new(Some("recovery_key".to_string()), "recovery_value".to_string(), None)).unwrap();
        log.sync().unwrap();
    }

    let recovered_log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();
    assert_eq!(recovered_log.len(), 1);
    
    let wal_content = std::fs::read(&wal_path).unwrap();
    assert!(wal_content.is_empty());
}

#[test]
fn test_wal_commit_threshold() {
    let config = TestConfig::new("wal_threshold");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 2, 1000).unwrap();
    let wal_path = config.temp_dir_path().join(format!("{}.wal", config.topic_name));

    log.append(Record::new(None, "value1".to_string(), None)).unwrap();
    assert!(!std::fs::read(&wal_path).unwrap().is_empty());

    log.append(Record::new(None, "value2".to_string(), None)).unwrap();
    assert!(std::fs::read(&wal_path).unwrap().is_empty());
}

#[test]
fn test_wal_recovery_uncommitted() {
    let config = TestConfig::new("wal_uncommitted");

    {
        let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();
        log.append(Record::new(None, "uncommitted_value".to_string(), None)).unwrap();
    }

    let recovered_log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();
    assert_eq!(recovered_log.len(), 1);
}

#[test]
fn test_cache_stores_appended_record() {
    let config = TestConfig::new("cache_append");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 1000).unwrap();

    let offset = log.append(Record::new(Some("key1".to_string()), "value1".to_string(), None)).unwrap();

    assert_eq!(offset, 0);
    let retrieved = log.get_records_from_offset(0, Some(1)).unwrap();
    assert_eq!(retrieved[0].record.value, "value1");
}

#[test]
fn test_cache_eviction_fallback() {
    let config = TestConfig::new("cache_eviction");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 5).unwrap();

    for i in 0..15 {
        log.append(Record::new(Some(format!("key{}", i)), format!("value{}", i), None)).unwrap();
    }

    let all_records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all_records.len(), 15);
    assert_eq!(all_records[0].record.value, "value0");
    assert_eq!(all_records[14].record.value, "value14");
}

#[test]
fn test_configurable_cache_size() {
    let config = TestConfig::new("custom_cache");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, config.temp_dir_path(), 10, 5).unwrap();

    for i in 0..10 {
        log.append(Record::new(Some(format!("key{}", i)), format!("value{}", i), None)).unwrap();
    }

    let all_records = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all_records.len(), 10);
    assert_eq!(all_records[0].record.value, "value0");
    assert_eq!(all_records[9].record.value, "value9");
}