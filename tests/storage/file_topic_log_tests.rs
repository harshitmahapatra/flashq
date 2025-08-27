use super::test_utilities::*;
use flashq::storage::file::FileTopicLog;
use flashq::storage::TopicLog;
use flashq::Record;

#[test]
fn test_basic_append_and_retrieval() {
    // Setup
    let config = TestConfig::new("basic_ops");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir).unwrap();

    // Action: Append records
    let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);
    
    let offset1 = log.append(record1);
    let offset2 = log.append(record2);

    // Expectation
    assert_eq!(offset1, 0);
    assert_eq!(offset2, 1);
    assert_eq!(log.len(), 2);
    assert_eq!(log.next_offset(), 2);
}

#[test]
fn test_empty_log_properties() {
    // Setup
    let config = TestConfig::new("empty_log");
    let log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir).unwrap();

    // Expectation
    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
    assert_eq!(log.next_offset(), 0);
}

#[test]
fn test_record_retrieval_from_offset() {
    // Setup
    let config = TestConfig::new("retrieval");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir).unwrap();
    
    log.append(Record::new(Some("key1".to_string()), "value1".to_string(), None));
    log.append(Record::new(Some("key2".to_string()), "value2".to_string(), None));
    log.append(Record::new(Some("key3".to_string()), "value3".to_string(), None));

    // Action & Expectation: Get all records from start
    let all_records = log.get_records_from_offset(0, None);
    assert_eq!(all_records.len(), 3);
    assert_eq!(all_records[0].record.value, "value1");
    assert_eq!(all_records[2].record.value, "value3");

    // Action & Expectation: Get records from specific offset
    let partial_records = log.get_records_from_offset(1, None);
    assert_eq!(partial_records.len(), 2);
    assert_eq!(partial_records[0].record.value, "value2");
}

#[test]
fn test_record_retrieval_with_count_limit() {
    // Setup
    let config = TestConfig::new("count_limit");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir).unwrap();
    
    log.append(Record::new(None, "value1".to_string(), None));
    log.append(Record::new(None, "value2".to_string(), None));
    log.append(Record::new(None, "value3".to_string(), None));

    // Action & Expectation: Get limited number of records
    let limited_records = log.get_records_from_offset(0, Some(2));
    assert_eq!(limited_records.len(), 2);
    assert_eq!(limited_records[0].record.value, "value1");
    assert_eq!(limited_records[1].record.value, "value2");
}

#[test]
fn test_offset_consistency() {
    // Setup
    let config = TestConfig::new("offset_consistency");
    let mut log = FileTopicLog::new(&config.topic_name, config.sync_mode, &config.temp_dir).unwrap();

    // Action: Add records and verify offsets
    for i in 0..5 {
        let offset = log.append(Record::new(None, format!("value{}", i), None));
        
        // Expectation: Sequential offsets
        assert_eq!(offset, i);
        assert_eq!(log.next_offset(), i + 1);
    }

    // Final expectation
    let records = log.get_records_from_offset(0, None);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
    }
}