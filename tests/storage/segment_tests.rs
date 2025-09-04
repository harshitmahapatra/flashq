use super::test_utilities::*;
use flashq::Record;
use flashq::storage::file::{IndexingConfig, LogSegment, SyncMode};
use serde_json::json;

#[test]
fn test_segment_creation() {
    let config = TestConfig::new("segment_creation");
    let base_offset = 0;
    let log_path = config.temp_dir_path().join("0000.log");
    let index_path = config.temp_dir_path().join("0000.index");
    let indexing_config = IndexingConfig::default();

    let segment = LogSegment::new(
        base_offset,
        log_path,
        index_path,
        SyncMode::Immediate,
        Default::default(),
        indexing_config,
    )
    .unwrap();

    assert_eq!(segment.base_offset, base_offset);
    assert_eq!(segment.max_offset, None);
    assert_eq!(segment.record_count(), 0);
}

#[test]
fn test_append_and_recover() {
    let config = TestConfig::new("append_and_recover");
    let base_offset = 100;
    let log_path = config.temp_dir_path().join("0100.log");
    let index_path = config.temp_dir_path().join("0100.index");
    let indexing_config = IndexingConfig::default();

    let mut segment = LogSegment::new(
        base_offset,
        log_path.clone(),
        index_path.clone(),
        SyncMode::Immediate,
        Default::default(),
        indexing_config.clone(),
    )
    .unwrap();

    let record1 = Record {
        value: json!("record1").to_string(),
        key: None,
        headers: None,
    };
    let record2 = Record {
        value: json!("record2").to_string(),
        key: None,
        headers: None,
    };

    segment.append_record(&record1, 100).unwrap();
    segment.append_record(&record2, 101).unwrap();

    drop(segment);

    let recovered_segment = LogSegment::recover(
        base_offset,
        log_path,
        index_path,
        SyncMode::Immediate,
        Default::default(),
        indexing_config,
    )
    .unwrap();

    assert_eq!(recovered_segment.base_offset, base_offset);
    assert_eq!(recovered_segment.max_offset, Some(101));
    assert_eq!(recovered_segment.record_count(), 2);
}
