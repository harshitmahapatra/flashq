use super::test_utilities::*;
use flashq::Record;
use flashq_storage::file::{IndexingConfig, LogSegment, SyncMode};
use serde_json::json;
use test_log::test;

#[test]
fn test_segment_creation() {
    let config = TestConfig::new("segment_creation");
    let base_offset = 0;
    let log_path = config.temp_dir_path().join("0000.log");
    let index_path = config.temp_dir_path().join("0000.index");
    let time_index_path = config.temp_dir_path().join("0000.timeindex");

    let indexing_config = IndexingConfig::default();

    let segment = LogSegment::new(
        base_offset,
        log_path,
        index_path,
        time_index_path,
        SyncMode::Immediate,
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
    let base_path = config.temp_dir_path().join("0100");
    let log_path = base_path.with_extension("log");
    let index_path = base_path.with_extension("index");
    let time_index_path = base_path.with_extension("timeindex");

    let indexing_config = IndexingConfig::default();

    let mut segment = LogSegment::new(
        base_offset,
        log_path.clone(),
        index_path.clone(),
        time_index_path.clone(),
        SyncMode::Immediate,
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

    let recovered_segment =
        LogSegment::recover(base_offset, base_path, SyncMode::Immediate, indexing_config).unwrap();

    assert_eq!(recovered_segment.base_offset, base_offset);
    assert_eq!(recovered_segment.max_offset, Some(101));
    assert_eq!(recovered_segment.record_count(), 2);
}
