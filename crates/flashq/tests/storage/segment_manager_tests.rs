use super::test_utilities::*;
use flashq::Record;
use flashq::storage::file::{IndexingConfig, SegmentManager, SyncMode};

#[test]
fn test_segment_manager_creation() {
    let config = TestConfig::new("sm_creation");
    let segment_size_bytes = 1024; // 1KB for testing
    let indexing_config = IndexingConfig::default();

    let mut sm = SegmentManager::new(
        config.temp_dir_path().to_path_buf(),
        segment_size_bytes,
        SyncMode::Immediate,
        indexing_config,
    );
    sm.roll_to_new_segment(0).unwrap();

    assert!(sm.active_segment_mut().is_some());
    assert_eq!(sm.all_segments().count(), 1);
}

#[test]
fn test_segment_rolling() {
    let config = TestConfig::new("sm_rolling");
    let segment_size_bytes = 256; // small size for testing
    let indexing_config = IndexingConfig::default();
    let mut sm = SegmentManager::new(
        config.temp_dir_path().to_path_buf(),
        segment_size_bytes,
        SyncMode::Immediate,
        indexing_config,
    );
    sm.roll_to_new_segment(0).unwrap();

    let record = Record {
        value: "a".repeat(100).to_string(),
        key: None,
        headers: None,
    };

    {
        let active_segment = sm.active_segment_mut().unwrap();
        active_segment.append_record(&record, 0).unwrap();
    }

    assert!(!sm.should_roll_segment());

    sm.active_segment_mut()
        .unwrap()
        .append_record(&record, 1)
        .unwrap();

    assert!(sm.should_roll_segment());

    sm.roll_to_new_segment(2).unwrap();

    assert_eq!(sm.all_segments().count(), 2);
    let active_segment = sm.active_segment_mut().unwrap();
    assert_eq!(active_segment.base_offset, 2);
}
