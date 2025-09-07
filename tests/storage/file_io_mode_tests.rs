use flashq::storage::StorageBackend;
use flashq::storage::file::FileIOMode;
use tempfile::tempdir;

#[test]
fn test_storage_backend_with_standard_io() {
    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        FileIOMode::Standard,
        temp_dir.path(),
    )
    .unwrap();

    let topic_log = backend.create("test_topic").unwrap();

    let record = flashq::Record::new(Some("key1".to_string()), "test_value".to_string(), None);
    let offset = topic_log.write().unwrap().append(record).unwrap();

    assert_eq!(offset, 0);
    assert_eq!(topic_log.read().unwrap().len(), 1);

    let records = topic_log
        .read()
        .unwrap()
        .get_records_from_offset(0, Some(1))
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "test_value");
}

#[test]
fn test_file_io_mode_default_selection() {
    let default_mode = FileIOMode::default();
    assert_eq!(default_mode, FileIOMode::Standard);
}

#[test]
fn test_consumer_group_with_standard_io() {
    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        FileIOMode::Standard,
        temp_dir.path(),
    )
    .unwrap();

    let consumer_group = backend.create_consumer_group("test_group").unwrap();

    // Test initial state
    assert_eq!(consumer_group.read().unwrap().get_offset("test_topic"), 0);

    // Test setting offset
    consumer_group
        .write()
        .unwrap()
        .set_offset("test_topic".to_string(), 42);
    assert_eq!(consumer_group.read().unwrap().get_offset("test_topic"), 42);

    // Test group ID
    assert_eq!(consumer_group.read().unwrap().group_id(), "test_group");
}
