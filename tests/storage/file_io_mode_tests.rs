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

    let mut topic_log = backend.create("test_topic").unwrap();

    let record = flashq::Record::new(Some("key1".to_string()), "test_value".to_string(), None);
    let offset = topic_log.append(record).unwrap();

    assert_eq!(offset, 0);
    assert_eq!(topic_log.len(), 1);

    let records = topic_log.get_records_from_offset(0, Some(1)).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "test_value");
}

#[cfg(target_os = "linux")]
#[test]
fn test_storage_backend_with_io_uring() {
    use flashq::storage::file::IoUringFileIO;

    if !IoUringFileIO::is_available() {
        println!("Skipping io_uring storage test - not available");
        return;
    }

    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        FileIOMode::IoUring,
        temp_dir.path(),
    )
    .unwrap();

    let mut topic_log = backend.create("io_uring_topic").unwrap();

    let record = flashq::Record::new(
        Some("io_uring_key".to_string()),
        "io_uring_value".to_string(),
        None,
    );
    let offset = topic_log.append(record).unwrap();

    assert_eq!(offset, 0);
    assert_eq!(topic_log.len(), 1);

    let records = topic_log.get_records_from_offset(0, Some(1)).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "io_uring_value");
}

#[test]
fn test_file_io_mode_default_selection() {
    let default_mode = FileIOMode::default();
    assert_eq!(default_mode, FileIOMode::Standard);
}

#[test]
fn test_file_io_mode_display() {
    assert_eq!(format!("{}", FileIOMode::Standard), "standard");

    #[cfg(target_os = "linux")]
    {
        assert_eq!(format!("{}", FileIOMode::IoUring), "io_uring");
    }
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

    let mut consumer_group = backend.create_consumer_group("test_group").unwrap();

    // Test initial state
    assert_eq!(consumer_group.get_offset("test_topic"), 0);

    // Test setting offset
    consumer_group.set_offset("test_topic".to_string(), 42);
    assert_eq!(consumer_group.get_offset("test_topic"), 42);

    // Test group ID
    assert_eq!(consumer_group.group_id(), "test_group");
}

#[cfg(target_os = "linux")]
#[test]
fn test_consumer_group_with_io_uring() {
    use flashq::storage::file::IoUringFileIO;

    if !IoUringFileIO::is_available() {
        println!("Skipping io_uring consumer group test - not available");
        return;
    }

    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        FileIOMode::IoUring,
        temp_dir.path(),
    )
    .unwrap();

    let mut consumer_group = backend.create_consumer_group("io_uring_group").unwrap();

    // Test initial state
    assert_eq!(consumer_group.get_offset("io_uring_topic"), 0);

    // Test setting offset
    consumer_group.set_offset("io_uring_topic".to_string(), 99);
    assert_eq!(consumer_group.get_offset("io_uring_topic"), 99);

    // Test group ID
    assert_eq!(consumer_group.group_id(), "io_uring_group");
}

/// Integration test comparing behavior between I/O modes
#[test]
fn test_io_mode_behavior_compatibility() {
    let temp_dir = tempdir().unwrap();

    // Test with Standard I/O
    let std_backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        FileIOMode::Standard,
        temp_dir.path().join("std"),
    )
    .unwrap();

    let mut std_topic_log = std_backend.create("compat_topic").unwrap();
    let record1 = flashq::Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let record2 = flashq::Record::new(Some("key2".to_string()), "value2".to_string(), None);

    let offset1_std = std_topic_log.append(record1.clone()).unwrap();
    let offset2_std = std_topic_log.append(record2.clone()).unwrap();

    #[cfg(target_os = "linux")]
    {
        use flashq::storage::file::IoUringFileIO;

        if IoUringFileIO::is_available() {
            // Test with io_uring I/O
            let io_uring_backend = StorageBackend::new_file_with_path(
                flashq::storage::file::SyncMode::Immediate,
                FileIOMode::IoUring,
                temp_dir.path().join("io_uring"),
            )
            .unwrap();

            let mut io_uring_topic_log = io_uring_backend.create("compat_topic").unwrap();
            let offset1_io_uring = io_uring_topic_log.append(record1.clone()).unwrap();
            let offset2_io_uring = io_uring_topic_log.append(record2.clone()).unwrap();

            // Both implementations should behave identically
            assert_eq!(offset1_std, offset1_io_uring);
            assert_eq!(offset2_std, offset2_io_uring);
            assert_eq!(std_topic_log.len(), io_uring_topic_log.len());
            assert_eq!(
                std_topic_log.next_offset(),
                io_uring_topic_log.next_offset()
            );

            // Records should be identical
            let std_records = std_topic_log.get_records_from_offset(0, None).unwrap();
            let io_uring_records = io_uring_topic_log.get_records_from_offset(0, None).unwrap();

            assert_eq!(std_records.len(), io_uring_records.len());
            for (std_rec, io_uring_rec) in std_records.iter().zip(io_uring_records.iter()) {
                assert_eq!(std_rec.offset, io_uring_rec.offset);
                assert_eq!(std_rec.record.key, io_uring_rec.record.key);
                assert_eq!(std_rec.record.value, io_uring_rec.record.value);
            }
        }
    }
}
