use flashq::storage::StorageBackend;
use flashq::storage::file::FileIo;
use tempfile::tempdir;
use test_log::test;

/// Test basic file operations with StdFileIO
#[test]
fn test_file_io_append_and_read() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("fileio_append_read_test.log");

    let mut file_handle = FileIo::create_with_append_and_read_permissions(&test_file_path).unwrap();

    let test_data = b"Hello, Standard I/O!";
    let start_position = FileIo::append_data_to_end(&mut file_handle, test_data).unwrap();
    FileIo::synchronize_to_disk(&mut file_handle).unwrap();

    let mut read_buffer = vec![0u8; test_data.len()];
    FileIo::read_data_at_offset(&mut file_handle, &mut read_buffer, 0).unwrap();

    assert_eq!(start_position, 0, "First write should start at position 0");
    assert_eq!(
        FileIo::get_file_size(&file_handle).unwrap(),
        test_data.len() as u64
    );
    assert_eq!(&read_buffer, test_data);
}

#[test]
fn test_file_io_write_truncate_mode() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("fileio_truncate_test.json");

    let mut file_handle = FileIo::create_with_write_truncate_permissions(&test_file_path).unwrap();

    let json_data = r#"{"test": "standard_io_data"}"#;
    FileIo::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0).unwrap();
    FileIo::synchronize_to_disk(&mut file_handle).unwrap();

    let file_size = FileIo::get_file_size(&file_handle).unwrap();
    assert_eq!(file_size, json_data.len() as u64);
}

#[test]
fn test_file_io_sequential_writes() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("fileio_sequential_test.log");

    let mut file_handle = FileIo::create_with_append_and_read_permissions(&test_file_path).unwrap();

    let data1 = b"First line\n";
    let data2 = b"Second line\n";

    let pos1 = FileIo::append_data_to_end(&mut file_handle, data1).unwrap();
    let pos2 = FileIo::append_data_to_end(&mut file_handle, data2).unwrap();

    assert_eq!(pos1, 0);
    assert_eq!(pos2, data1.len() as u64);

    let total_size = FileIo::get_file_size(&file_handle).unwrap();
    assert_eq!(total_size, (data1.len() + data2.len()) as u64);
}

#[test]
fn test_storage_backend_with_file_io() {
    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
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
fn test_consumer_group_with_file_io() {
    let temp_dir = tempdir().unwrap();
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
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
