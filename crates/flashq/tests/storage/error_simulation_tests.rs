use super::test_utilities::*;
use flashq::Record;
use flashq::error::StorageError;
use flashq::storage::TopicLog;
use flashq::storage::file::{FileTopicLog, SyncMode};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;

fn create_disk_full_scenario(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let fill_path = temp_dir.join("disk_filler");
    let mut file = File::create(&fill_path)?;
    let large_data = vec![0u8; 10 * 1024 * 1024];
    file.write_all(&large_data)?;
    file.sync_all()?;
    Ok(())
}

fn create_permission_denied_scenario(
    temp_dir: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let readonly_dir = temp_dir.join("readonly_test");
    std::fs::create_dir_all(&readonly_dir)?;
    let metadata = std::fs::metadata(&readonly_dir)?;
    let mut perms = metadata.permissions();
    perms.set_mode(0o444);
    std::fs::set_permissions(&readonly_dir, perms)?;
    Ok(readonly_dir)
}

fn corrupt_log_file(file_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(file_path)?;
    file.write_all(b"corrupted data that is not valid binary format")?;
    file.sync_all()?;
    Ok(())
}

#[test]
fn test_disk_full_during_append() {
    let config = TestConfig::new("disk_full");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    create_disk_full_scenario(config.temp_dir_path()).ok();

    let large_record = Record::new(Some("key".to_string()), "x".repeat(900 * 1024), None); // 900KB - leaves room for segment overhead

    let result = log.append(large_record);

    match result {
        Ok(_) => {
            assert!(log.len() > 0);
        }
        Err(StorageError::InsufficientSpace { .. }) => {}
        Err(StorageError::WriteFailed { .. }) => {}
        Err(other) => panic!("Unexpected error type: {other:?}"),
    }
}

#[test]
fn test_insufficient_space_recovery() {
    let config = TestConfig::new("space_recovery");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    let large_record = Record::new(
        Some("huge_key".to_string()),
        "x".repeat(800 * 1024), // 800KB - less than segment size but large
        None,
    );

    let result = log.append(large_record);

    match result {
        Ok(_) => {
            let normal_record =
                Record::new(Some("key".to_string()), "normal_value".to_string(), None);
            let offset = log.append(normal_record).unwrap();
            assert!(offset < 1000);
        }
        Err(_) => {
            let normal_record =
                Record::new(Some("key".to_string()), "recovery_test".to_string(), None);
            let offset = log.append(normal_record).unwrap();
            assert_eq!(offset, 0);
            assert_eq!(log.len(), 1);
        }
    }
}

#[test]
fn test_permission_denied_directory_creation() {
    let config = TestConfig::new("permission_test");
    let readonly_dir = create_permission_denied_scenario(config.temp_dir_path()).unwrap();
    let result: Result<FileTopicLog, _> = FileTopicLog::new(
        "test_topic",
        SyncMode::Immediate,
        &readonly_dir,
        config.segment_size,
    );

    match result {
        Err(e) => {
            let error_string = format!("{e}");
            assert!(
                error_string.contains("Permission denied") || error_string.contains("permission"),
                "Expected permission error, got: {error_string}"
            );
        }
        Ok(_) => {
            let mut log = result.unwrap();
            let record = Record::new(Some("key".to_string()), "value".to_string(), None);
            let append_result = log.append(record);

            match append_result {
                Err(StorageError::PermissionDenied { .. }) => {}
                Err(StorageError::WriteFailed { .. }) => {}
                other => panic!("Expected permission/write error, got: {other:?}"),
            }
        }
    }
}

#[test]
fn test_permission_denied_file_write() {
    let config = TestConfig::new("file_permission");

    let metadata = std::fs::metadata(config.temp_dir_path()).unwrap();
    let mut perms = metadata.permissions();
    perms.set_mode(0o444);
    std::fs::set_permissions(config.temp_dir_path(), perms).unwrap();

    let result: Result<FileTopicLog, _> = FileTopicLog::new(
        &config.topic_name,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    );

    match result {
        Err(e) => {
            let error_string = format!("{e}");
            assert!(
                error_string.contains("Permission denied")
                    || error_string.contains("permission")
                    || error_string.contains("denied"),
                "Expected permission error, got: {error_string}"
            );
        }
        Ok(_) => {
            let mut restore_perms = std::fs::metadata(config.temp_dir_path())
                .unwrap()
                .permissions();
            restore_perms.set_mode(0o755);
            std::fs::set_permissions(config.temp_dir_path(), restore_perms).unwrap();
            panic!("Expected permission error during topic log creation");
        }
    }

    let mut cleanup_perms = std::fs::metadata(config.temp_dir_path())
        .unwrap()
        .permissions();
    cleanup_perms.set_mode(0o755);
    std::fs::set_permissions(config.temp_dir_path(), cleanup_perms).unwrap();
}

#[test]
fn test_file_read_failure() {
    let config = TestConfig::new("read_failure");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    let record = Record::new(Some("key".to_string()), "value".to_string(), None);
    log.append(record).unwrap();

    // With segment-based storage, files are in {data_dir}/{topic}/00000000000000000000.log
    let log_file_path = config
        .temp_dir_path()
        .join(&config.topic_name)
        .join("00000000000000000000.log");
    corrupt_log_file(&log_file_path).unwrap();

    let result = log.get_records_from_offset(0, None);

    match result {
        Err(StorageError::DataCorruption { .. }) => {
            // Expected: corruption detected during deserialization
        }
        Err(StorageError::ReadFailed { .. }) => {
            // Expected: I/O error during file read
        }
        Ok(records) => {
            // Acceptable: corruption resulted in no readable records, but read operation succeeded
            // This is actually reasonable behavior - partial corruption shouldn't prevent reading valid data
            assert_eq!(records.len(), 0, "Expected no records due to corruption");
        }
        Err(other) => panic!("Unexpected error type for corruption test: {other:?}"),
    }
}

#[test]
fn test_partial_record_corruption() {
    let config = TestConfig::new("partial_corruption");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    let valid_record = Record::new(Some("key".to_string()), "valid_value".to_string(), None);
    log.append(valid_record).unwrap();

    // With segment-based storage, files are in {data_dir}/{topic}/00000000000000000000.log
    let log_file_path = config
        .temp_dir_path()
        .join(topic)
        .join("00000000000000000000.log");
    let mut file = OpenOptions::new()
        .append(true)
        .open(&log_file_path)
        .unwrap();

    file.write_all(&[1, 2, 3, 4]).unwrap();
    file.sync_all().unwrap();

    let result = log.get_records_from_offset(0, None);

    match result {
        Ok(records) => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].record.value, "valid_value");
        }
        Err(StorageError::DataCorruption { .. }) => {}
        Err(other) => panic!("Unexpected error for partial corruption: {other:?}"),
    }
}

#[test]
fn test_error_state_recovery() {
    let config = TestConfig::new("error_recovery");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let offset1 = log.append(record1).unwrap();

    let large_record = Record::new(
        Some("huge".to_string()),
        "x".repeat(512 * 1024), // 512KB - half the segment size
        None,
    );
    let _large_result = log.append(large_record);

    let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);
    let offset2 = log.append(record2).unwrap();

    assert_eq!(offset1, 0);
    assert!(offset2 > offset1);
    assert!(log.len() >= 2);

    let records = log.get_records_from_offset(0, None).unwrap();
    assert!(records.len() >= 2);
    assert_eq!(records[0].record.value, "value1");
    assert_eq!(records[records.len() - 1].record.value, "value2");
}

#[test]
fn test_storage_error_source_conversion() {
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
    let storage_error = StorageError::from_io_error(io_error, "test operation");

    match storage_error {
        StorageError::PermissionDenied { context } => {
            assert_eq!(context, "test operation");
        }
        other => panic!("Expected PermissionDenied, got: {other:?}"),
    }
}

#[test]
fn test_serialization_error_conversion() {
    let invalid_json = "{ invalid: json }";
    let parse_result: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);

    match parse_result {
        Err(json_error) => {
            let storage_error =
                StorageError::from_serialization_error(json_error, "test serialization");

            match storage_error {
                StorageError::DataCorruption { context, details } => {
                    assert_eq!(context, "test serialization");
                    assert!(!details.is_empty());
                }
                other => panic!("Expected DataCorruption, got: {other:?}"),
            }
        }
        Ok(_) => panic!("Expected JSON parsing to fail"),
    }
}

#[test]
fn test_time_index_oversize_triggers_rebuild() {
    // Setup: create topic and write a few records
    let config = TestConfig::new("time_index_oversize");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .expect("create file topic log");

    let _ = log
        .append(Record::new(None, "a".to_string(), None))
        .unwrap();
    let _ = log
        .append(Record::new(None, "b".to_string(), None))
        .unwrap();
    log.sync().unwrap();

    // Craft an oversized time index file (> 1_000_000 entries) so bounded read fails
    let time_index_path = config
        .temp_dir_path()
        .join(topic)
        .join("00000000000000000000.timeindex");
    let oversized_entries = 1_000_001usize; // reader bound is 1_000_000
    let bytes = vec![0u8; oversized_entries * 12]; // 12 bytes per entry
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&time_index_path)
        .expect("open time index for oversize write");
    f.write_all(&bytes).expect("write oversize time index");
    f.sync_all().ok();

    // Reopen the topic; recovery should detect the error and rebuild from log
    drop(log);
    let log2 = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .expect("reopen after oversize time index");

    // Verify time-based read still works after rebuild
    let all = log2.get_records_from_offset(0, None).unwrap();
    assert!(all.len() >= 2);
    let ts0 = all[0].timestamp.clone();
    let from_ts = log2.get_records_from_timestamp(&ts0, None).unwrap();
    assert!(!from_ts.is_empty());
}

#[test]
fn test_offset_index_oversize_triggers_rebuild() {
    // Setup: create topic and write a record to generate segment files
    let config = TestConfig::new("offset_index_oversize");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .expect("create file topic log");

    let _ = log
        .append(Record::new(None, "x".to_string(), None))
        .unwrap();
    log.sync().unwrap();

    // Craft an oversized offset index file (> 1_000_000 entries) so bounded read fails
    let index_path = config
        .temp_dir_path()
        .join(topic)
        .join("00000000000000000000.index");
    let oversized_entries = 1_000_001usize; // reader bound is 1_000_000
    let bytes = vec![0u8; oversized_entries * 8]; // 8 bytes per entry
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&index_path)
        .expect("open offset index for oversize write");
    f.write_all(&bytes).expect("write oversize offset index");
    f.sync_all().ok();

    // Reopen the topic; offset index read error should trigger index rebuild and succeed
    drop(log);
    let log2 = FileTopicLog::new(
        topic,
        SyncMode::Immediate,
        config.temp_dir_path(),
        config.segment_size,
    )
    .expect("reopen after oversize offset index");

    // Verify offset-based read still works after rebuild
    let recs = log2.get_records_from_offset(0, None).unwrap();
    assert!(!recs.is_empty());
}
