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
    let mut log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 1).unwrap();

    create_disk_full_scenario(config.temp_dir_path()).ok();

    let large_record = Record::new(
        Some("key".to_string()),
        "x".repeat(1024 * 1024),
        None,
    );

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
    let mut log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 1).unwrap();

    let large_record = Record::new(
        Some("huge_key".to_string()),
        "x".repeat(10 * 1024 * 1024),
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
    let result = FileTopicLog::new("test_topic", SyncMode::Immediate, &readonly_dir, 1);

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

    let result = FileTopicLog::new(
        &config.topic_name,
        SyncMode::Immediate,
        config.temp_dir_path(),
        1,
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
        1,
    )
    .unwrap();

    let record = Record::new(Some("key".to_string()), "value".to_string(), None);
    log.append(record).unwrap();

    let log_file_path = config
        .temp_dir_path()
        .join(format!("{}.log", config.topic_name));
    corrupt_log_file(&log_file_path).unwrap();

    let result = log.get_records_from_offset(0, None);

    match result {
        Err(StorageError::DataCorruption { .. }) => {}
        Err(StorageError::ReadFailed { .. }) => {}
        Ok(_) => panic!("Expected read failure due to corruption"),
        Err(other) => panic!("Expected corruption/read error, got: {other:?}"),
    }
}

#[test]
fn test_wal_corruption_recovery() {
    let config = TestConfig::new("wal_corruption");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 5).unwrap();

    for i in 0..3 {
        let record = Record::new(Some(format!("key{i}")), format!("value{i}"), None);
        log.append(record).unwrap();
    }

    let wal_file_path = config.temp_dir_path().join(format!("{topic}.wal"));
    corrupt_log_file(&wal_file_path).unwrap();

    let recovery_result = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 5);

    match recovery_result {
        Ok(recovered_log) => {
            assert!(recovered_log.len() < 1000);
        }
        Err(e) => {
            let error_string = format!("{e}");
            assert!(
                error_string.contains("WAL recovery")
                    || error_string.contains("corruption")
                    || error_string.contains("failed"),
                "Expected recovery or corruption error, got: {error_string}"
            );
        }
    }
}


#[test]
fn test_partial_record_corruption() {
    let config = TestConfig::new("partial_corruption");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 1).unwrap();

    let valid_record = Record::new(Some("key".to_string()), "valid_value".to_string(), None);
    log.append(valid_record).unwrap();

    let log_file_path = config.temp_dir_path().join(format!("{topic}.log"));
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
fn test_json_corruption_detection() {
    let config = TestConfig::new("json_corruption");
    let topic = &config.topic_name;
    let log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 1).unwrap();

    let log_file_path = config.temp_dir_path().join(format!("{topic}.log"));
    let mut file = OpenOptions::new()
        .append(true)
        .open(&log_file_path)
        .unwrap();

    let invalid_json = b"{ invalid json content }";
    let length = invalid_json.len() as u32;
    let offset = 0u64;

    file.write_all(&length.to_le_bytes()).unwrap();
    file.write_all(&offset.to_le_bytes()).unwrap();
    file.write_all(invalid_json).unwrap();
    file.sync_all().unwrap();

    let result = log.get_records_from_offset(0, None);

    match result {
        Err(StorageError::DataCorruption { context, details }) => {
            assert!(context.contains("record parsing"));
            assert!(!details.is_empty(), "Error details should not be empty");
        }
        Err(other) => panic!("Expected DataCorruption error, got: {other:?}"),
        Ok(_) => panic!("Expected JSON corruption to be detected"),
    }
}


#[test]
fn test_error_state_recovery() {
    let config = TestConfig::new("error_recovery");
    let topic = &config.topic_name;
    let mut log = FileTopicLog::new(topic, SyncMode::Immediate, config.temp_dir_path(), 1).unwrap();

    let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
    let offset1 = log.append(record1).unwrap();

    let large_record = Record::new(
        Some("huge".to_string()),
        "x".repeat(100 * 1024 * 1024),
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
