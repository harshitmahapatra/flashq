use super::test_utilities::*;
use flashq::FlashQ;
use flashq::storage::StorageBackend;
use test_log::test;

#[test]
fn test_consumer_group_creation_backends() {
    let config = TestConfig::new("consumer_creation");
    let group_id = create_test_consumer_group("creation");

    let memory_backend = StorageBackend::new_memory();
    let memory_group = memory_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(memory_group.read().group_id(), &group_id);
    assert_eq!(memory_group.read().get_offset("test_topic"), 0);

    let file_backend =
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap();
    let file_group = file_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(file_group.read().group_id(), &group_id);
    assert_eq!(file_group.read().get_offset("test_topic"), 0);
}

#[test]
fn test_consumer_group_persistence() {
    let config = TestConfig::new("consumer_persistence");
    let group_id = create_test_consumer_group("persistence");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir_path().to_path_buf();

    {
        let backend =
            StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap();
        let consumer_group = backend.create_consumer_group(&group_id).unwrap();
        consumer_group.write().set_offset(topic_name.clone(), 5);
    }

    let backend = StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap();
    let recovered_group = backend.create_consumer_group(&group_id).unwrap();

    assert_eq!(recovered_group.read().get_offset(&topic_name), 5);
}

#[test]
fn test_consumer_group_topic_recovery() {
    // Setup
    let config = TestConfig::new("topic_recovery");
    let group_id = create_test_consumer_group("topic_recovery");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir_path().to_path_buf();

    {
        let queue = FlashQ::with_storage_backend(
            StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap(),
        );

        queue
            .post_records(
                topic_name.clone(),
                vec![flashq::Record::new(None, "msg1".to_string(), None)],
            )
            .unwrap();
        queue
            .post_records(
                topic_name.clone(),
                vec![flashq::Record::new(None, "msg2".to_string(), None)],
            )
            .unwrap();

        queue.create_consumer_group(group_id.clone()).unwrap();
        let current_offset = queue
            .get_consumer_group_offset(&group_id, &topic_name)
            .unwrap_or(0);
        let records = queue
            .poll_records_for_consumer_group_from_offset(
                &group_id,
                &topic_name,
                current_offset,
                Some(1),
            )
            .unwrap();
        assert_eq!(records.len(), 1);
        queue
            .update_consumer_group_offset(&group_id, topic_name.clone(), 1)
            .unwrap();
    }

    let new_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap(),
    );

    let current_offset = new_queue
        .get_consumer_group_offset(&group_id, &topic_name)
        .unwrap_or(0);
    let records = new_queue
        .poll_records_for_consumer_group_from_offset(&group_id, &topic_name, current_offset, None)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "msg2");
}

#[test]
fn test_multiple_consumer_groups_isolation() {
    // Setup
    let config = TestConfig::new("multi_groups");
    let group1 = create_test_consumer_group("group1");
    let group2 = create_test_consumer_group("group2");
    let topic_name = config.topic_name.clone();

    let queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap(),
    );

    queue
        .post_records(
            topic_name.clone(),
            vec![flashq::Record::new(None, "msg1".to_string(), None)],
        )
        .unwrap();
    queue
        .post_records(
            topic_name.clone(),
            vec![flashq::Record::new(None, "msg2".to_string(), None)],
        )
        .unwrap();

    queue.create_consumer_group(group1.clone()).unwrap();
    let group1_offset = queue
        .get_consumer_group_offset(&group1, &topic_name)
        .unwrap_or(0);
    let group1_records = queue
        .poll_records_for_consumer_group_from_offset(&group1, &topic_name, group1_offset, None)
        .unwrap();

    queue.create_consumer_group(group2.clone()).unwrap();
    let group2_offset = queue
        .get_consumer_group_offset(&group2, &topic_name)
        .unwrap_or(0);
    let group2_records = queue
        .poll_records_for_consumer_group_from_offset(&group2, &topic_name, group2_offset, None)
        .unwrap();

    assert_eq!(group1_records.len(), 2);
    assert_eq!(group2_records.len(), 2);
    assert_eq!(group1_records[0].record.value, "msg1");
    assert_eq!(group2_records[0].record.value, "msg1");
}

#[test]
fn test_empty_index_file_warning() {
    use std::fs;

    let config = TestConfig::new("empty_index_warning");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir_path().to_path_buf();

    // Create topic directory structure with empty index files
    let topic_dir = temp_dir.join(&topic_name);
    let partition_dir = topic_dir.join("0");
    fs::create_dir_all(&partition_dir).unwrap();

    // Create a log file with properly serialized record data
    let log_path = partition_dir.join("00000000000000000000.log");
    let record = flashq::Record::new(None, "test_value".to_string(), None);
    let serialized = flashq::storage::file::common::serialize_record(&record, 0).unwrap();
    fs::write(&log_path, serialized).unwrap();

    // Create empty index files (this should trigger the warning)
    let index_path = partition_dir.join("00000000000000000000.index");
    let time_index_path = partition_dir.join("00000000000000000000.timeindex");
    fs::write(&index_path, b"").unwrap(); // Empty index file
    fs::write(&time_index_path, b"").unwrap(); // Empty time index file

    // Now create a FlashQ instance which should trigger recovery and the warning
    let _queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, temp_dir).unwrap(),
    );

    // If we get here without panicking, the recovery handled empty index files correctly
    // The warning should have been logged (visible with RUST_LOG=debug)
}

#[test]
fn test_debug_empty_index_files() {
    let config = TestConfig::new("debug_empty_index");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir_path().to_path_buf();

    println!("Test directory: {}", temp_dir.display());

    // First FlashQ instance - create some records
    {
        let queue = FlashQ::with_storage_backend(
            StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap(),
        );

        // Post just 2 small records (should not trigger index writes)
        queue
            .post_records(
                topic_name.clone(),
                vec![flashq::Record::new(None, "msg1".to_string(), None)],
            )
            .unwrap();
        queue
            .post_records(
                topic_name.clone(),
                vec![flashq::Record::new(None, "msg2".to_string(), None)],
            )
            .unwrap();
    }

    // Check what files exist
    let topic_dir = temp_dir.join(&topic_name).join("0");
    if topic_dir.exists() {
        println!("Files in partition directory:");
        for entry in std::fs::read_dir(&topic_dir).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            println!(
                "  {} - {} bytes",
                entry.file_name().to_string_lossy(),
                metadata.len()
            );
        }
    }

    // Second FlashQ instance - should trigger recovery
    println!("Creating second FlashQ instance (should trigger recovery)...");
    let _queue2 = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, temp_dir).unwrap(),
    );

    println!("Recovery complete");
}
