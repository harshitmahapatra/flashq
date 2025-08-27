use super::test_utilities::*;
use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{FlashQ, Record};

#[test]
fn test_memory_vs_file_basic_operations() {
    // Setup
    let config = TestConfig::new("basic_compat");
    let topic_name = config.topic_name.clone();

    // Action: Test same operations on both backends
    let memory_queue = FlashQ::with_storage_backend(StorageBackend::new_memory());
    let file_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir.clone()).unwrap(),
    );

    let test_record = Record::new(Some("test_key".to_string()), "test_value".to_string(), None);

    // Action & Expectation: Both should behave identically
    let memory_offset = memory_queue
        .post_record(topic_name.clone(), test_record.clone())
        .unwrap();
    let file_offset = file_queue
        .post_record(topic_name.clone(), test_record.clone())
        .unwrap();

    assert_eq!(memory_offset, file_offset); // Both should start at 0

    let memory_records = memory_queue.poll_records(&topic_name, None).unwrap();
    let file_records = file_queue.poll_records(&topic_name, None).unwrap();

    assert_eq!(memory_records.len(), file_records.len());
    assert_eq!(memory_records[0].record.value, file_records[0].record.value);
    assert_eq!(memory_records[0].offset, file_records[0].offset);
}

#[test]
fn test_consumer_group_compatibility() {
    // Setup
    let config = TestConfig::new("consumer_compat");
    let group_id = create_test_consumer_group("compat");
    let topic_name = config.topic_name.clone();

    let memory_queue = FlashQ::with_storage_backend(StorageBackend::new_memory());
    let file_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir.clone()).unwrap(),
    );

    // Action: Add identical records to both
    for i in 0..3 {
        let record = Record::new(None, format!("message_{i}"), None);
        memory_queue
            .post_record(topic_name.clone(), record.clone())
            .unwrap();
        file_queue
            .post_record(topic_name.clone(), record.clone())
            .unwrap();
    }

    // Action: Create consumer groups and consume partially
    memory_queue
        .create_consumer_group(group_id.clone())
        .unwrap();
    file_queue.create_consumer_group(group_id.clone()).unwrap();

    let memory_batch = memory_queue
        .poll_records_for_consumer_group(&group_id, &topic_name, Some(2))
        .unwrap();
    let file_batch = file_queue
        .poll_records_for_consumer_group(&group_id, &topic_name, Some(2))
        .unwrap();

    // Expectation: Both should behave identically
    assert_eq!(memory_batch.len(), file_batch.len());
    assert_eq!(memory_batch.len(), 2);

    for (memory_record, file_record) in memory_batch.iter().zip(file_batch.iter()) {
        assert_eq!(memory_record.record.value, file_record.record.value);
        assert_eq!(memory_record.offset, file_record.offset);
    }
}

#[test]
fn test_sync_mode_behavior() {
    // Setup
    let config = TestConfig::new("sync_modes");
    let topic_name = config.topic_name.clone();

    // Test different sync modes
    let sync_modes = [SyncMode::Immediate, SyncMode::None];

    for (i, sync_mode) in sync_modes.iter().enumerate() {
        // Action: Create queue with specific sync mode
        let queue = FlashQ::with_storage_backend(
            StorageBackend::new_file_with_path(*sync_mode, config.temp_dir.clone()).unwrap(),
        );

        // Use unique topic name for each sync mode
        let mode_topic = format!("{topic_name}_{i}");

        // Action: Post record
        let record = Record::new(None, format!("sync_test_{sync_mode:?}"), None);
        let offset = queue.post_record(mode_topic.clone(), record).unwrap();

        // Expectation: Should work regardless of sync mode
        assert_eq!(offset, 0);

        let records = queue.poll_records(&mode_topic, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.value, format!("sync_test_{sync_mode:?}"));
    }
}

#[test]
fn test_file_storage_vs_memory_storage_interface() {
    // Setup
    let config = TestConfig::new("interface_compat");
    let group_id = create_test_consumer_group("interface");

    // Action & Expectation: Both backends should implement same interface
    let memory_backend = StorageBackend::new_memory();
    let file_backend =
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir.clone()).unwrap();

    // Consumer group creation should work identically
    let memory_group = memory_backend.create_consumer_group(&group_id).unwrap();
    let file_group = file_backend.create_consumer_group(&group_id).unwrap();

    assert_eq!(memory_group.group_id(), file_group.group_id());

    // Default offset behavior should be identical
    assert_eq!(
        memory_group.get_offset("any_topic"),
        file_group.get_offset("any_topic")
    );
}
