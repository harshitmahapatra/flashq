use super::test_utilities::*;
use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{FlashQ, Record};
use test_log::test;

#[test]
fn test_memory_vs_file_basic_operations() {
    let config = TestConfig::new("basic_compat");
    let topic_name = config.topic_name.clone();

    let memory_queue = FlashQ::with_storage_backend(StorageBackend::new_memory());
    let file_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap(),
    );
    let test_record = Record::new(Some("test_key".to_string()), "test_value".to_string(), None);

    let memory_offset = memory_queue
        .post_records(topic_name.clone(), vec![test_record.clone()])
        .unwrap();
    let file_offset = file_queue
        .post_records(topic_name.clone(), vec![test_record.clone()])
        .unwrap();

    assert_eq!(memory_offset, file_offset);

    let memory_records = memory_queue.poll_records(&topic_name, None).unwrap();
    let file_records = file_queue.poll_records(&topic_name, None).unwrap();

    assert_eq!(memory_records.len(), file_records.len());
    assert_eq!(memory_records[0].record.value, file_records[0].record.value);
    assert_eq!(memory_records[0].offset, file_records[0].offset);
}

#[test]
fn test_consumer_group_compatibility() {
    let config = TestConfig::new("consumer_compat");
    let group_id = create_test_consumer_group("compat");
    let topic_name = config.topic_name.clone();

    let memory_queue = FlashQ::with_storage_backend(StorageBackend::new_memory());
    let file_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap(),
    );

    for i in 0..3 {
        let record = Record::new(None, format!("message_{i}"), None);
        memory_queue
            .post_records(topic_name.clone(), vec![record.clone()])
            .unwrap();
        file_queue
            .post_records(topic_name.clone(), vec![record.clone()])
            .unwrap();
    }

    memory_queue
        .create_consumer_group(group_id.clone())
        .unwrap();
    file_queue.create_consumer_group(group_id.clone()).unwrap();

    let memory_offset = memory_queue
        .get_consumer_group_offset(&group_id, &topic_name)
        .unwrap_or(0);
    let file_offset = file_queue
        .get_consumer_group_offset(&group_id, &topic_name)
        .unwrap_or(0);

    let memory_batch = memory_queue
        .poll_records_for_consumer_group_from_offset(&group_id, &topic_name, memory_offset, Some(2))
        .unwrap();
    let file_batch = file_queue
        .poll_records_for_consumer_group_from_offset(&group_id, &topic_name, file_offset, Some(2))
        .unwrap();

    assert_eq!(memory_batch.len(), file_batch.len());
    assert_eq!(memory_batch.len(), 2);

    for (memory_record, file_record) in memory_batch.iter().zip(file_batch.iter()) {
        assert_eq!(memory_record.record.value, file_record.record.value);
        assert_eq!(memory_record.offset, file_record.offset);
    }
}

#[test]
fn test_sync_mode_behavior() {
    let config = TestConfig::new("sync_modes");
    let topic_name = config.topic_name.clone();
    let sync_modes = [SyncMode::Immediate, SyncMode::None];

    for (i, sync_mode) in sync_modes.iter().enumerate() {
        let queue = FlashQ::with_storage_backend(
            StorageBackend::new_file_with_path(*sync_mode, config.temp_dir_path()).unwrap(),
        );
        let mode_topic = format!("{topic_name}_{i}");
        let record = Record::new(None, format!("sync_test_{sync_mode:?}"), None);

        let offset = queue
            .post_records(mode_topic.clone(), vec![record])
            .unwrap();
        assert_eq!(offset, 0);

        let records = queue.poll_records(&mode_topic, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.value, format!("sync_test_{sync_mode:?}"));
    }
}

#[test]
fn test_file_storage_vs_memory_storage_interface() {
    let config = TestConfig::new("interface_compat");
    let group_id = create_test_consumer_group("interface");

    let memory_backend = StorageBackend::new_memory();
    let file_backend =
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap();

    let memory_group = memory_backend.create_consumer_group(&group_id).unwrap();
    let file_group = file_backend.create_consumer_group(&group_id).unwrap();

    assert_eq!(
        memory_group.read().unwrap().group_id(),
        file_group.read().unwrap().group_id()
    );
    assert_eq!(
        memory_group.read().unwrap().get_offset("any_topic"),
        file_group.read().unwrap().get_offset("any_topic")
    );
}
