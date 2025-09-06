use super::test_utilities::*;
use flashq::FlashQ;
use flashq::storage::StorageBackend;

#[test]
fn test_consumer_group_creation_backends() {
    let config = TestConfig::new("consumer_creation");
    let group_id = create_test_consumer_group("creation");

    let memory_backend = StorageBackend::new_memory();
    let memory_group = memory_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(memory_group.read().unwrap().group_id(), &group_id);
    assert_eq!(memory_group.read().unwrap().get_offset("test_topic"), 0);

    let file_backend = StorageBackend::new_file_with_path(
        config.sync_mode,
        Default::default(),
        config.temp_dir_path(),
    )
    .unwrap();
    let file_group = file_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(file_group.read().unwrap().group_id(), &group_id);
    assert_eq!(file_group.read().unwrap().get_offset("test_topic"), 0);
}

#[test]
fn test_consumer_group_persistence() {
    let config = TestConfig::new("consumer_persistence");
    let group_id = create_test_consumer_group("persistence");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir_path().to_path_buf();

    {
        let backend = StorageBackend::new_file_with_path(
            config.sync_mode,
            Default::default(),
            temp_dir.clone(),
        )
        .unwrap();
        let consumer_group = backend.create_consumer_group(&group_id).unwrap();
        consumer_group
            .write()
            .unwrap()
            .set_offset(topic_name.clone(), 5);
    }

    let backend =
        StorageBackend::new_file_with_path(config.sync_mode, Default::default(), temp_dir.clone())
            .unwrap();
    let recovered_group = backend.create_consumer_group(&group_id).unwrap();

    assert_eq!(recovered_group.read().unwrap().get_offset(&topic_name), 5);
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
            StorageBackend::new_file_with_path(
                config.sync_mode,
                Default::default(),
                temp_dir.clone(),
            )
            .unwrap(),
        );

        queue
            .post_record(
                topic_name.clone(),
                flashq::Record::new(None, "msg1".to_string(), None),
            )
            .unwrap();
        queue
            .post_record(
                topic_name.clone(),
                flashq::Record::new(None, "msg2".to_string(), None),
            )
            .unwrap();

        queue.create_consumer_group(group_id.clone()).unwrap();
        let records = queue
            .poll_records_for_consumer_group(&group_id, &topic_name, Some(1))
            .unwrap();
        assert_eq!(records.len(), 1);
        queue
            .update_consumer_group_offset(&group_id, topic_name.clone(), 1)
            .unwrap();
    }

    let new_queue = FlashQ::with_storage_backend(
        StorageBackend::new_file_with_path(config.sync_mode, Default::default(), temp_dir.clone())
            .unwrap(),
    );

    let records = new_queue
        .poll_records_for_consumer_group(&group_id, &topic_name, None)
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
        StorageBackend::new_file_with_path(
            config.sync_mode,
            Default::default(),
            config.temp_dir_path(),
        )
        .unwrap(),
    );

    queue
        .post_record(
            topic_name.clone(),
            flashq::Record::new(None, "msg1".to_string(), None),
        )
        .unwrap();
    queue
        .post_record(
            topic_name.clone(),
            flashq::Record::new(None, "msg2".to_string(), None),
        )
        .unwrap();

    queue.create_consumer_group(group1.clone()).unwrap();
    let group1_records = queue
        .poll_records_for_consumer_group(&group1, &topic_name, None)
        .unwrap();

    queue.create_consumer_group(group2.clone()).unwrap();
    let group2_records = queue
        .poll_records_for_consumer_group(&group2, &topic_name, None)
        .unwrap();

    assert_eq!(group1_records.len(), 2);
    assert_eq!(group2_records.len(), 2);
    assert_eq!(group1_records[0].record.value, "msg1");
    assert_eq!(group2_records[0].record.value, "msg1");
}
