use super::test_utilities::*;
use flashq::storage::StorageBackend;
use flashq::FlashQ;

#[test]
fn test_consumer_group_creation_backends() {
    // Setup
    let config = TestConfig::new("consumer_creation");
    let group_id = create_test_consumer_group("creation");

    // Action & Expectation: Memory backend
    let memory_backend = StorageBackend::Memory;
    let memory_group = memory_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(memory_group.group_id(), &group_id);
    assert_eq!(memory_group.get_offset("test_topic"), 0);

    // Action & Expectation: File backend  
    let file_backend = StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: config.temp_dir.clone(),
    };
    let file_group = file_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(file_group.group_id(), &group_id);
    assert_eq!(file_group.get_offset("test_topic"), 0);
}

#[test]
fn test_consumer_group_persistence() {
    // Setup
    let config = TestConfig::new("consumer_persistence");
    let group_id = create_test_consumer_group("persistence");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir.clone();

    // Action: Create consumer group, set offset, drop it
    {
        let backend = StorageBackend::FileWithPath {
            sync_mode: config.sync_mode,
            data_dir: temp_dir.clone(),
        };
        let mut consumer_group = backend.create_consumer_group(&group_id).unwrap();
        consumer_group.set_offset(topic_name.clone(), 5);
    }

    // Action: Create new consumer group with same ID
    let backend = StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: temp_dir.clone(),
    };
    let recovered_group = backend.create_consumer_group(&group_id).unwrap();

    // Expectation: Should recover the offset
    assert_eq!(recovered_group.get_offset(&topic_name), 5);
}

#[test]
fn test_consumer_group_topic_recovery() {
    // Setup
    let config = TestConfig::new("topic_recovery");
    let group_id = create_test_consumer_group("topic_recovery");
    let topic_name = config.topic_name.clone();
    let temp_dir = config.temp_dir.clone();

    // Action: Create FlashQ, add records and consumer group
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: config.sync_mode,
            data_dir: temp_dir.clone(),
        });
        
        // Add some records
        queue.post_record(topic_name.clone(), flashq::Record::new(None, "msg1".to_string(), None)).unwrap();
        queue.post_record(topic_name.clone(), flashq::Record::new(None, "msg2".to_string(), None)).unwrap();
        
        // Create consumer group and consume one record, then commit the offset
        let _consumer_group = queue.create_consumer_group(group_id.clone()).unwrap();
        let records = queue.poll_records_for_consumer_group(&group_id, &topic_name, Some(1)).unwrap();
        assert_eq!(records.len(), 1);
        // Commit the offset after consuming the first record
        queue.update_consumer_group_offset(&group_id, topic_name.clone(), 1).unwrap();
    }

    // Action: Create new FlashQ instance
    let new_queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: temp_dir.clone(),
    });

    // Expectation: Consumer group should resume from correct offset
    let records = new_queue.poll_records_for_consumer_group(&group_id, &topic_name, None).unwrap();
    assert_eq!(records.len(), 1); // Should only get the second record
    assert_eq!(records[0].record.value, "msg2");
}

#[test]
fn test_multiple_consumer_groups_isolation() {
    // Setup
    let config = TestConfig::new("multi_groups");
    let group1 = create_test_consumer_group("group1");
    let group2 = create_test_consumer_group("group2");
    let topic_name = config.topic_name.clone();

    let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: config.sync_mode,
        data_dir: config.temp_dir.clone(),
    });

    // Action: Add records
    queue.post_record(topic_name.clone(), flashq::Record::new(None, "msg1".to_string(), None)).unwrap();
    queue.post_record(topic_name.clone(), flashq::Record::new(None, "msg2".to_string(), None)).unwrap();

    // Action: Group1 consumes all records
    let _group1_consumer = queue.create_consumer_group(group1.clone()).unwrap();
    let group1_records = queue.poll_records_for_consumer_group(&group1, &topic_name, None).unwrap();

    // Action: Group2 starts consuming
    let _group2_consumer = queue.create_consumer_group(group2.clone()).unwrap();  
    let group2_records = queue.poll_records_for_consumer_group(&group2, &topic_name, None).unwrap();

    // Expectation: Both groups should see all records independently
    assert_eq!(group1_records.len(), 2);
    assert_eq!(group2_records.len(), 2);
    assert_eq!(group1_records[0].record.value, "msg1");
    assert_eq!(group2_records[0].record.value, "msg1");
}