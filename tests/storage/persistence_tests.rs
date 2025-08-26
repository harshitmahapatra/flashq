use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{FlashQ, Record};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_file_storage_persistence() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_persistence_test_{test_id}"));
    let topic_name = format!("persistent_topic_{test_id}");

    // Create queue and add records
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });
        let record1 = Record::new(None, "persistent_value1".to_string(), None);
        let record2 = Record::new(None, "persistent_value2".to_string(), None);

        queue.post_record(topic_name.clone(), record1).unwrap();
        queue.post_record(topic_name.clone(), record2).unwrap();
    } // Queue goes out of scope

    // Create new queue instance - should automatically recover existing topics
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });

        // Should be able to poll existing records WITHOUT posting new ones first
        let records = queue.poll_records(&topic_name, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "persistent_value1");
        assert_eq!(records[1].record.value, "persistent_value2");
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        
        // Now post a new record - should continue with correct offset  
        let record3 = Record::new(None, "new_value".to_string(), None);
        let offset = queue.post_record(topic_name.clone(), record3).unwrap();
        assert_eq!(offset, 2); // Should start from offset 2 (after the recovered records)
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_flashq_with_file_storage() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_file_test_{test_id}"));
    let topic_name = format!("file_test_{test_id}");

    let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    });

    let record = Record::new(Some("file_key".to_string()), "file_value".to_string(), None);
    let offset = queue.post_record(topic_name.clone(), record).unwrap();
    assert_eq!(offset, 0);

    let records = queue.poll_records(&topic_name, None).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "file_value");
    assert_eq!(records[0].record.key, Some("file_key".to_string()));
    assert_eq!(records[0].offset, 0);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_topic_recovery_on_startup() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_topic_recovery_test_{test_id}"));
    let topic1 = format!("topic1_{test_id}");
    let topic2 = format!("topic2_{test_id}");

    // Create queue and add records to multiple topics
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });
        
        // Post to topic1
        queue.post_record(topic1.clone(), Record::new(None, "topic1_msg1".to_string(), None)).unwrap();
        queue.post_record(topic1.clone(), Record::new(None, "topic1_msg2".to_string(), None)).unwrap();
        
        // Post to topic2  
        queue.post_record(topic2.clone(), Record::new(None, "topic2_msg1".to_string(), None)).unwrap();
    } // Queue goes out of scope

    // Create new queue instance - should automatically discover and load all existing topics
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });

        // Should be able to poll from topic1 immediately (tests topic recovery)
        let topic1_records = queue.poll_records(&topic1, None).unwrap();
        assert_eq!(topic1_records.len(), 2);
        assert_eq!(topic1_records[0].record.value, "topic1_msg1");
        assert_eq!(topic1_records[1].record.value, "topic1_msg2");
        
        // Should be able to poll from topic2 immediately (tests topic recovery)
        let topic2_records = queue.poll_records(&topic2, None).unwrap();
        assert_eq!(topic2_records.len(), 1);
        assert_eq!(topic2_records[0].record.value, "topic2_msg1");
        
        // High water marks should be correct after recovery
        assert_eq!(queue.get_high_water_mark(&topic1), 2);
        assert_eq!(queue.get_high_water_mark(&topic2), 1);
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test] 
fn test_consumer_group_recovery_on_startup() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_recovery_test_{test_id}"));
    let topic = format!("test_topic_{test_id}");
    let group1 = format!("group1_{test_id}");
    let group2 = format!("group2_{test_id}");

    // Create queue, topics, and consumer groups with committed offsets
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });
        
        // Post some records
        for i in 0..5 {
            queue.post_record(topic.clone(), Record::new(None, format!("msg{}", i), None)).unwrap();
        }
        
        // Create consumer groups and commit different offsets
        queue.create_consumer_group(group1.clone()).unwrap();
        queue.create_consumer_group(group2.clone()).unwrap();
        
        // Group1 consumes first 2 records and commits offset 2
        queue.poll_records_for_consumer_group(&group1, &topic, Some(2)).unwrap();
        queue.update_consumer_group_offset(&group1, topic.clone(), 2).unwrap();
        
        // Group2 consumes first 3 records and commits offset 3  
        queue.poll_records_for_consumer_group(&group2, &topic, Some(3)).unwrap();
        queue.update_consumer_group_offset(&group2, topic.clone(), 3).unwrap();
    } // Queue goes out of scope

    // Create new queue instance - should automatically recover consumer groups
    {
        let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        });

        // Should be able to get consumer group offsets immediately (tests consumer group recovery)
        assert_eq!(queue.get_consumer_group_offset(&group1, &topic).unwrap(), 2);
        assert_eq!(queue.get_consumer_group_offset(&group2, &topic).unwrap(), 3);
        
        // Should be able to poll from correct offsets
        let group1_records = queue.poll_records_for_consumer_group(&group1, &topic, None).unwrap();
        assert_eq!(group1_records.len(), 3); // Records 2, 3, 4 (offsets 2-4)
        assert_eq!(group1_records[0].record.value, "msg2");
        assert_eq!(group1_records[0].offset, 2);
        
        let group2_records = queue.poll_records_for_consumer_group(&group2, &topic, None).unwrap();
        assert_eq!(group2_records.len(), 2); // Records 3, 4 (offsets 3-4)
        assert_eq!(group2_records[0].record.value, "msg3");
        assert_eq!(group2_records[0].offset, 3);
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_recovery_with_no_existing_data() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_empty_recovery_test_{test_id}"));

    // Create queue pointing to non-existent directory - should handle gracefully
    let queue = FlashQ::with_storage_backend(StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    });

    // Should be able to post and poll normally
    let topic = format!("new_topic_{test_id}");
    queue.post_record(topic.clone(), Record::new(None, "test_msg".to_string(), None)).unwrap();
    
    let records = queue.poll_records(&topic, None).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.value, "test_msg");

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}
