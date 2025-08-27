use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_consumer_group_creation_across_backends() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_backends_test_{test_id}"));
    let group_id = format!("test_group_{test_id}");

    // Test Memory backend
    let memory_backend = StorageBackend::Memory;
    let memory_consumer_group = memory_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(memory_consumer_group.group_id(), &group_id);
    assert_eq!(memory_consumer_group.get_offset("topic1"), 0); // Default offset

    // Test File backend
    let file_backend = StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    };
    let file_consumer_group = file_backend.create_consumer_group(&group_id).unwrap();
    assert_eq!(file_consumer_group.group_id(), &group_id);
    assert_eq!(file_consumer_group.get_offset("topic1"), 0); // Default offset

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_consumer_group_persistence_across_file_backend_instances() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_persistence_test_{test_id}"));
    let group_id = format!("persistent_group_{test_id}");
    let topic1 = format!("topic1_{test_id}");
    let topic2 = format!("topic2_{test_id}");

    // First instance - create and set offsets
    {
        let backend = StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        };
        let mut consumer_group = backend.create_consumer_group(&group_id).unwrap();
        
        // Set some offsets
        consumer_group.set_offset(topic1.clone(), 5);
        consumer_group.set_offset(topic2.clone(), 10);
        
        assert_eq!(consumer_group.get_offset(&topic1), 5);
        assert_eq!(consumer_group.get_offset(&topic2), 10);
        
        let all_offsets = consumer_group.get_all_offsets();
        assert_eq!(all_offsets.len(), 2);
        assert_eq!(all_offsets.get(&topic1), Some(&5));
        assert_eq!(all_offsets.get(&topic2), Some(&10));
    } // Consumer group goes out of scope

    // Second instance - should recover state
    {
        let backend = StorageBackend::FileWithPath {
            sync_mode: SyncMode::Immediate,
            data_dir: temp_dir.clone(),
        };
        let consumer_group = backend.create_consumer_group(&group_id).unwrap();
        
        // Should have recovered the previous state
        assert_eq!(consumer_group.get_offset(&topic1), 5);
        assert_eq!(consumer_group.get_offset(&topic2), 10);
        
        let all_offsets = consumer_group.get_all_offsets();
        assert_eq!(all_offsets.len(), 2);
        assert_eq!(all_offsets.get(&topic1), Some(&5));
        assert_eq!(all_offsets.get(&topic2), Some(&10));
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_consumer_group_file_vs_memory_compatibility() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_compatibility_test_{test_id}"));
    let group_id = format!("compat_group_{test_id}");
    let topic = format!("compat_topic_{test_id}");

    // Test that both backends provide the same interface and behavior
    let memory_backend = StorageBackend::Memory;
    let mut memory_consumer = memory_backend.create_consumer_group(&group_id).unwrap();

    let file_backend = StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    };
    let mut file_consumer = file_backend.create_consumer_group(&group_id).unwrap();

    // Test initial state
    assert_eq!(memory_consumer.group_id(), file_consumer.group_id());
    assert_eq!(memory_consumer.get_offset(&topic), file_consumer.get_offset(&topic));
    assert_eq!(memory_consumer.get_offset(&topic), 0); // Default

    // Test offset setting behavior
    memory_consumer.set_offset(topic.clone(), 42);
    file_consumer.set_offset(topic.clone(), 42);

    assert_eq!(memory_consumer.get_offset(&topic), 42);
    assert_eq!(file_consumer.get_offset(&topic), 42);

    // Test get_all_offsets behavior
    let memory_offsets = memory_consumer.get_all_offsets();
    let file_offsets = file_consumer.get_all_offsets();

    assert_eq!(memory_offsets.len(), file_offsets.len());
    assert_eq!(memory_offsets.get(&topic), file_offsets.get(&topic));
    assert_eq!(memory_offsets.get(&topic), Some(&42));

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_consumer_group_concurrent_access_simulation() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_concurrent_test_{test_id}"));
    let group_id = format!("concurrent_group_{test_id}");

    let backend = StorageBackend::FileWithPath {
        sync_mode: SyncMode::Immediate,
        data_dir: temp_dir.clone(),
    };

    // Simulate multiple consumer group instances (as would happen in concurrent scenarios)
    let topics = vec![
        format!("topic1_{test_id}"),
        format!("topic2_{test_id}"),
        format!("topic3_{test_id}"),
    ];

    // First "process" - set initial offsets
    {
        let mut consumer1 = backend.create_consumer_group(&group_id).unwrap();
        consumer1.set_offset(topics[0].clone(), 10);
        consumer1.set_offset(topics[1].clone(), 20);
    }

    // Second "process" - should see the changes and add more
    {
        let mut consumer2 = backend.create_consumer_group(&group_id).unwrap();
        assert_eq!(consumer2.get_offset(&topics[0]), 10);
        assert_eq!(consumer2.get_offset(&topics[1]), 20);
        assert_eq!(consumer2.get_offset(&topics[2]), 0); // Not set yet

        consumer2.set_offset(topics[2].clone(), 30);
    }

    // Third "process" - should see all changes
    {
        let consumer3 = backend.create_consumer_group(&group_id).unwrap();
        assert_eq!(consumer3.get_offset(&topics[0]), 10);
        assert_eq!(consumer3.get_offset(&topics[1]), 20);
        assert_eq!(consumer3.get_offset(&topics[2]), 30);

        let all_offsets = consumer3.get_all_offsets();
        assert_eq!(all_offsets.len(), 3);
        assert_eq!(all_offsets.get(&topics[0]), Some(&10));
        assert_eq!(all_offsets.get(&topics[1]), Some(&20));
        assert_eq!(all_offsets.get(&topics[2]), Some(&30));
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_consumer_group_sync_modes() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let temp_dir = std::env::temp_dir().join(format!("flashq_consumer_sync_test_{test_id}"));
    let group_id = format!("sync_group_{test_id}");
    let topic = format!("sync_topic_{test_id}");

    // Test different sync modes
    for sync_mode in [SyncMode::None, SyncMode::Immediate] {
        let backend = StorageBackend::FileWithPath {
            sync_mode,
            data_dir: temp_dir.join(format!("sync_{sync_mode:?}")),
        };
        
        let mut consumer_group = backend.create_consumer_group(&group_id).unwrap();
        consumer_group.set_offset(topic.clone(), 123);
        assert_eq!(consumer_group.get_offset(&topic), 123);

        // Create new instance to test persistence worked
        let consumer_group2 = backend.create_consumer_group(&group_id).unwrap();
        assert_eq!(consumer_group2.get_offset(&topic), 123);
    }

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}