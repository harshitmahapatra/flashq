use super::test_utilities::*;
use flashq::storage::{PartitionId, StorageBackend};
use test_log::test;

#[test]
fn test_load_snapshot_returns_zero_for_nonexistent_offset() {
    let group_id = create_test_consumer_group("test_group");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    let offset = store
        .load_snapshot("test_topic", PartitionId::new(0))
        .unwrap();

    assert_eq!(offset, 0);
}

#[test]
fn test_persist_and_load_snapshot_memory() {
    let group_id = create_test_consumer_group("persist_load");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    let persisted = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 42)
        .unwrap();
    assert!(persisted);

    let offset = store.load_snapshot("topic1", PartitionId::new(0)).unwrap();

    assert_eq!(offset, 42);
}

#[test]
fn test_persist_and_load_snapshot_file() {
    let config = TestConfig::new("persist_load_file");
    let group_id = create_test_consumer_group("persist_load_file");

    let backend =
        StorageBackend::new_file_with_path(config.sync_mode, config.temp_dir_path()).unwrap();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    let persisted = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();
    assert!(persisted);

    let offset = store.load_snapshot("topic1", PartitionId::new(0)).unwrap();

    assert_eq!(offset, 100);
}

#[test]
fn test_monotonic_enforcement_rejects_stale_offset() {
    let group_id = create_test_consumer_group("monotonic_reject");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();
    let result = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 50)
        .unwrap();

    assert!(!result);
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        100
    );
}

#[test]
fn test_monotonic_enforcement_accepts_equal_offset() {
    let group_id = create_test_consumer_group("monotonic_equal");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();
    let result = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();

    assert!(result);
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        100
    );
}

#[test]
fn test_monotonic_enforcement_accepts_greater_offset() {
    let group_id = create_test_consumer_group("monotonic_greater");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 50)
        .unwrap();
    let result = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();

    assert!(result);
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        100
    );
}

#[test]
fn test_file_persistence_survives_restart() {
    let config = TestConfig::new("file_restart");
    let group_id = create_test_consumer_group("restart_test");
    let temp_dir = config.temp_dir_path().to_path_buf();

    {
        let backend =
            StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap();
        let store = backend.create_consumer_offset_store(&group_id).unwrap();
        store
            .persist_snapshot("topic1".to_string(), PartitionId::new(0), 123)
            .unwrap();
        store
            .persist_snapshot("topic2".to_string(), PartitionId::new(1), 456)
            .unwrap();
    }

    let backend = StorageBackend::new_file_with_path(config.sync_mode, temp_dir).unwrap();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        123
    );
    assert_eq!(
        store.load_snapshot("topic2", PartitionId::new(1)).unwrap(),
        456
    );
}

#[test]
fn test_get_all_snapshots_returns_all_offsets() {
    let group_id = create_test_consumer_group("get_all");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 10)
        .unwrap();
    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(1), 20)
        .unwrap();
    store
        .persist_snapshot("topic2".to_string(), PartitionId::new(0), 30)
        .unwrap();

    let all_snapshots = store.get_all_snapshots().unwrap();

    assert_eq!(all_snapshots.len(), 3);
    assert_eq!(
        all_snapshots.get(&("topic1".to_string(), PartitionId::new(0))),
        Some(&10)
    );
    assert_eq!(
        all_snapshots.get(&("topic1".to_string(), PartitionId::new(1))),
        Some(&20)
    );
    assert_eq!(
        all_snapshots.get(&("topic2".to_string(), PartitionId::new(0))),
        Some(&30)
    );
}

#[test]
fn test_multiple_partitions_same_topic() {
    let group_id = create_test_consumer_group("multi_partition");

    let backend = StorageBackend::new_memory();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();

    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
        .unwrap();
    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(1), 200)
        .unwrap();
    store
        .persist_snapshot("topic1".to_string(), PartitionId::new(2), 300)
        .unwrap();

    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        100
    );
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(1)).unwrap(),
        200
    );
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(2)).unwrap(),
        300
    );
}

#[test]
fn test_file_monotonic_enforcement_survives_restart() {
    let config = TestConfig::new("file_monotonic_restart");
    let group_id = create_test_consumer_group("monotonic_restart");
    let temp_dir = config.temp_dir_path().to_path_buf();

    {
        let backend =
            StorageBackend::new_file_with_path(config.sync_mode, temp_dir.clone()).unwrap();
        let store = backend.create_consumer_offset_store(&group_id).unwrap();
        store
            .persist_snapshot("topic1".to_string(), PartitionId::new(0), 100)
            .unwrap();
    }

    let backend = StorageBackend::new_file_with_path(config.sync_mode, temp_dir).unwrap();
    let store = backend.create_consumer_offset_store(&group_id).unwrap();
    let result = store
        .persist_snapshot("topic1".to_string(), PartitionId::new(0), 50)
        .unwrap();

    assert!(!result);
    assert_eq!(
        store.load_snapshot("topic1", PartitionId::new(0)).unwrap(),
        100
    );
}
