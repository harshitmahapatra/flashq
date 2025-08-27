use flashq::error::StorageError;
use flashq::storage::StorageBackend;
use tempfile::tempdir;

#[test]
fn test_directory_locking() {
    // Create a temporary directory for this test
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("test_data");

    // First backend should acquire the lock successfully
    let backend1 = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("First backend should acquire lock successfully");

    // Second backend should fail to acquire the lock
    let backend2_result = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    );

    match backend2_result {
        Err(StorageError::DirectoryLocked { context, pid: _ }) => {
            assert!(context.contains("already in use"));
            // PID might be None if process detection fails, that's OK for the test
            // The important part is that we got the DirectoryLocked error
        }
        _ => panic!("Expected DirectoryLocked error, got {:?}", backend2_result),
    }

    // Verify lock file exists
    let lock_file = test_data_dir.join(".flashq.lock");
    assert!(lock_file.exists());

    // After dropping backend1, the lock should be released
    drop(backend1);

    // Now backend2 should be able to acquire the lock
    let _backend2 = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("Second backend should acquire lock after first is dropped");
}

#[test]
fn test_file_backend_creation() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("test_data");

    // Test file backend creation should succeed
    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("Should be able to create file backend");

    // Should be able to create a topic log
    let _storage = backend.create("test_topic").unwrap();
    
    // Should be able to create a consumer group
    let _consumer_group = backend.create_consumer_group("test_group").unwrap();
}

#[test]
fn test_stale_lock_recovery() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("test_data");
    
    // Create lock file manually to simulate a stale lock
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let lock_file_path = test_data_dir.join(".flashq.lock");
    std::fs::write(&lock_file_path, "PID: 999999\nTimestamp: 2023-01-01T00:00:00Z\n").unwrap();
    
    // Should be able to acquire lock despite stale lock file (PID 999999 shouldn't exist)
    let _backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("Should recover from stale lock");
}

#[test]
fn test_concurrent_access_prevention() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("concurrent_test");

    // First instance acquires lock
    let _backend1 = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("First backend should acquire lock");

    // Multiple attempts to acquire same lock should all fail
    for i in 0..3 {
        let result = StorageBackend::new_file_with_path(
            flashq::storage::file::SyncMode::Immediate,
            &test_data_dir,
        );
        
        match result {
            Err(StorageError::DirectoryLocked { .. }) => {
                // Expected - this is good
            }
            _ => panic!("Attempt {} should have failed with DirectoryLocked error", i),
        }
    }
}