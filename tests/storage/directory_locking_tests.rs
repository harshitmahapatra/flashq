use flashq::error::StorageError;
use flashq::storage::StorageBackend;
use tempfile::tempdir;

#[test]
fn test_directory_locking() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("test_data");

    let backend1 = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("First backend should acquire lock successfully");

    let backend2_result = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    );

    match backend2_result {
        Err(StorageError::DirectoryLocked { context, .. }) => {
            assert!(context.contains("already in use"));
        }
        _ => panic!("Expected DirectoryLocked error, got {backend2_result:?}"),
    }

    let lock_file = test_data_dir.join(".flashq.lock");
    assert!(lock_file.exists());

    drop(backend1);

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

    let backend = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("Should be able to create file backend");

    let _storage = backend.create("test_topic").unwrap();
    let _consumer_group = backend.create_consumer_group("test_group").unwrap();
}

#[test]
fn test_stale_lock_recovery() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let test_data_dir = temp_dir.path().join("test_data");

    std::fs::create_dir_all(&test_data_dir).unwrap();
    let lock_file_path = test_data_dir.join(".flashq.lock");
    std::fs::write(
        &lock_file_path,
        "PID: 999999\nTimestamp: 2023-01-01T00:00:00Z\n",
    )
    .unwrap();

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

    let _backend1 = StorageBackend::new_file_with_path(
        flashq::storage::file::SyncMode::Immediate,
        &test_data_dir,
    )
    .expect("First backend should acquire lock");

    for i in 0..3 {
        let result = StorageBackend::new_file_with_path(
            flashq::storage::file::SyncMode::Immediate,
            &test_data_dir,
        );

        match result {
            Err(StorageError::DirectoryLocked { .. }) => {}
            _ => panic!("Attempt {i} should have failed with DirectoryLocked error"),
        }
    }
}
