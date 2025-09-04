use flashq::storage::file::async_io::{AsyncFileHandle, IoRingExecutor};
use tempfile::tempdir;

#[test]
fn test_io_uring_availability_detection() {
    let availability_status = IoRingExecutor::is_available_on_current_system();
    println!("io_uring available: {availability_status}");
}

#[test]
fn test_async_file_handle_comprehensive_operations() {
    let temporary_directory = tempdir().unwrap();
    let test_file_path = temporary_directory.path().join("test.log");

    let mut file_handle =
        AsyncFileHandle::create_with_append_and_read_permissions(&test_file_path).unwrap();

    let test_data = b"Hello, World!";
    let bytes_written = file_handle.append_data_to_end_of_file(test_data).unwrap();
    assert_eq!(bytes_written, test_data.len());

    file_handle.synchronize_file_to_disk().unwrap();

    let file_size = file_handle.get_current_file_size_in_bytes().unwrap();
    assert_eq!(file_size, test_data.len() as u64);

    let mut read_buffer = vec![0u8; test_data.len()];
    let bytes_read = file_handle
        .read_data_at_specific_offset(&mut read_buffer, 0)
        .unwrap();
    assert_eq!(bytes_read, test_data.len());
    assert_eq!(&read_buffer[..bytes_read], test_data);
}

#[test]
fn test_async_file_handle_write_truncate_mode() {
    let temporary_directory = tempdir().unwrap();
    let test_file_path = temporary_directory.path().join("truncate_test.json");

    let mut file_handle =
        AsyncFileHandle::create_with_write_truncate_permissions(&test_file_path).unwrap();

    let json_data = r#"{"test": "data"}"#;
    let bytes_written = file_handle
        .write_data_at_specific_offset(json_data.as_bytes(), 0)
        .unwrap();
    assert_eq!(bytes_written, json_data.len());

    file_handle.synchronize_file_to_disk().unwrap();

    let file_size = file_handle.get_current_file_size_in_bytes().unwrap();
    assert_eq!(file_size, json_data.len() as u64);
}

#[test]
fn test_async_file_handle_read_only_mode() {
    let temporary_directory = tempdir().unwrap();
    let test_file_path = temporary_directory.path().join("read_test.log");

    // First create a file with some data
    std::fs::write(&test_file_path, b"Test content for reading").unwrap();

    let mut file_handle =
        AsyncFileHandle::open_with_read_only_permissions(&test_file_path).unwrap();

    let file_size = file_handle.get_current_file_size_in_bytes().unwrap();
    assert_eq!(file_size, 24); // Length of "Test content for reading"

    let mut read_buffer = vec![0u8; file_size as usize];
    let bytes_read = file_handle
        .read_data_at_specific_offset(&mut read_buffer, 0)
        .unwrap();
    assert_eq!(bytes_read, file_size as usize);
    assert_eq!(&read_buffer, b"Test content for reading");
}

#[test]
fn test_async_file_handle_multiple_operations() {
    let temporary_directory = tempdir().unwrap();
    let test_file_path = temporary_directory.path().join("multi_test.log");

    let mut file_handle =
        AsyncFileHandle::create_with_append_and_read_permissions(&test_file_path).unwrap();

    // Write multiple chunks
    let chunk1 = b"First chunk\n";
    let chunk2 = b"Second chunk\n";
    let chunk3 = b"Third chunk\n";

    let bytes1 = file_handle.append_data_to_end_of_file(chunk1).unwrap();
    let bytes2 = file_handle.append_data_to_end_of_file(chunk2).unwrap();
    let bytes3 = file_handle.append_data_to_end_of_file(chunk3).unwrap();

    assert_eq!(bytes1, chunk1.len());
    assert_eq!(bytes2, chunk2.len());
    assert_eq!(bytes3, chunk3.len());

    file_handle.synchronize_file_to_disk().unwrap();

    let total_size = chunk1.len() + chunk2.len() + chunk3.len();
    let file_size = file_handle.get_current_file_size_in_bytes().unwrap();
    assert_eq!(file_size, total_size as u64);

    // Read back all data
    let mut read_buffer = vec![0u8; total_size];
    let bytes_read = file_handle
        .read_data_at_specific_offset(&mut read_buffer, 0)
        .unwrap();
    assert_eq!(bytes_read, total_size);

    let mut expected_content = Vec::new();
    expected_content.extend_from_slice(chunk1);
    expected_content.extend_from_slice(chunk2);
    expected_content.extend_from_slice(chunk3);
    assert_eq!(&read_buffer, &expected_content);
}
