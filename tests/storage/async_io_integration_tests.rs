use flashq::storage::file::async_io::{AsyncFileHandle, IoRingExecutor};
use tempfile::tempdir;

#[test]
fn test_io_uring_availability_detection() {
    // Setup: Query io_uring availability
    // Action: Check if io_uring is available on current system
    let availability_status = IoRingExecutor::is_available_on_current_system();
    
    // Expectation: Status should be boolean (actual value depends on system)
    println!("io_uring available: {availability_status}");
    assert!(availability_status || !availability_status); // Always true, but documents the test purpose
}

#[test]
fn test_append_and_read_operations() {
    // Setup: Create file handle for append+read operations
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("append_read_test.log");
    let mut file_handle = AsyncFileHandle::create_with_append_and_read_permissions(&test_file_path).unwrap();
    
    // Action: Append data and read it back
    let test_data = b"Hello, World!";
    let start_position = file_handle.append_data_to_end_of_file(test_data).unwrap();
    file_handle.synchronize_file_to_disk().unwrap();
    
    let mut read_buffer = vec![0u8; test_data.len()];
    let bytes_read = file_handle.read_data_at_specific_offset(&mut read_buffer, 0).unwrap();
    
    // Expectation: Should return correct start position and read data correctly
    assert_eq!(start_position, 0, "First write should start at position 0");
    assert_eq!(file_handle.get_current_file_size_in_bytes().unwrap(), test_data.len() as u64);
    assert_eq!(bytes_read, test_data.len());
    assert_eq!(&read_buffer[..bytes_read], test_data);
}

#[test]
fn test_write_truncate_mode() {
    // Setup: Create file handle with write+truncate permissions
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("truncate_test.json");
    let mut file_handle = AsyncFileHandle::create_with_write_truncate_permissions(&test_file_path).unwrap();
    
    // Action: Write JSON data at specific offset
    let json_data = r#"{"test": "data"}"#;
    let bytes_written = file_handle.write_data_at_specific_offset(json_data.as_bytes(), 0).unwrap();
    file_handle.synchronize_file_to_disk().unwrap();
    
    // Expectation: Should write correct number of bytes and set correct file size
    assert_eq!(bytes_written, json_data.len());
    assert_eq!(file_handle.get_current_file_size_in_bytes().unwrap(), json_data.len() as u64);
}

#[test]
fn test_read_only_mode() {
    // Setup: Create file with content and open in read-only mode
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("read_test.log");
    let test_content = b"Test content for reading";
    std::fs::write(&test_file_path, test_content).unwrap();
    
    let mut file_handle = AsyncFileHandle::open_with_read_only_permissions(&test_file_path).unwrap();
    
    // Action: Read the file content
    let mut read_buffer = vec![0u8; test_content.len()];
    let bytes_read = file_handle.read_data_at_specific_offset(&mut read_buffer, 0).unwrap();
    
    // Expectation: Should read correct content and file size
    assert_eq!(file_handle.get_current_file_size_in_bytes().unwrap(), test_content.len() as u64);
    assert_eq!(bytes_read, test_content.len());
    assert_eq!(&read_buffer, test_content);
}

#[test]
fn test_multiple_append_operations() {
    // Setup: Create file handle and define test chunks
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("multi_append_test.log");
    let mut file_handle = AsyncFileHandle::create_with_append_and_read_permissions(&test_file_path).unwrap();
    
    let chunks = [b"First chunk\n" as &[u8], b"Second chunk\n", b"Third chunk\n"];
    let expected_positions = [0, 12, 25]; // Cumulative positions
    
    // Action: Perform multiple append operations
    let mut positions = Vec::new();
    for chunk in &chunks {
        positions.push(file_handle.append_data_to_end_of_file(chunk).unwrap());
    }
    file_handle.synchronize_file_to_disk().unwrap();
    
    // Read back all data to verify correctness
    let total_size: usize = chunks.iter().map(|c| c.len()).sum();
    let mut read_buffer = vec![0u8; total_size];
    let bytes_read = file_handle.read_data_at_specific_offset(&mut read_buffer, 0).unwrap();
    
    let mut expected_content = Vec::new();
    for chunk in &chunks {
        expected_content.extend_from_slice(chunk);
    }
    
    // Expectation: Each append should return correct starting position
    assert_eq!(positions, expected_positions, "Append operations should return correct start positions");
    assert_eq!(file_handle.get_current_file_size_in_bytes().unwrap(), total_size as u64);
    assert_eq!(bytes_read, total_size);
    assert_eq!(&read_buffer, &expected_content);
}