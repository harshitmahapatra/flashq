use flashq::storage::file::{FileIO, StdFileIO};
use tempfile::tempdir;

/// Test basic file operations with StdFileIO
#[test]
fn test_std_file_io_append_and_read() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("std_append_read_test.log");

    let mut file_handle =
        StdFileIO::create_with_append_and_read_permissions(&test_file_path).unwrap();

    let test_data = b"Hello, Standard I/O!";
    let start_position = StdFileIO::append_data_to_end(&mut file_handle, test_data).unwrap();
    StdFileIO::synchronize_to_disk(&mut file_handle).unwrap();

    let mut read_buffer = vec![0u8; test_data.len()];
    StdFileIO::read_data_at_offset(&mut file_handle, &mut read_buffer, 0).unwrap();

    assert_eq!(start_position, 0, "First write should start at position 0");
    assert_eq!(
        StdFileIO::get_file_size(&file_handle).unwrap(),
        test_data.len() as u64
    );
    assert_eq!(&read_buffer, test_data);
}

#[test]
fn test_std_file_io_write_truncate_mode() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("std_truncate_test.json");

    let mut file_handle =
        StdFileIO::create_with_write_truncate_permissions(&test_file_path).unwrap();

    let json_data = r#"{"test": "standard_io_data"}"#;
    StdFileIO::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0).unwrap();
    StdFileIO::synchronize_to_disk(&mut file_handle).unwrap();

    let file_size = StdFileIO::get_file_size(&file_handle).unwrap();
    assert_eq!(file_size, json_data.len() as u64);
}

#[test]
fn test_std_file_io_sequential_writes() {
    let temp_dir = tempdir().unwrap();
    let test_file_path = temp_dir.path().join("std_sequential_test.log");

    let mut file_handle =
        StdFileIO::create_with_append_and_read_permissions(&test_file_path).unwrap();

    let data1 = b"First line\n";
    let data2 = b"Second line\n";

    let pos1 = StdFileIO::append_data_to_end(&mut file_handle, data1).unwrap();
    let pos2 = StdFileIO::append_data_to_end(&mut file_handle, data2).unwrap();

    assert_eq!(pos1, 0);
    assert_eq!(pos2, data1.len() as u64);

    let total_size = StdFileIO::get_file_size(&file_handle).unwrap();
    assert_eq!(total_size, (data1.len() + data2.len()) as u64);
}
