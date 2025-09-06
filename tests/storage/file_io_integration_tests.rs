#[cfg(target_os = "linux")]
use flashq::storage::file::IoUringFileIO;
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

// io_uring tests (Linux only)
#[cfg(target_os = "linux")]
mod io_uring_tests {
    use super::*;

    #[test]
    fn test_io_uring_availability_detection() {
        let available = IoUringFileIO::is_available();
        println!("io_uring available: {available}");
        // This test always passes, it's just informational
    }

    #[test]
    fn test_io_uring_file_io_append_and_read() {
        if !IoUringFileIO::is_available() {
            println!("Skipping io_uring test - not available");
            return;
        }

        let temp_dir = tempdir().unwrap();
        let test_file_path = temp_dir.path().join("io_uring_append_read_test.log");

        let mut file_handle =
            IoUringFileIO::create_with_append_and_read_permissions(&test_file_path).unwrap();

        let test_data = b"Hello, io_uring I/O!";
        let start_position =
            IoUringFileIO::append_data_to_end(&mut file_handle, test_data).unwrap();
        IoUringFileIO::synchronize_to_disk(&mut file_handle).unwrap();

        let mut read_buffer = vec![0u8; test_data.len()];
        IoUringFileIO::read_data_at_offset(&mut file_handle, &mut read_buffer, 0).unwrap();

        assert_eq!(start_position, 0, "First write should start at position 0");
        assert_eq!(
            IoUringFileIO::get_file_size(&file_handle).unwrap(),
            test_data.len() as u64
        );
        assert_eq!(&read_buffer, test_data);
    }

    #[test]
    fn test_io_uring_write_truncate_mode() {
        if !IoUringFileIO::is_available() {
            println!("Skipping io_uring test - not available");
            return;
        }

        let temp_dir = tempdir().unwrap();
        let test_file_path = temp_dir.path().join("io_uring_truncate_test.json");

        let mut file_handle =
            IoUringFileIO::create_with_write_truncate_permissions(&test_file_path).unwrap();

        let json_data = r#"{"test": "io_uring_data"}"#;
        IoUringFileIO::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0).unwrap();
        IoUringFileIO::synchronize_to_disk(&mut file_handle).unwrap();

        let file_size = IoUringFileIO::get_file_size(&file_handle).unwrap();
        assert_eq!(file_size, json_data.len() as u64);
    }

    #[test]
    fn test_io_uring_sequential_writes() {
        if !IoUringFileIO::is_available() {
            println!("Skipping io_uring test - not available");
            return;
        }

        let temp_dir = tempdir().unwrap();
        let test_file_path = temp_dir.path().join("io_uring_sequential_test.log");

        let mut file_handle =
            IoUringFileIO::create_with_append_and_read_permissions(&test_file_path).unwrap();

        let data1 = b"First io_uring line\n";
        let data2 = b"Second io_uring line\n";

        let pos1 = IoUringFileIO::append_data_to_end(&mut file_handle, data1).unwrap();
        let pos2 = IoUringFileIO::append_data_to_end(&mut file_handle, data2).unwrap();

        assert_eq!(pos1, 0);
        assert_eq!(pos2, data1.len() as u64);

        let total_size = IoUringFileIO::get_file_size(&file_handle).unwrap();
        assert_eq!(total_size, (data1.len() + data2.len()) as u64);
    }

    #[test]
    fn test_io_uring_large_write() {
        if !IoUringFileIO::is_available() {
            println!("Skipping io_uring test - not available");
            return;
        }

        let temp_dir = tempdir().unwrap();
        let test_file_path = temp_dir.path().join("io_uring_large_test.log");

        let mut file_handle =
            IoUringFileIO::create_with_append_and_read_permissions(&test_file_path).unwrap();

        // Create a 64KB buffer
        let large_data = vec![b'A'; 64 * 1024];
        let start_position =
            IoUringFileIO::append_data_to_end(&mut file_handle, &large_data).unwrap();
        IoUringFileIO::synchronize_to_disk(&mut file_handle).unwrap();

        assert_eq!(start_position, 0);
        assert_eq!(
            IoUringFileIO::get_file_size(&file_handle).unwrap(),
            large_data.len() as u64
        );
    }
}

/// Test compatibility - ensure both implementations produce same results
#[test]
fn test_file_io_compatibility() {
    let temp_dir = tempdir().unwrap();

    // Test with StdFileIO
    let std_path = temp_dir.path().join("std_compatibility.log");
    let mut std_handle = StdFileIO::create_with_append_and_read_permissions(&std_path).unwrap();
    let test_data = b"Compatibility test data";
    StdFileIO::append_data_to_end(&mut std_handle, test_data).unwrap();
    StdFileIO::synchronize_to_disk(&mut std_handle).unwrap();
    let std_size = StdFileIO::get_file_size(&std_handle).unwrap();

    #[cfg(target_os = "linux")]
    {
        if IoUringFileIO::is_available() {
            // Test with IoUringFileIO
            let io_uring_path = temp_dir.path().join("io_uring_compatibility.log");
            let mut io_uring_handle =
                IoUringFileIO::create_with_append_and_read_permissions(&io_uring_path).unwrap();
            IoUringFileIO::append_data_to_end(&mut io_uring_handle, test_data).unwrap();
            IoUringFileIO::synchronize_to_disk(&mut io_uring_handle).unwrap();
            let io_uring_size = IoUringFileIO::get_file_size(&io_uring_handle).unwrap();

            // Both implementations should produce same file size
            assert_eq!(std_size, io_uring_size);
            assert_eq!(std_size, test_data.len() as u64);
        }
    }
}
