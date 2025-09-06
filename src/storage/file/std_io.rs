use super::file_io::FileIO;
use crate::error::{FlashQError, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Standard file I/O implementation using std::fs::File
/// This implementation extracts and consolidates the file I/O patterns
/// currently scattered across segment.rs, consumer_group.rs, and async_io.rs
pub struct StdFileIO;

impl FileIO for StdFileIO {
    type Handle = File;

    fn create_with_append_and_read_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        // Extract from async_io.rs UnifiedAsyncFileHandle::create_with_append_and_read_permissions
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with append+read permissions: {path:?}"),
                ))
            })
    }

    fn create_with_write_truncate_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        // Extract from async_io.rs UnifiedAsyncFileHandle::create_with_write_truncate_permissions
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with write+truncate permissions: {path:?}"),
                ))
            })
    }

    fn open_with_read_only_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        // Extract from async_io.rs UnifiedAsyncFileHandle::open_with_read_only_permissions
        File::open(path).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to open file with read-only permissions: {path:?}"),
            ))
        })
    }

    fn write_data_at_offset(
        handle: &mut Self::Handle,
        data: &[u8],
        offset: u64,
    ) -> Result<(), FlashQError> {
        // Extract from async_io.rs write_at_offset_using_standard_io
        handle.seek(SeekFrom::Start(offset)).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to seek to offset {offset}"),
            ))
        })?;

        handle.write_all(data).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(e, "Failed to write data"))
        })
    }

    fn read_data_at_offset(
        handle: &mut Self::Handle,
        buffer: &mut [u8],
        offset: u64,
    ) -> Result<(), FlashQError> {
        // Extract from async_io.rs read_at_offset_using_standard_io + common.rs patterns
        handle.seek(SeekFrom::Start(offset)).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to seek to offset {offset}"),
            ))
        })?;

        handle.read_exact(buffer).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(e, "Failed to read exact data"))
        })
    }

    fn append_data_to_end(handle: &mut Self::Handle, data: &[u8]) -> Result<u64, FlashQError> {
        // Extract from async_io.rs append_data_using_standard_io
        let current_position = handle.seek(SeekFrom::End(0)).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to seek to end of file",
            ))
        })?;

        handle.write_all(data).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to append data to file",
            ))
        })?;

        Ok(current_position)
    }

    fn synchronize_to_disk(handle: &mut Self::Handle) -> Result<(), FlashQError> {
        // Extract from async_io.rs sync_file_using_standard_io + common.rs sync_file_if_needed
        handle.sync_all().map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to sync file to disk",
            ))
        })
    }

    fn get_file_size(handle: &Self::Handle) -> Result<u64, FlashQError> {
        // Extract from async_io.rs get_current_file_size_in_bytes
        let file_metadata = handle.metadata().map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to get file metadata",
            ))
        })?;

        Ok(file_metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_create_with_append_and_read_permissions() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Should create/open successfully
        let mut handle = StdFileIO::create_with_append_and_read_permissions(path).unwrap();

        // Should be able to write (append mode)
        let data = b"test data";
        StdFileIO::append_data_to_end(&mut handle, data).unwrap();

        // Should be able to read
        let mut buffer = vec![0u8; data.len()];
        StdFileIO::read_data_at_offset(&mut handle, &mut buffer, 0).unwrap();
        assert_eq!(buffer, data);
    }

    #[test]
    fn test_write_and_read_at_offset_with_write_handle() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Use write+truncate handle for positional writes (like index files)
        let mut write_handle = StdFileIO::create_with_write_truncate_permissions(path).unwrap();

        // Write data at specific offset
        let data = b"hello world";
        let offset = 5u64;

        // First write some initial data to create the file with content
        let padding = vec![0u8; offset as usize];
        StdFileIO::write_data_at_offset(&mut write_handle, &padding, 0).unwrap();
        StdFileIO::write_data_at_offset(&mut write_handle, data, offset).unwrap();

        // Now open with read-only to read it back
        let mut read_handle = StdFileIO::open_with_read_only_permissions(path).unwrap();
        let mut buffer = vec![0u8; data.len()];
        StdFileIO::read_data_at_offset(&mut read_handle, &mut buffer, offset).unwrap();
        assert_eq!(buffer, data);
    }

    #[test]
    fn test_append_data_returns_correct_position() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut handle = StdFileIO::create_with_append_and_read_permissions(path).unwrap();

        let data1 = b"first";
        let data2 = b"second";

        // First append should return position 0
        let pos1 = StdFileIO::append_data_to_end(&mut handle, data1).unwrap();
        assert_eq!(pos1, 0);

        // Second append should return position after first data
        let pos2 = StdFileIO::append_data_to_end(&mut handle, data2).unwrap();
        assert_eq!(pos2, data1.len() as u64);

        // Verify total file size
        let size = StdFileIO::get_file_size(&handle).unwrap();
        assert_eq!(size, (data1.len() + data2.len()) as u64);
    }

    #[test]
    fn test_sync_does_not_error() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut handle = StdFileIO::create_with_append_and_read_permissions(path).unwrap();

        // Write some data
        StdFileIO::append_data_to_end(&mut handle, b"test").unwrap();

        // Sync should not error
        StdFileIO::synchronize_to_disk(&mut handle).unwrap();
    }

    #[test]
    fn test_truncate_permissions() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"existing data").unwrap();
        let path = temp_file.path();

        // Open with truncate should clear the file
        let handle = StdFileIO::create_with_write_truncate_permissions(path).unwrap();
        let size = StdFileIO::get_file_size(&handle).unwrap();
        assert_eq!(size, 0);
    }
}
