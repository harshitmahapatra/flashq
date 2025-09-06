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
