use crate::error::StorageError;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Standard file I/O implementation using std::fs::File
pub struct FileIo;

impl FileIo {
    #[tracing::instrument(level = "debug", skip(path), fields(path = %path.display()))]
    pub fn create_with_append_and_read_permissions(path: &Path) -> Result<File, StorageError> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| {
                StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with append+read permissions: {path:?}"),
                )
            })
    }

    #[tracing::instrument(level = "debug", skip(path), fields(path = %path.display()))]
    pub fn create_with_write_truncate_permissions(path: &Path) -> Result<File, StorageError> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with write+truncate permissions: {path:?}"),
                )
            })
    }

    #[tracing::instrument(level = "debug", skip(path), fields(path = %path.display()))]
    pub fn open_with_read_only_permissions(path: &Path) -> Result<File, StorageError> {
        // Extract from async_io.rs UnifiedAsyncFileHandle::open_with_read_only_permissions
        File::open(path).map_err(|e| {
            StorageError::from_io_error(
                e,
                &format!("Failed to open file with read-only permissions: {path:?}"),
            )
        })
    }

    #[tracing::instrument(level = "debug", skip(handle, data), fields(len = data.len(), offset))]
    pub fn write_data_at_offset(
        handle: &mut File,
        data: &[u8],
        offset: u64,
    ) -> Result<(), StorageError> {
        handle.seek(SeekFrom::Start(offset)).map_err(|e| {
            StorageError::from_io_error(e, &format!("Failed to seek to offset {offset}"))
        })?;

        handle
            .write_all(data)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write data"))
    }

    #[tracing::instrument(level = "debug", skip(handle, buffer), fields(len = buffer.len(), offset))]
    pub fn read_data_at_offset(
        handle: &mut File,
        buffer: &mut [u8],
        offset: u64,
    ) -> Result<(), StorageError> {
        handle.seek(SeekFrom::Start(offset)).map_err(|e| {
            StorageError::from_io_error(e, &format!("Failed to seek to offset {offset}"))
        })?;

        handle
            .read_exact(buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read exact data"))
    }

    #[tracing::instrument(level = "debug", skip(handle, data), fields(len = data.len()))]
    pub fn append_data_to_end(handle: &mut File, data: &[u8]) -> Result<u64, StorageError> {
        let current_position = handle
            .seek(SeekFrom::End(0))
            .map_err(|e| StorageError::from_io_error(e, "Failed to seek to end of file"))?;

        handle
            .write_all(data)
            .map_err(|e| StorageError::from_io_error(e, "Failed to append data to file"))?;

        Ok(current_position)
    }

    #[tracing::instrument(level = "debug", skip(handle))]
    pub fn synchronize_to_disk(handle: &mut File) -> Result<(), StorageError> {
        handle
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync file to disk"))
    }

    #[tracing::instrument(level = "debug", skip(handle))]
    pub fn get_file_size(handle: &File) -> Result<u64, StorageError> {
        let file_metadata = handle
            .metadata()
            .map_err(|e| StorageError::from_io_error(e, "Failed to get file metadata"))?;

        Ok(file_metadata.len())
    }
}
