use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{FlashQError, StorageError};
use super::file_io::FileIO;

use io_uring::{opcode, types::Fd, IoUring};
use log::{debug, info, warn};

static GLOBAL_IO_URING_AVAILABILITY_STATUS: OnceLock<bool> = OnceLock::new();

const INITIAL_RING_ENTRIES: u32 = 32;
const NOP_OPERATION_USER_DATA: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoRingOperationType {
    Read,
    Write,
    Append,
    Sync,
}

impl IoRingOperationType {
    fn as_str(self) -> &'static str {
        match self {
            IoRingOperationType::Read => "read",
            IoRingOperationType::Write => "write",
            IoRingOperationType::Append => "append",
            IoRingOperationType::Sync => "sync",
        }
    }

    fn user_data(self) -> u64 {
        match self {
            IoRingOperationType::Read => 0x02,
            IoRingOperationType::Write => 0x01,
            IoRingOperationType::Append => 0x03,
            IoRingOperationType::Sync => 0x04,
        }
    }

    fn is_read_operation(self) -> bool {
        matches!(self, IoRingOperationType::Read)
    }
}

/// io_uring file handle that wraps a standard File with io_uring functionality
pub struct IoUringFileHandle {
    file: std::fs::File,
    ring: Arc<Mutex<IoUring>>,
}

impl AsRawFd for IoUringFileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// io_uring file I/O implementation using Linux io_uring for high performance
pub struct IoUringFileIO;

impl IoUringFileIO {
    /// Check if io_uring is available on the current system
    pub fn is_available() -> bool {
        *GLOBAL_IO_URING_AVAILABILITY_STATUS.get_or_init(Self::detect_io_uring_functionality)
    }

    fn detect_io_uring_functionality() -> bool {
        match IoUring::new(INITIAL_RING_ENTRIES) {
            Ok(mut new_ring) => match Self::validate_io_uring_with_nop_operation(&mut new_ring) {
                Ok(_) => {
                    info!("io_uring detected and functional");
                    true
                }
                Err(validation_error) => {
                    warn!("io_uring detected but not functional: {validation_error}");
                    false
                }
            },
            Err(creation_error) => {
                info!("io_uring not available: {creation_error}");
                false
            }
        }
    }

    fn validate_io_uring_with_nop_operation(
        ring: &mut IoUring,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nop_operation_entry = opcode::Nop::new()
            .build()
            .user_data(NOP_OPERATION_USER_DATA);

        unsafe {
            ring.submission().push(&nop_operation_entry)?;
        }

        ring.submit()?;

        let completion_queue = ring.completion();
        for completion_queue_entry in completion_queue {
            if completion_queue_entry.user_data() == NOP_OPERATION_USER_DATA {
                return Ok(());
            }
        }

        Err("No-op operation failed".into())
    }

    fn create_handle(file: std::fs::File) -> Result<IoUringFileHandle, FlashQError> {
        let ring = IoUring::new(INITIAL_RING_ENTRIES)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to create IoUring instance"
            )))?;

        Ok(IoUringFileHandle {
            file,
            ring: Arc::new(Mutex::new(ring)),
        })
    }
}

impl FileIO for IoUringFileIO {
    type Handle = IoUringFileHandle;

    fn create_with_append_and_read_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to create file with append+read permissions: {path:?}"),
            )))?;

        Self::create_handle(file)
    }

    fn create_with_write_truncate_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to create file with write+truncate permissions: {path:?}"),
            )))?;

        Self::create_handle(file)
    }

    fn open_with_read_only_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        let file = std::fs::File::open(path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to open file with read-only permissions: {path:?}"),
            )))?;

        Self::create_handle(file)
    }

    fn write_data_at_offset(handle: &mut Self::Handle, data: &[u8], offset: u64) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Write;
        let operation = opcode::Write::new(
            Fd(handle.as_raw_fd()),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(op_type.user_data());

        let result = Self::execute_io_uring_operation(handle, operation, op_type)?;
        debug!("io_uring write completed: {result} bytes at offset {offset}");
        Ok(())
    }

    fn read_data_at_offset(handle: &mut Self::Handle, buffer: &mut [u8], offset: u64) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Read;
        let operation = opcode::Read::new(
            Fd(handle.as_raw_fd()),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(op_type.user_data());

        let result = Self::execute_io_uring_operation(handle, operation, op_type)?;
        debug!("io_uring read completed: {result} bytes from offset {offset}");
        
        if result < buffer.len() as i32 {
            return Err(FlashQError::Storage(StorageError::ReadFailed {
                context: "io_uring read returned fewer bytes than expected".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    format!("Expected {} bytes, got {}", buffer.len(), result)
                )),
            }));
        }
        
        Ok(())
    }

    fn append_data_to_end(handle: &mut Self::Handle, data: &[u8]) -> Result<u64, FlashQError> {
        // For io_uring append operations, we need to get current file size first
        let current_size = Self::get_file_size(handle)?;
        Self::write_data_at_offset(handle, data, current_size)?;
        Ok(current_size)
    }

    fn synchronize_to_disk(handle: &mut Self::Handle) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Sync;
        let operation = opcode::Fsync::new(Fd(handle.as_raw_fd()))
            .build()
            .user_data(op_type.user_data());

        let _result = Self::execute_io_uring_operation(handle, operation, op_type)?;
        debug!("io_uring fsync completed");
        Ok(())
    }

    fn get_file_size(handle: &Self::Handle) -> Result<u64, FlashQError> {
        let file_metadata = handle.file.metadata()
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to get file metadata",
            )))?;

        Ok(file_metadata.len())
    }
}

impl IoUringFileIO {
    fn execute_io_uring_operation(
        handle: &mut IoUringFileHandle,
        operation: io_uring::squeue::Entry,
        operation_type: IoRingOperationType,
    ) -> Result<i32, FlashQError> {
        let mut ring = handle.ring.lock().unwrap();

        // Submit operation without waiting
        unsafe {
            ring.submission()
                .push(&operation)
                .map_err(|e| Self::create_io_uring_error(operation_type, "submission push", e))?;
        }

        // Submit to kernel without blocking
        ring.submit()
            .map_err(|e| Self::create_io_uring_error(operation_type, "submit", e))?;

        // Poll for completion (non-blocking loop with yield)
        loop {
            if let Some(completion_entry) = ring.completion().next() {
                let result = completion_entry.result();
                if result < 0 {
                    return Err(Self::create_operation_failed_error(operation_type, result));
                }
                return Ok(result);
            }

            // Yield CPU to avoid busy waiting
            std::thread::yield_now();
        }
    }

    fn create_io_uring_error(
        operation_type: IoRingOperationType,
        context: &str,
        error: impl std::fmt::Display,
    ) -> FlashQError {
        let storage_error = if operation_type.is_read_operation() {
            StorageError::ReadFailed {
                context: format!("io_uring {context} for {}", operation_type.as_str()),
                source: Box::new(crate::error::StorageErrorSource::Custom(error.to_string())),
            }
        } else {
            StorageError::WriteFailed {
                context: format!("io_uring {context} for {}", operation_type.as_str()),
                source: Box::new(crate::error::StorageErrorSource::Custom(error.to_string())),
            }
        };
        FlashQError::Storage(storage_error)
    }

    fn create_operation_failed_error(
        operation_type: IoRingOperationType,
        result: i32,
    ) -> FlashQError {
        let storage_error = if operation_type.is_read_operation() {
            StorageError::ReadFailed {
                context: format!(
                    "io_uring {} failed with code: {result}",
                    operation_type.as_str()
                ),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {result}"
                ))),
            }
        } else {
            StorageError::WriteFailed {
                context: format!(
                    "io_uring {} failed with code: {result}",
                    operation_type.as_str()
                ),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {result}"
                ))),
            }
        };
        FlashQError::Storage(storage_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_io_uring_availability_detection() {
        // This test will succeed on systems with io_uring support
        // and provide useful information on systems without it
        let available = IoUringFileIO::is_available();
        println!("io_uring availability: {}", available);
        
        // Test should not fail regardless of availability
        // It's informational only
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_io_uring_file_operations() {
        // Only run this test on Linux systems with io_uring support
        if !IoUringFileIO::is_available() {
            println!("Skipping io_uring test - not available");
            return;
        }

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Test create and write
        let mut handle = IoUringFileIO::create_with_append_and_read_permissions(path).unwrap();
        
        let data = b"test data for io_uring";
        let pos = IoUringFileIO::append_data_to_end(&mut handle, data).unwrap();
        assert_eq!(pos, 0);

        // Test read back
        let mut buffer = vec![0u8; data.len()];
        IoUringFileIO::read_data_at_offset(&mut handle, &mut buffer, 0).unwrap();
        assert_eq!(buffer, data);

        // Test file size
        let size = IoUringFileIO::get_file_size(&handle).unwrap();
        assert_eq!(size, data.len() as u64);

        // Test sync
        IoUringFileIO::synchronize_to_disk(&mut handle).unwrap();
    }
}