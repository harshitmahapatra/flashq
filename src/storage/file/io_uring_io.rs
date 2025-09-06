use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::file_io::FileIO;
use crate::error::{FlashQError, StorageError};

use io_uring::{IoUring, opcode, types::Fd};
use log::{info, trace, warn};

static GLOBAL_IO_URING_AVAILABILITY_STATUS: OnceLock<bool> = OnceLock::new();

const DEFAULT_RING_ENTRIES: u32 = 256;
const NOP_OPERATION_USER_DATA: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoRingOperationType {
    Read,
    Write,
    Sync,
}

impl IoRingOperationType {
    fn as_str(self) -> &'static str {
        match self {
            IoRingOperationType::Read => "read",
            IoRingOperationType::Write => "write",
            IoRingOperationType::Sync => "sync",
        }
    }

    fn user_data(self) -> u64 {
        match self {
            IoRingOperationType::Read => 0x02,
            IoRingOperationType::Write => 0x01,
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
    /// Cached file size for append operations (optimization for single-writer segments)
    cached_size: AtomicU64,
}

impl AsRawFd for IoUringFileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// io_uring file I/O implementation using Linux io_uring for high performance
pub struct IoUringFileIO;

impl IoUringFileIO {
    /// Get the ring entries count from environment variable or use default
    fn ring_entries() -> u32 {
        std::env::var("FLASHQ_IOURING_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RING_ENTRIES)
    }

    /// Create IoUring with optimized builder flags, fallback to basic creation if needed
    fn create_io_uring(entries: u32) -> Result<IoUring, std::io::Error> {
        // Try with cooperation flags for better performance (best-effort)
        match io_uring::IoUring::builder()
            .setup_coop_taskrun()
            .setup_single_issuer()
            .build(entries)
        {
            Ok(ring) => {
                info!("io_uring created with cooperation flags enabled");
                Ok(ring)
            }
            Err(_) => {
                // Fallback to basic IoUring creation without advanced flags
                match IoUring::new(entries) {
                    Ok(ring) => {
                        info!("io_uring created with basic configuration");
                        Ok(ring)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Check if io_uring is available on the current system
    pub fn is_available() -> bool {
        *GLOBAL_IO_URING_AVAILABILITY_STATUS.get_or_init(Self::detect_io_uring_functionality)
    }

    fn detect_io_uring_functionality() -> bool {
        match Self::create_io_uring(Self::ring_entries()) {
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
        let ring = Self::create_io_uring(Self::ring_entries()).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to create IoUring instance",
            ))
        })?;

        // Initialize cached size with current file size
        let initial_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(IoUringFileHandle {
            file,
            ring: Arc::new(Mutex::new(ring)),
            cached_size: AtomicU64::new(initial_size),
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
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with append+read permissions: {path:?}"),
                ))
            })?;

        Self::create_handle(file)
    }

    fn create_with_write_truncate_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("Failed to create file with write+truncate permissions: {path:?}"),
                ))
            })?;

        Self::create_handle(file)
    }

    fn open_with_read_only_permissions(path: &Path) -> Result<Self::Handle, FlashQError> {
        let file = std::fs::File::open(path).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("Failed to open file with read-only permissions: {path:?}"),
            ))
        })?;

        Self::create_handle(file)
    }

    fn write_data_at_offset(
        handle: &mut Self::Handle,
        data: &[u8],
        offset: u64,
    ) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Write;
        let operation =
            opcode::Write::new(Fd(handle.as_raw_fd()), data.as_ptr(), data.len() as u32)
                .offset(offset)
                .build()
                .user_data(op_type.user_data());

        let result = Self::execute_io_uring_operation(handle, operation, op_type)?;
        trace!("io_uring write completed: {result} bytes at offset {offset}");
        Ok(())
    }

    fn read_data_at_offset(
        handle: &mut Self::Handle,
        buffer: &mut [u8],
        offset: u64,
    ) -> Result<(), FlashQError> {
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
        trace!("io_uring read completed: {result} bytes from offset {offset}");

        if result < buffer.len() as i32 {
            return Err(FlashQError::Storage(StorageError::ReadFailed {
                context: "io_uring read returned fewer bytes than expected".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "Expected {} bytes, got {}",
                    buffer.len(),
                    result
                ))),
            }));
        }

        Ok(())
    }

    fn append_data_to_end(handle: &mut Self::Handle, data: &[u8]) -> Result<u64, FlashQError> {
        // Use cached size for append operations (optimization for single-writer segments)
        let current_size = handle.cached_size.load(Ordering::Relaxed);
        Self::write_data_at_offset(handle, data, current_size)?;
        // Update cached size to include the appended data
        handle
            .cached_size
            .store(current_size + data.len() as u64, Ordering::Relaxed);
        Ok(current_size)
    }

    fn synchronize_to_disk(handle: &mut Self::Handle) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Sync;
        let operation = opcode::Fsync::new(Fd(handle.as_raw_fd()))
            .build()
            .user_data(op_type.user_data());

        let _result = Self::execute_io_uring_operation(handle, operation, op_type)?;
        trace!("io_uring fsync completed");
        Ok(())
    }

    fn get_file_size(handle: &Self::Handle) -> Result<u64, FlashQError> {
        let file_metadata = handle.file.metadata().map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to get file metadata",
            ))
        })?;

        let actual_size = file_metadata.len();

        // Sync cached size with actual size when explicitly queried
        handle.cached_size.store(actual_size, Ordering::Relaxed);

        Ok(actual_size)
    }
}

impl IoUringFileIO {
    fn execute_io_uring_operation(
        handle: &mut IoUringFileHandle,
        operation: io_uring::squeue::Entry,
        operation_type: IoRingOperationType,
    ) -> Result<i32, FlashQError> {
        let mut ring = handle.ring.lock().unwrap();

        // Submit operation
        unsafe {
            ring.submission()
                .push(&operation)
                .map_err(|e| Self::create_io_uring_error(operation_type, "submission push", e))?;
        }

        // Submit-first optimization: submit and try to get completion without waiting
        ring.submit()
            .map_err(|e| Self::create_io_uring_error(operation_type, "submit", e))?;

        // Check if completion is already available (reduces syscalls)
        if let Some(completion_entry) = ring.completion().next() {
            return Self::process_completion_entry(completion_entry, operation_type);
        }

        // Fallback: wait for completion if not immediately available
        ring.submit_and_wait(1)
            .map_err(|e| Self::create_io_uring_error(operation_type, "submit_and_wait", e))?;

        if let Some(completion_entry) = ring.completion().next() {
            return Self::process_completion_entry(completion_entry, operation_type);
        }

        Err(Self::create_io_uring_error(
            operation_type,
            "completion queue",
            "No completion received after submit_and_wait",
        ))
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

    fn process_completion_entry(
        completion_entry: io_uring::cqueue::Entry,
        operation_type: IoRingOperationType,
    ) -> Result<i32, FlashQError> {
        let result = completion_entry.result();
        if result < 0 {
            Err(Self::create_operation_failed_error(operation_type, result))
        } else {
            Ok(result)
        }
    }

    /// Batch execution of multiple io_uring operations for improved performance
    /// Internal helper that submits multiple operations and waits for all completions
    fn execute_batch(
        handle: &mut IoUringFileHandle,
        operations: &[io_uring::squeue::Entry],
        operation_types: &[IoRingOperationType],
    ) -> Result<Vec<i32>, FlashQError> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let op_count = operations.len();
        if op_count != operation_types.len() {
            return Err(FlashQError::Storage(StorageError::WriteFailed {
                context: "Batch operation count mismatch".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    "Operations and types arrays must have same length".to_string(),
                )),
            }));
        }

        let mut ring = handle.ring.lock().unwrap();
        let mut results = Vec::with_capacity(op_count);

        // Submit all operations to the submission queue
        for (i, operation) in operations.iter().enumerate() {
            unsafe {
                ring.submission().push(operation).map_err(|e| {
                    Self::create_io_uring_error(operation_types[i], "batch submission push", e)
                })?;
            }
        }

        // Submit all operations and wait for all completions
        ring.submit_and_wait(op_count)
            .map_err(|e| Self::create_io_uring_error(operation_types[0], "batch submit_and_wait", e))?;

        // Collect all completion results
        for _ in 0..op_count {
            if let Some(completion_entry) = ring.completion().next() {
                let result = completion_entry.result();
                if result < 0 {
                    // For batch operations, we need to determine which operation failed
                    // For now, we'll use the first operation type for error context
                    return Err(Self::create_operation_failed_error(operation_types[0], result));
                }
                results.push(result);
            } else {
                return Err(Self::create_io_uring_error(
                    operation_types[0],
                    "batch completion queue",
                    format!("Expected {} completions, got {}", op_count, results.len()),
                ));
            }
        }

        trace!("io_uring batch completed: {op_count} operations");
        Ok(results)
    }
}
