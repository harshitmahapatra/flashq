use crate::error::{FlashQError, StorageError};
use io_uring::{IoUring, opcode};
use log::{debug, info, warn};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static GLOBAL_IO_URING_AVAILABILITY_STATUS: OnceLock<bool> = OnceLock::new();

const INITIAL_RING_ENTRIES: u32 = 32;
const SINGLE_OPERATION_RING_ENTRIES: u32 = 1;
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

pub struct IoRingExecutorWithRuntime {
    tokio_runtime: Runtime,
}

impl IoRingExecutorWithRuntime {
    pub fn is_available_on_current_system() -> bool {
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


    pub fn execute_future_synchronously<F, R>(&mut self, future: F) -> R
    where
        F: std::future::Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        self.tokio_runtime.block_on(future)
    }
}

pub struct UnifiedAsyncFileHandle {
    underlying_file: File,
    should_prefer_io_uring: bool,
}

impl UnifiedAsyncFileHandle {
    pub fn create_with_append_and_read_permissions<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, FlashQError> {
        let opened_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| Self::create_file_error(e, path.as_ref(), "opening file"))?;

        Self::create_handle_with_debug_logging(opened_file, path.as_ref())
    }

    pub fn create_with_write_truncate_permissions<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, FlashQError> {
        let opened_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Self::create_file_error(e, path.as_ref(), "opening file"))?;

        Self::create_handle_with_debug_logging(opened_file, path.as_ref())
    }

    pub fn open_with_read_only_permissions<P: AsRef<Path>>(path: P) -> Result<Self, FlashQError> {
        let opened_file = File::open(&path)
            .map_err(|e| Self::create_file_error(e, path.as_ref(), "opening file"))?;

        Self::create_handle_with_debug_logging(opened_file, path.as_ref())
    }

    fn create_file_error(error: std::io::Error, path: &Path, context: &str) -> FlashQError {
        FlashQError::Storage(StorageError::from_io_error(
            error,
            &format!("{context} {path:?}"),
        ))
    }

    fn create_handle_with_debug_logging(file: File, path: &Path) -> Result<Self, FlashQError> {
        let io_uring_availability = IoRingExecutorWithRuntime::is_available_on_current_system();
        
        debug!(
            "Created AsyncFileHandle for {path:?}, io_uring: {io_uring_availability}"
        );

        Ok(UnifiedAsyncFileHandle {
            underlying_file: file,
            should_prefer_io_uring: io_uring_availability,
        })
    }

    pub fn write_data_at_specific_offset(
        &mut self,
        data: &[u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        if self.should_prefer_io_uring {
            self.write_at_offset_using_io_uring(data, offset)
        } else {
            self.write_at_offset_using_standard_io(data, offset)
        }
    }

    pub fn read_data_at_specific_offset(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        if self.should_prefer_io_uring {
            self.read_at_offset_using_io_uring(buf, offset)
        } else {
            self.read_at_offset_using_standard_io(buf, offset)
        }
    }

    pub fn append_data_to_end_of_file(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        if self.should_prefer_io_uring {
            self.append_data_using_io_uring(data)
        } else {
            self.append_data_using_standard_io(data)
        }
    }

    pub fn synchronize_file_to_disk(&mut self) -> Result<(), FlashQError> {
        if self.should_prefer_io_uring {
            self.sync_file_using_io_uring()
        } else {
            self.sync_file_using_standard_io()
        }
    }

    pub fn seek_to_end_and_get_position(&mut self) -> Result<u64, FlashQError> {
        self.underlying_file
            .seek(SeekFrom::End(0))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "seeking to end")))
    }

    pub fn get_current_file_size_in_bytes(&mut self) -> Result<u64, FlashQError> {
        let file_metadata = self.underlying_file.metadata().map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(e, "getting file metadata"))
        })?;
        Ok(file_metadata.len())
    }

    pub fn create_buffered_reader_for_sequential_access(&mut self) -> BufReader<&mut File> {
        BufReader::new(&mut self.underlying_file)
    }

    fn write_at_offset_using_standard_io(
        &mut self,
        data: &[u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        self.underlying_file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("seeking to offset {offset}"),
                ))
            })?;

        self.underlying_file
            .write_all(data)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "writing data")))?;

        Ok(data.len())
    }

    fn read_at_offset_using_standard_io(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        self.underlying_file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("seeking to offset {offset}"),
                ))
            })?;

        self.underlying_file
            .read(buf)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "reading data")))
    }

    fn append_data_using_standard_io(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        self.underlying_file
            .seek(SeekFrom::End(0))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "seeking to end")))?;

        self.underlying_file
            .write_all(data)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "appending data")))?;

        Ok(data.len())
    }

    fn sync_file_using_standard_io(&mut self) -> Result<(), FlashQError> {
        self.underlying_file
            .sync_all()
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "syncing file")))
    }

    fn execute_io_uring_operation(
        &mut self, 
        operation: io_uring::squeue::Entry, 
        operation_type: IoRingOperationType
    ) -> Result<i32, FlashQError> {
        let mut ring = self.create_io_uring_for_operation(operation_type)?;
        
        unsafe {
            ring.submission()
                .push(&operation)
                .map_err(|e| self.create_io_uring_error(operation_type, "submission push", e))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| self.create_io_uring_error(operation_type, "submit_and_wait", e))?;

        let completion_entry = ring.completion()
            .next()
            .ok_or_else(|| self.create_completion_queue_empty_error(operation_type))?;

        let result = completion_entry.result();
        if result < 0 {
            return Err(self.create_operation_failed_error(operation_type, result));
        }

        Ok(result)
    }

    fn create_io_uring_for_operation(&self, operation_type: IoRingOperationType) -> Result<IoUring, FlashQError> {
        IoUring::new(SINGLE_OPERATION_RING_ENTRIES)
            .map_err(|e| self.create_io_uring_error(operation_type, "creation", e))
    }

    fn create_io_uring_error(
        &self, 
        operation_type: IoRingOperationType, 
        context: &str, 
        error: impl std::fmt::Display
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

    fn create_completion_queue_empty_error(&self, operation_type: IoRingOperationType) -> FlashQError {
        let storage_error = if operation_type.is_read_operation() {
            StorageError::ReadFailed {
                context: "io_uring completion queue empty".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    "No completion entry".to_string(),
                )),
            }
        } else {
            StorageError::WriteFailed {
                context: "io_uring completion queue empty".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    "No completion entry".to_string(),
                )),
            }
        };
        FlashQError::Storage(storage_error)
    }

    fn create_operation_failed_error(&self, operation_type: IoRingOperationType, result: i32) -> FlashQError {
        let storage_error = if operation_type.is_read_operation() {
            StorageError::ReadFailed {
                context: format!("io_uring {} failed with code: {result}", operation_type.as_str()),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {result}"
                ))),
            }
        } else {
            StorageError::WriteFailed {
                context: format!("io_uring {} failed with code: {result}", operation_type.as_str()),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {result}"
                ))),
            }
        };
        FlashQError::Storage(storage_error)
    }

    fn write_at_offset_using_io_uring(
        &mut self,
        data: &[u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        let op_type = IoRingOperationType::Write;
        let operation = opcode::Write::new(
            io_uring::types::Fd(self.underlying_file.as_raw_fd()),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(op_type.user_data());

        let result = self.execute_io_uring_operation(operation, op_type)?;
        debug!("io_uring write completed: {result} bytes at offset {offset}");
        Ok(result as usize)
    }

    fn read_at_offset_using_io_uring(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        let op_type = IoRingOperationType::Read;
        let operation = opcode::Read::new(
            io_uring::types::Fd(self.underlying_file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(op_type.user_data());

        let result = self.execute_io_uring_operation(operation, op_type)?;
        debug!("io_uring read completed: {result} bytes at offset {offset}");
        Ok(result as usize)
    }

    fn append_data_using_io_uring(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        let current_file_size_for_append_offset = self.get_current_file_size_in_bytes()?;
        let op_type = IoRingOperationType::Append;

        let operation = opcode::Write::new(
            io_uring::types::Fd(self.underlying_file.as_raw_fd()),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(current_file_size_for_append_offset)
        .build()
        .user_data(op_type.user_data());

        let result = self.execute_io_uring_operation(operation, op_type)?;
        debug!("io_uring append completed: {result} bytes at end of file");
        Ok(result as usize)
    }

    fn sync_file_using_io_uring(&mut self) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Sync;
        let operation = opcode::Fsync::new(io_uring::types::Fd(self.underlying_file.as_raw_fd()))
            .build()
            .user_data(op_type.user_data());

        self.execute_io_uring_operation(operation, op_type)?;
        debug!("io_uring fsync completed successfully");
        Ok(())
    }
}

impl AsRawFd for UnifiedAsyncFileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.underlying_file.as_raw_fd()
    }
}

pub type AsyncFileHandle = UnifiedAsyncFileHandle;
pub type IoRingExecutor = IoRingExecutorWithRuntime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_availability_detection_unit() {
        let availability_status = IoRingExecutor::is_available_on_current_system();
        println!("io_uring available: {availability_status}");
    }

    #[test]
    fn test_type_aliases_work() {
        assert_eq!(
            std::any::type_name::<AsyncFileHandle>(),
            std::any::type_name::<UnifiedAsyncFileHandle>()
        );
        assert_eq!(
            std::any::type_name::<IoRingExecutor>(),
            std::any::type_name::<IoRingExecutorWithRuntime>()
        );
    }
}
