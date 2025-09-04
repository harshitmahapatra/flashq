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
const WRITE_OPERATION_USER_DATA: u64 = 0x01;
const READ_OPERATION_USER_DATA: u64 = 0x02;
const APPEND_OPERATION_USER_DATA: u64 = 0x03;
const SYNC_OPERATION_USER_DATA: u64 = 0x04;

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
        let canonical_file_path = path.as_ref().to_path_buf();
        let opened_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&canonical_file_path)
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("opening file {canonical_file_path:?}"),
                ))
            })?;

        let io_uring_availability = IoRingExecutorWithRuntime::is_available_on_current_system();

        debug!(
            "Created AsyncFileHandle for {canonical_file_path:?}, io_uring: {io_uring_availability}"
        );

        Ok(UnifiedAsyncFileHandle {
            underlying_file: opened_file,
            should_prefer_io_uring: io_uring_availability,
        })
    }

    pub fn create_with_write_truncate_permissions<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, FlashQError> {
        let canonical_file_path = path.as_ref().to_path_buf();
        let opened_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&canonical_file_path)
            .map_err(|e| {
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    &format!("opening file {canonical_file_path:?}"),
                ))
            })?;

        let io_uring_availability = IoRingExecutorWithRuntime::is_available_on_current_system();

        Ok(UnifiedAsyncFileHandle {
            underlying_file: opened_file,
            should_prefer_io_uring: io_uring_availability,
        })
    }

    pub fn open_with_read_only_permissions<P: AsRef<Path>>(path: P) -> Result<Self, FlashQError> {
        let canonical_file_path = path.as_ref().to_path_buf();
        let opened_file = File::open(&canonical_file_path).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                &format!("opening file {canonical_file_path:?}"),
            ))
        })?;

        let io_uring_availability = IoRingExecutorWithRuntime::is_available_on_current_system();

        Ok(UnifiedAsyncFileHandle {
            underlying_file: opened_file,
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

    fn write_at_offset_using_io_uring(
        &mut self,
        data: &[u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        let mut single_operation_ring =
            IoUring::new(SINGLE_OPERATION_RING_ENTRIES).map_err(|e| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring creation for write".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                })
            })?;

        let file_descriptor = self.underlying_file.as_raw_fd();

        let positional_write_operation = opcode::Write::new(
            io_uring::types::Fd(file_descriptor),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(WRITE_OPERATION_USER_DATA);

        unsafe {
            single_operation_ring
                .submission()
                .push(&positional_write_operation)
                .map_err(|e| {
                    FlashQError::Storage(StorageError::WriteFailed {
                        context: "io_uring submission push".to_string(),
                        source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                    })
                })?;
        }

        single_operation_ring.submit_and_wait(1).map_err(|e| {
            FlashQError::Storage(StorageError::WriteFailed {
                context: "io_uring submit_and_wait".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            })
        })?;

        let completion_queue_entry =
            single_operation_ring.completion().next().ok_or_else(|| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring completion queue empty".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(
                        "No completion entry".to_string(),
                    )),
                })
            })?;

        let operation_result = completion_queue_entry.result();
        if operation_result < 0 {
            return Err(FlashQError::Storage(StorageError::WriteFailed {
                context: format!("io_uring write failed with code: {operation_result}"),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {operation_result}"
                ))),
            }));
        }

        debug!(
            "io_uring write completed: {operation_result} bytes at offset {offset}"
        );
        Ok(operation_result as usize)
    }

    fn read_at_offset_using_io_uring(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize, FlashQError> {
        let mut single_operation_ring =
            IoUring::new(SINGLE_OPERATION_RING_ENTRIES).map_err(|e| {
                FlashQError::Storage(StorageError::ReadFailed {
                    context: "io_uring creation for read".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                })
            })?;

        let file_descriptor = self.underlying_file.as_raw_fd();

        let positional_read_operation = opcode::Read::new(
            io_uring::types::Fd(file_descriptor),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(READ_OPERATION_USER_DATA);

        unsafe {
            single_operation_ring
                .submission()
                .push(&positional_read_operation)
                .map_err(|e| {
                    FlashQError::Storage(StorageError::ReadFailed {
                        context: "io_uring submission push".to_string(),
                        source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                    })
                })?;
        }

        single_operation_ring.submit_and_wait(1).map_err(|e| {
            FlashQError::Storage(StorageError::ReadFailed {
                context: "io_uring submit_and_wait".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            })
        })?;

        let completion_queue_entry =
            single_operation_ring.completion().next().ok_or_else(|| {
                FlashQError::Storage(StorageError::ReadFailed {
                    context: "io_uring completion queue empty".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(
                        "No completion entry".to_string(),
                    )),
                })
            })?;

        let operation_result = completion_queue_entry.result();
        if operation_result < 0 {
            return Err(FlashQError::Storage(StorageError::ReadFailed {
                context: format!("io_uring read failed with code: {operation_result}"),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {operation_result}"
                ))),
            }));
        }

        debug!(
            "io_uring read completed: {operation_result} bytes at offset {offset}"
        );
        Ok(operation_result as usize)
    }

    fn append_data_using_io_uring(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        let mut single_operation_ring =
            IoUring::new(SINGLE_OPERATION_RING_ENTRIES).map_err(|e| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring creation for append".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                })
            })?;

        let file_descriptor = self.underlying_file.as_raw_fd();

        let current_file_size_for_append_offset = self.get_current_file_size_in_bytes()?;

        let append_write_operation = opcode::Write::new(
            io_uring::types::Fd(file_descriptor),
            data.as_ptr(),
            data.len() as u32,
        )
        .offset(current_file_size_for_append_offset)
        .build()
        .user_data(APPEND_OPERATION_USER_DATA);

        unsafe {
            single_operation_ring
                .submission()
                .push(&append_write_operation)
                .map_err(|e| {
                    FlashQError::Storage(StorageError::WriteFailed {
                        context: "io_uring submission push".to_string(),
                        source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                    })
                })?;
        }

        single_operation_ring.submit_and_wait(1).map_err(|e| {
            FlashQError::Storage(StorageError::WriteFailed {
                context: "io_uring submit_and_wait".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            })
        })?;

        let completion_queue_entry =
            single_operation_ring.completion().next().ok_or_else(|| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring completion queue empty".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(
                        "No completion entry".to_string(),
                    )),
                })
            })?;

        let operation_result = completion_queue_entry.result();
        if operation_result < 0 {
            return Err(FlashQError::Storage(StorageError::WriteFailed {
                context: format!("io_uring append failed with code: {operation_result}"),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {operation_result}"
                ))),
            }));
        }

        debug!(
            "io_uring append completed: {operation_result} bytes at end of file"
        );
        Ok(operation_result as usize)
    }

    fn sync_file_using_io_uring(&mut self) -> Result<(), FlashQError> {
        let mut single_operation_ring =
            IoUring::new(SINGLE_OPERATION_RING_ENTRIES).map_err(|e| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring creation for sync".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                })
            })?;

        let file_descriptor = self.underlying_file.as_raw_fd();

        let file_sync_operation = opcode::Fsync::new(io_uring::types::Fd(file_descriptor))
            .build()
            .user_data(SYNC_OPERATION_USER_DATA);

        unsafe {
            single_operation_ring
                .submission()
                .push(&file_sync_operation)
                .map_err(|e| {
                    FlashQError::Storage(StorageError::WriteFailed {
                        context: "io_uring submission push".to_string(),
                        source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
                    })
                })?;
        }

        single_operation_ring.submit_and_wait(1).map_err(|e| {
            FlashQError::Storage(StorageError::WriteFailed {
                context: "io_uring submit_and_wait".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            })
        })?;

        let completion_queue_entry =
            single_operation_ring.completion().next().ok_or_else(|| {
                FlashQError::Storage(StorageError::WriteFailed {
                    context: "io_uring completion queue empty".to_string(),
                    source: Box::new(crate::error::StorageErrorSource::Custom(
                        "No completion entry".to_string(),
                    )),
                })
            })?;

        let operation_result = completion_queue_entry.result();
        if operation_result < 0 {
            return Err(FlashQError::Storage(StorageError::WriteFailed {
                context: format!("io_uring fsync failed with code: {operation_result}"),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {operation_result}"
                ))),
            }));
        }

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
