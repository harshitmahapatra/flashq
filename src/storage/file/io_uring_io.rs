use dashmap::DashMap;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use super::file_io::FileIO;
use crate::error::{FlashQError, StorageError};

use io_uring::{IoUring, opcode, types::Fd};
use log::{info, trace, warn};

static GLOBAL_IO_URING_AVAILABILITY_STATUS: OnceLock<bool> = OnceLock::new();

const DEFAULT_RING_ENTRIES: u32 = 256;
const NOP_OPERATION_USER_DATA: u64 = 1;

static NEXT_OPERATION_ID: AtomicUsize = AtomicUsize::new(2);

/// Shared io_uring instance with completion dispatcher
struct SharedRingManager {
    ring: Arc<Mutex<IoUring>>,
    pending_operations: Arc<DashMap<u64, mpsc::Sender<Result<i32, FlashQError>>>>,
    _completion_thread: thread::JoinHandle<()>,
}

struct OperationHandle {
    receiver: mpsc::Receiver<Result<i32, FlashQError>>,
}

impl OperationHandle {
    fn wait(self) -> Result<i32, FlashQError> {
        self.receiver.recv().map_err(|_| {
            FlashQError::Storage(StorageError::WriteFailed {
                context: "Operation channel closed unexpectedly".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    "Completion dispatcher thread terminated".to_string(),
                )),
            })
        })?
    }
}

impl SharedRingManager {
    fn new() -> Result<Arc<Self>, FlashQError> {
        let ring = IoUringFileIO::create_io_uring(IoUringFileIO::ring_entries()).map_err(|e| {
            FlashQError::Storage(StorageError::from_io_error(
                e,
                "Failed to create shared io_uring instance",
            ))
        })?;

        let ring = Arc::new(Mutex::new(ring));
        let pending_operations = Arc::new(DashMap::new());

        let completion_thread =
            Self::spawn_completion_thread(Arc::clone(&ring), Arc::clone(&pending_operations));

        let manager = Arc::new(SharedRingManager {
            ring,
            pending_operations,
            _completion_thread: completion_thread,
        });

        Ok(manager)
    }

    fn spawn_completion_thread(
        ring: Arc<Mutex<IoUring>>,
        pending_operations: Arc<DashMap<u64, mpsc::Sender<Result<i32, FlashQError>>>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || loop {
            let completions = SharedRingManager::drain_completions(&ring);
            if !completions.is_empty() {
                SharedRingManager::dispatch_completions(&pending_operations, completions);
                continue;
            }

            if !pending_operations.is_empty() {
                // Avoid deadlock: do not block with submit_and_wait while holding the ring lock,
                // because submitters also need the same lock to push SQEs.
                // Briefly yield when there is pending work but no completions yet.
                std::thread::park_timeout(std::time::Duration::from_micros(100));
                continue;
            }

            std::thread::park_timeout(std::time::Duration::from_millis(1));
        })
    }

    fn drain_completions(ring: &Arc<Mutex<IoUring>>) -> Vec<(u64, i32)> {
        let mut out = Vec::new();
        if let Ok(mut r) = ring.lock() {
            while let Some(cqe) = r.completion().next() {
                out.push((cqe.user_data(), cqe.result()));
            }
        }
        out
    }

    fn dispatch_completions(
        pending: &Arc<DashMap<u64, mpsc::Sender<Result<i32, FlashQError>>>>,
        completions: Vec<(u64, i32)>,
    ) {
        for (user_data, result) in completions {
            if let Some((_, sender)) = pending.remove(&user_data) {
                let mapped = SharedRingManager::map_cqe_result(result);
                let _ = sender.send(mapped);
            }
        }
    }

    fn map_cqe_result(result: i32) -> Result<i32, FlashQError> {
        if result < 0 {
            Err(FlashQError::Storage(StorageError::WriteFailed {
                context: format!("io_uring operation failed with code: {result}"),
                source: Box::new(crate::error::StorageErrorSource::Custom(format!(
                    "io_uring error code: {result}"
                ))),
            }))
        } else {
            Ok(result)
        }
    }

    fn submit_operation(
        &self,
        operation: io_uring::squeue::Entry,
    ) -> Result<OperationHandle, FlashQError> {
        let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed) as u64;
        let (sender, receiver) = mpsc::channel();

        // Register without external locking (DashMap is concurrent)
        self.pending_operations.insert(operation_id, sender);
        let operation_with_id = operation.user_data(operation_id);

        {
            let mut ring = self.ring.lock().unwrap();
            unsafe {
                ring.submission().push(&operation_with_id).map_err(|e| {
                    self.pending_operations.remove(&operation_id);
                    FlashQError::Storage(StorageError::from_io_error(
                        std::io::Error::other(e.to_string()),
                        "Failed to submit operation to shared ring",
                    ))
                })?;
            }

            // Submit without waiting; completion thread will handle results
            ring.submit().map_err(|e| {
                self.pending_operations.remove(&operation_id);
                FlashQError::Storage(StorageError::from_io_error(
                    e,
                    "Failed to submit operations to kernel",
                ))
            })?;
        }

        Ok(OperationHandle { receiver })
    }
}

fn get_shared_ring() -> Result<Arc<SharedRingManager>, FlashQError> {
    static INIT_RESULT: OnceLock<Result<Arc<SharedRingManager>, FlashQError>> = OnceLock::new();

    let result = INIT_RESULT.get_or_init(SharedRingManager::new);
    result.clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoRingOperationType {
    Read,
    Write,
    Sync,
}

pub struct IoUringFileHandle {
    file: std::fs::File,
    cached_size: AtomicU64,
    _shared_ring: Arc<SharedRingManager>,
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

    fn create_io_uring(entries: u32) -> Result<IoUring, std::io::Error> {
        // Try with cooperative task run for better performance (best-effort).
        // Avoid setup_single_issuer to allow submissions from multiple threads
        // when tests run in parallel.
        match io_uring::IoUring::builder()
            .setup_coop_taskrun()
            .build(entries)
        {
            Ok(ring) => {
                info!("io_uring created with cooperation flags enabled");
                Ok(ring)
            }
            Err(_) => match IoUring::new(entries) {
                Ok(ring) => {
                    info!("io_uring created with basic configuration");
                    Ok(ring)
                }
                Err(e) => Err(e),
            },
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
        let initial_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        let shared_ring = get_shared_ring()?;

        Ok(IoUringFileHandle {
            file,
            cached_size: AtomicU64::new(initial_size),
            _shared_ring: shared_ring,
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
                .build();

        let result = Self::execute_shared_operation(handle, operation, op_type)?;
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
        .build();

        let result = Self::execute_shared_operation(handle, operation, op_type)?;
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
        let current_size = handle.cached_size.load(Ordering::Relaxed);
        Self::write_data_at_offset(handle, data, current_size)?;
        handle
            .cached_size
            .store(current_size + data.len() as u64, Ordering::Relaxed);
        Ok(current_size)
    }

    fn synchronize_to_disk(handle: &mut Self::Handle) -> Result<(), FlashQError> {
        let op_type = IoRingOperationType::Sync;
        let operation = opcode::Fsync::new(Fd(handle.as_raw_fd())).build();

        let _result = Self::execute_shared_operation(handle, operation, op_type)?;
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

        handle.cached_size.store(actual_size, Ordering::Relaxed);

        Ok(actual_size)
    }
}

impl IoUringFileIO {
    fn execute_shared_operation(
        _handle: &mut IoUringFileHandle,
        operation: io_uring::squeue::Entry,
        _operation_type: IoRingOperationType,
    ) -> Result<i32, FlashQError> {
        let shared_ring = get_shared_ring()?;
        let op_handle = shared_ring.submit_operation(operation)?;
        op_handle.wait()
    }
}
