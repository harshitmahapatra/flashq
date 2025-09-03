use crate::error::{FlashQError, StorageError};
use io_uring::{opcode, IoUring};
use log::{debug, info, warn};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global io_uring availability detection result
static IO_URING_AVAILABLE: OnceLock<bool> = OnceLock::new();

/// IoRing executor with global singleton pattern and runtime detection
pub struct IoRingExecutor {
    ring: IoUring,
    runtime: Runtime,
}

impl IoRingExecutor {
    /// Check if io_uring is available on this system
    pub fn is_available() -> bool {
        *IO_URING_AVAILABLE.get_or_init(|| Self::detect_io_uring())
    }

    /// Detect if io_uring is available on this system
    fn detect_io_uring() -> bool {
        match IoUring::new(32) {
            Ok(mut ring) => {
                // Test with a no-op operation
                match Self::test_nop_operation(&mut ring) {
                    Ok(_) => {
                        info!("io_uring detected and functional");
                        true
                    }
                    Err(e) => {
                        warn!("io_uring detected but not functional: {}", e);
                        false
                    }
                }
            }
            Err(e) => {
                info!("io_uring not available: {}", e);
                false
            }
        }
    }

    /// Test io_uring functionality with a no-op operation
    fn test_nop_operation(ring: &mut IoUring) -> Result<(), Box<dyn std::error::Error>> {
        let nop_e = opcode::Nop::new().build().user_data(1);
        
        unsafe {
            ring.submission().push(&nop_e)?;
        }
        
        ring.submit()?;
        
        let cq = ring.completion();
        for cqe in cq {
            if cqe.user_data() == 1 {
                return Ok(());
            }
        }
        
        Err("No-op operation failed".into())
    }

    /// Try to create an IoRing executor
    fn try_create() -> Result<IoRingExecutor, FlashQError> {
        let ring = IoUring::new(256)
            .map_err(|e| FlashQError::Storage(StorageError::WriteFailed {
                context: "io_uring creation".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            }))?;

        let runtime = Runtime::new()
            .map_err(|e| FlashQError::Storage(StorageError::WriteFailed {
                context: "tokio runtime creation".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(e.to_string())),
            }))?;

        Ok(IoRingExecutor { ring, runtime })
    }

    /// Execute an async operation synchronously
    pub fn execute_sync<F, R>(&mut self, future: F) -> R
    where
        F: std::future::Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        self.runtime.block_on(future)
    }
}

/// Unified file handle abstraction supporting both io_uring and standard I/O
pub struct AsyncFileHandle {
    file: File,
    path: std::path::PathBuf,
    use_io_uring: bool,
}

impl AsyncFileHandle {
    /// Create a file handle with create/append/read permissions
    pub fn create_append_read<P: AsRef<Path>>(path: P) -> Result<Self, FlashQError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, &format!("opening file {:?}", path))))?;

        let use_io_uring = IoRingExecutor::is_available();

        debug!("Created AsyncFileHandle for {:?}, io_uring: {}", path, use_io_uring);

        Ok(AsyncFileHandle {
            file,
            path,
            use_io_uring,
        })
    }

    /// Create a file handle with create/write/truncate permissions (for consumer groups)
    pub fn create_write_truncate<P: AsRef<Path>>(path: P) -> Result<Self, FlashQError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, &format!("opening file {:?}", path))))?;

        let use_io_uring = IoRingExecutor::is_available();

        Ok(AsyncFileHandle {
            file,
            path,
            use_io_uring,
        })
    }

    /// Create a file handle with read-only permissions
    pub fn open_read<P: AsRef<Path>>(path: P) -> Result<Self, FlashQError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, &format!("opening file {:?}", path))))?;

        let use_io_uring = IoRingExecutor::is_available();

        Ok(AsyncFileHandle {
            file,
            path,
            use_io_uring,
        })
    }

    /// Write data at a specific offset (positional write)
    pub fn write_at(&mut self, data: &[u8], offset: u64) -> Result<usize, FlashQError> {
        if self.use_io_uring {
            self.write_at_io_uring(data, offset)
        } else {
            self.write_at_std(data, offset)
        }
    }

    /// Read data at a specific offset (positional read)
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize, FlashQError> {
        if self.use_io_uring {
            self.read_at_io_uring(buf, offset)
        } else {
            self.read_at_std(buf, offset)
        }
    }

    /// Append data to the end of the file
    pub fn append(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        if self.use_io_uring {
            self.append_io_uring(data)
        } else {
            self.append_std(data)
        }
    }

    /// Synchronize file to disk (fsync)
    pub fn sync(&mut self) -> Result<(), FlashQError> {
        if self.use_io_uring {
            self.sync_io_uring()
        } else {
            self.sync_std()
        }
    }

    /// Get current file position (for append operations)
    pub fn seek_end(&mut self) -> Result<u64, FlashQError> {
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "seeking to end")))
    }

    /// Get file size
    pub fn file_size(&mut self) -> Result<u64, FlashQError> {
        let metadata = self.file
            .metadata()
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "getting file metadata")))?;
        Ok(metadata.len())
    }

    /// Create a buffered reader for sequential reading
    pub fn buffered_reader(&mut self) -> BufReader<&mut File> {
        BufReader::new(&mut self.file)
    }

    // Standard I/O implementations
    fn write_at_std(&mut self, data: &[u8], offset: u64) -> Result<usize, FlashQError> {
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, &format!("seeking to offset {}", offset))))?;
        
        self.file.write_all(data)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "writing data")))?;
        
        Ok(data.len())
    }

    fn read_at_std(&mut self, buf: &mut [u8], offset: u64) -> Result<usize, FlashQError> {
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, &format!("seeking to offset {}", offset))))?;
        
        self.file.read(buf)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "reading data")))
    }

    fn append_std(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        self.file.seek(SeekFrom::End(0))
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "seeking to end")))?;
        
        self.file.write_all(data)
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "appending data")))?;
        
        Ok(data.len())
    }

    fn sync_std(&mut self) -> Result<(), FlashQError> {
        self.file.sync_all()
            .map_err(|e| FlashQError::Storage(StorageError::from_io_error(e, "syncing file")))
    }

    // io_uring implementations (placeholder - will be implemented)
    fn write_at_io_uring(&mut self, data: &[u8], offset: u64) -> Result<usize, FlashQError> {
        // TODO: Implement io_uring write
        // For now, fallback to standard I/O
        warn!("io_uring write not yet implemented, falling back to standard I/O");
        self.write_at_std(data, offset)
    }

    fn read_at_io_uring(&mut self, buf: &mut [u8], offset: u64) -> Result<usize, FlashQError> {
        // TODO: Implement io_uring read
        // For now, fallback to standard I/O
        warn!("io_uring read not yet implemented, falling back to standard I/O");
        self.read_at_std(buf, offset)
    }

    fn append_io_uring(&mut self, data: &[u8]) -> Result<usize, FlashQError> {
        // TODO: Implement io_uring append
        // For now, fallback to standard I/O
        warn!("io_uring append not yet implemented, falling back to standard I/O");
        self.append_std(data)
    }

    fn sync_io_uring(&mut self) -> Result<(), FlashQError> {
        // TODO: Implement io_uring fsync
        // For now, fallback to standard I/O
        warn!("io_uring sync not yet implemented, falling back to standard I/O");
        self.sync_std()
    }
}

impl AsRawFd for AsyncFileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_io_uring_detection() {
        // Test that detection doesn't panic
        let available = IoRingExecutor::detect_io_uring();
        println!("io_uring available: {}", available);
    }

    #[test]
    fn test_async_file_handle_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let test_file = temp_dir.path().join("test.log");

        // Test create_append_read
        let mut handle = AsyncFileHandle::create_append_read(&test_file).unwrap();

        // Test append
        let data = b"Hello, World!";
        let written = handle.append(data).unwrap();
        assert_eq!(written, data.len());

        // Test sync
        handle.sync().unwrap();

        // Test file size
        let size = handle.file_size().unwrap();
        assert_eq!(size, data.len() as u64);

        // Test read_at
        let mut buf = vec![0u8; data.len()];
        let read_count = handle.read_at(&mut buf, 0).unwrap();
        assert_eq!(read_count, data.len());
        assert_eq!(&buf[..read_count], data);
    }
}