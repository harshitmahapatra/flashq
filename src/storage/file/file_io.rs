use std::os::unix::io::AsRawFd;
use std::path::Path;
use crate::error::FlashQError;

/// Trait for file I/O operations that can be implemented by different backends
/// (standard I/O, io_uring, etc.) to provide a unified interface for file operations
/// used throughout the storage layer.
pub trait FileIO: Send + Sync {
    /// The file handle type used by this implementation
    type Handle: AsRawFd + Send + Sync;
    
    // File lifecycle operations
    
    /// Create a new file or open existing with append and read permissions
    fn create_with_append_and_read_permissions(path: &Path) -> Result<Self::Handle, FlashQError>;
    
    /// Create a new file or truncate existing with write permissions
    fn create_with_write_truncate_permissions(path: &Path) -> Result<Self::Handle, FlashQError>;
    
    /// Open an existing file with read-only permissions
    fn open_with_read_only_permissions(path: &Path) -> Result<Self::Handle, FlashQError>;
    
    // Core I/O operations
    
    /// Write data at a specific offset in the file
    fn write_data_at_offset(handle: &mut Self::Handle, data: &[u8], offset: u64) -> Result<(), FlashQError>;
    
    /// Read data from a specific offset in the file, filling the entire buffer
    fn read_data_at_offset(handle: &mut Self::Handle, buffer: &mut [u8], offset: u64) -> Result<(), FlashQError>;
    
    /// Append data to the end of the file, returning the position where data was written
    fn append_data_to_end(handle: &mut Self::Handle, data: &[u8]) -> Result<u64, FlashQError>;
    
    // File operations
    
    /// Synchronize file data and metadata to disk (equivalent to fsync)
    fn synchronize_to_disk(handle: &mut Self::Handle) -> Result<(), FlashQError>;
    
    /// Get the current size of the file in bytes
    fn get_file_size(handle: &Self::Handle) -> Result<u64, FlashQError>;
}