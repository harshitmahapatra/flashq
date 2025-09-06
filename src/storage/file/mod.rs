pub mod common;
pub mod consumer_group;
pub mod file_io;
pub mod index;
#[cfg(target_os = "linux")]
pub mod io_uring_io;
pub mod segment;
pub mod segment_manager;
pub mod std_io;
pub mod topic_log;

pub use common::SyncMode;
pub use consumer_group::FileConsumerGroup;
pub use file_io::FileIO;
pub use index::{IndexEntry, SparseIndex};
#[cfg(target_os = "linux")]
pub use io_uring_io::IoUringFileIO;
pub use segment::{IndexingConfig, LogSegment};
pub use segment_manager::SegmentManager;
pub use std_io::StdFileIO;
pub use topic_log::FileTopicLog;

/// Configuration enum for selecting file I/O backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileIOMode {
    /// Standard file I/O using std::fs operations
    Standard,
    /// High-performance I/O using Linux io_uring (Linux only)
    #[cfg(target_os = "linux")]
    IoUring,
}

impl Default for FileIOMode {
    fn default() -> Self {
        #[cfg(target_os = "linux")]
        {
            if IoUringFileIO::is_available() {
                Self::IoUring
            } else {
                Self::Standard
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::Standard
        }
    }
}

impl std::fmt::Display for FileIOMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileIOMode::Standard => write!(f, "standard"),
            #[cfg(target_os = "linux")]
            FileIOMode::IoUring => write!(f, "io_uring"),
        }
    }
}
