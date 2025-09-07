pub mod common;
pub mod consumer_group;
pub mod file_io;
pub mod index;
pub mod segment;
pub mod segment_manager;
pub mod std_io;
pub mod topic_log;

pub use common::SyncMode;
pub use consumer_group::FileConsumerGroup;
pub use file_io::FileIO;
pub use index::{IndexEntry, SparseIndex};
pub use segment::{IndexingConfig, LogSegment};
pub use segment_manager::SegmentManager;
pub use std_io::StdFileIO;
pub use topic_log::FileTopicLog;

/// Configuration enum for selecting file I/O backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileIOMode {
    /// Standard file I/O using std::fs operations
    Standard,
}

impl Default for FileIOMode {
    fn default() -> Self {
        // Use Standard I/O by default for better performance
        // io_uring can still be explicitly selected when needed
        Self::Standard
    }
}

impl std::fmt::Display for FileIOMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileIOMode::Standard => write!(f, "standard"),
        }
    }
}
