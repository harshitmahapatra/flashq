pub mod common;
pub mod consumer_group;
pub mod file_io;
pub mod index;
pub mod segment;
pub mod segment_manager;
pub mod time_index;
pub mod topic_log;

pub use common::SyncMode;
pub use consumer_group::FileConsumerGroup;
pub use topic_log::FileTopicLog;

#[derive(Debug, Clone)]
pub struct IndexingConfig {
    pub time_seek_back_bytes: u32,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            time_seek_back_bytes: 1 * 1024 * 1024, // 1 MiB by default
        }
    }
}

