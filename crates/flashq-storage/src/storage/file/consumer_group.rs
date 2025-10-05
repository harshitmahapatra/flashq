use std::path::Path;
use std::sync::Arc;

use crate::storage::{
    ConsumerGroup, ConsumerOffsetStore,
    file::{FileConsumerOffsetStore, SyncMode},
};

pub struct FileConsumerGroup {
    offset_store: Arc<FileConsumerOffsetStore>,
}

impl FileConsumerGroup {
    pub fn new<P: AsRef<Path>>(
        group_id: &str,
        sync_mode: SyncMode,
        data_dir: P,
    ) -> Result<Self, std::io::Error> {
        let offset_store = FileConsumerOffsetStore::new(group_id, sync_mode, data_dir)?;
        Ok(FileConsumerGroup {
            offset_store: Arc::new(offset_store),
        })
    }

    pub fn new_default(group_id: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
        Self::new(group_id, sync_mode, "./data")
    }

    pub fn with_offset_store(offset_store: Arc<FileConsumerOffsetStore>) -> Self {
        FileConsumerGroup { offset_store }
    }
}

impl ConsumerGroup for FileConsumerGroup {
    fn offset_store(&self) -> &dyn ConsumerOffsetStore {
        self.offset_store.as_ref()
    }
}
