use std::{
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use log::warn;
use serde::{Deserialize, Serialize};

use crate::storage::{
    ConsumerGroup,
    file::{
        SyncMode,
        common::ensure_directory_exists,
        file_io::FileIO,
        std_io::StdFileIO,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerGroupData {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

pub struct FileConsumerGroup<F: FileIO = StdFileIO> {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
    file_path: PathBuf,
    sync_mode: SyncMode,
    _phantom: PhantomData<F>,
}

impl<F: FileIO> FileConsumerGroup<F> {
    pub fn new<P: AsRef<Path>>(
        group_id: &str,
        sync_mode: SyncMode,
        data_dir: P,
    ) -> Result<Self, std::io::Error> {
        let file_path = Self::setup_consumer_group_file(data_dir, group_id)?;
        let topic_offsets = Self::load_existing_offsets(&file_path)?;

        let consumer_group = FileConsumerGroup {
            group_id: group_id.to_string(),
            topic_offsets,
            file_path,
            sync_mode,
            _phantom: PhantomData,
        };

        consumer_group.persist_to_disk()?;
        Ok(consumer_group)
    }

    pub fn new_default(
        group_id: &str,
        sync_mode: SyncMode,
    ) -> Result<Self, std::io::Error> {
        Self::new(group_id, sync_mode, "./data")
    }

    fn setup_consumer_group_file<P: AsRef<Path>>(
        data_dir: P,
        group_id: &str,
    ) -> Result<PathBuf, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let consumer_groups_dir = data_dir.join("consumer_groups");
        ensure_directory_exists(&consumer_groups_dir)?;
        Ok(consumer_groups_dir.join(format!("{group_id}.json")))
    }

    fn load_existing_offsets(
        file_path: &PathBuf,
    ) -> Result<HashMap<String, u64>, std::io::Error> {
        if !file_path.exists() {
            return Ok(HashMap::new());
        }

        // Use standard file operations for reading consumer group state
        let contents = std::fs::read_to_string(file_path)?;

        if contents.trim().is_empty() {
            return Ok(HashMap::new());
        }

        match serde_json::from_str::<ConsumerGroupData>(&contents) {
            Ok(data) => Ok(data.topic_offsets),
            Err(e) => {
                warn!(
                    "Failed to parse consumer group file {}: {}",
                    file_path.display(),
                    e
                );
                Ok(HashMap::new())
            }
        }
    }

    fn persist_to_disk(&self) -> Result<(), std::io::Error> {
        let data = ConsumerGroupData {
            group_id: self.group_id.clone(),
            topic_offsets: self.topic_offsets.clone(),
        };

        let json_data = serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut file_handle = F::create_with_write_truncate_permissions(&self.file_path)
            .map_err(std::io::Error::other)?;
        F::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0)
            .map_err(std::io::Error::other)?;

        if self.sync_mode == SyncMode::Immediate {
            F::synchronize_to_disk(&mut file_handle)
                .map_err(std::io::Error::other)?;
        }

        Ok(())
    }
}

// ================================================================================================
// CONSUMER GROUP TRAIT IMPLEMENTATION
// ================================================================================================

impl<F: FileIO> ConsumerGroup for FileConsumerGroup<F> {
    fn get_offset(&self, topic: &str) -> u64 {
        self.topic_offsets.get(topic).copied().unwrap_or(0)
    }

    fn set_offset(&mut self, topic: String, offset: u64) {
        self.topic_offsets.insert(topic, offset);

        if let Err(e) = self.persist_to_disk() {
            warn!("Failed to persist consumer group state: {e}");
        }
    }

    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn get_all_offsets(&self) -> HashMap<String, u64> {
        self.topic_offsets.clone()
    }
}
