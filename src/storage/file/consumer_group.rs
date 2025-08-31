use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use log::warn;
use serde::{Deserialize, Serialize};

use crate::storage::{
    ConsumerGroup,
    file::{
        SyncMode,
        common::{ensure_directory_exists, sync_file_if_needed},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerGroupData {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

pub struct FileConsumerGroup {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
    file_path: PathBuf,
    sync_mode: SyncMode,
}

impl FileConsumerGroup {
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
        };

        consumer_group.persist_to_disk()?;
        Ok(consumer_group)
    }

    pub fn new_default(group_id: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
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

    fn load_existing_offsets(file_path: &PathBuf) -> Result<HashMap<String, u64>, std::io::Error> {
        if !file_path.exists() {
            return Ok(HashMap::new());
        }

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

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;

        file.write_all(json_data.as_bytes())?;
        sync_file_if_needed(&file, self.sync_mode)?;

        Ok(())
    }
}

// ================================================================================================
// CONSUMER GROUP TRAIT IMPLEMENTATION
// ================================================================================================

impl ConsumerGroup for FileConsumerGroup {
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
