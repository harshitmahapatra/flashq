use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};

use crate::storage::{
    ConsumerGroup, PartitionId,
    file::{SyncMode, common::ensure_directory_exists, file_io::FileIo},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerGroupData {
    group_id: String,
    partition_offsets: HashMap<String, u64>, // Format: "topic--partition_id" -> offset
}

pub struct FileConsumerGroup {
    group_id: String,
    partition_offsets: HashMap<(String, PartitionId), u64>,
    file_path: PathBuf,
    sync_mode: SyncMode,
}

impl FileConsumerGroup {
    #[tracing::instrument(level = "info", skip_all, fields(group_id = %group_id))]
    pub fn new<P: AsRef<Path>>(
        group_id: &str,
        sync_mode: SyncMode,
        data_dir: P,
    ) -> Result<Self, std::io::Error> {
        debug!("Creating consumer group: {}", group_id);

        let file_path = Self::setup_consumer_group_file(data_dir, group_id)?;
        let partition_offsets = Self::load_existing_offsets(&file_path)?;

        info!(
            "Loaded {} partition offsets for consumer group: {}",
            partition_offsets.len(),
            group_id
        );

        let consumer_group = FileConsumerGroup {
            group_id: group_id.to_string(),
            partition_offsets,
            file_path,
            sync_mode,
        };

        consumer_group.persist_to_disk()?;
        info!("Successfully created consumer group: {}", group_id);
        Ok(consumer_group)
    }

    pub fn new_default(group_id: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
        Self::new(group_id, sync_mode, "./data")
    }

    #[tracing::instrument(level = "debug", skip_all, fields(group_id = %group_id))]
    fn setup_consumer_group_file<P: AsRef<Path>>(
        data_dir: P,
        group_id: &str,
    ) -> Result<PathBuf, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let consumer_groups_dir = data_dir.join("consumer_groups");

        debug!(
            "Setting up consumer group directory: {}",
            consumer_groups_dir.display()
        );

        ensure_directory_exists(&consumer_groups_dir)?;
        let file_path = consumer_groups_dir.join(format!("{group_id}.json"));

        debug!(
            "Consumer group file path for {}: {}",
            group_id,
            file_path.display()
        );

        Ok(file_path)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(file_path = %file_path.display()))]
    fn load_existing_offsets(
        file_path: &PathBuf,
    ) -> Result<HashMap<(String, PartitionId), u64>, std::io::Error> {
        if !file_path.exists() {
            debug!("Consumer group file does not exist, starting with empty offsets");
            return Ok(HashMap::new());
        }

        debug!("Loading existing offsets from: {}", file_path.display());

        // Use standard file operations for reading consumer group state
        let contents = std::fs::read_to_string(file_path)?;

        if contents.trim().is_empty() {
            debug!("Consumer group file is empty, starting with empty offsets");
            return Ok(HashMap::new());
        }

        match serde_json::from_str::<ConsumerGroupData>(&contents) {
            Ok(data) => {
                debug!(
                    "Successfully parsed consumer group data with {} entries",
                    data.partition_offsets.len()
                );

                // Convert from string-based keys to tuple-based keys
                let mut partition_offsets = HashMap::new();
                for (key, offset) in data.partition_offsets {
                    if let Some((topic, partition_str)) = key.split_once("--") {
                        if let Ok(partition_id) = partition_str.parse::<u32>() {
                            trace!(
                                "Loaded offset for topic:{topic} partition:{partition_id} offset:{offset}"
                            );
                            partition_offsets
                                .insert((topic.to_string(), PartitionId(partition_id)), offset);
                        } else {
                            warn!("Invalid partition ID in key: {key}");
                        }
                    } else {
                        warn!("Invalid key format (expected topic--partition): {key}");
                    }
                }

                info!("Loaded {} partition offsets", partition_offsets.len());
                Ok(partition_offsets)
            }
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

    #[tracing::instrument(level = "debug", skip(self), fields(group_id = %self.group_id, offsets_count = self.partition_offsets.len()))]
    fn persist_to_disk(&self) -> Result<(), std::io::Error> {
        debug!(
            "Persisting consumer group {} with {} partition offsets to disk",
            self.group_id,
            self.partition_offsets.len()
        );

        // Convert from tuple-based keys to string-based keys for serialization
        let mut serializable_offsets = HashMap::new();
        for ((topic, partition_id), offset) in &self.partition_offsets {
            let key = format!("{}--{}", topic, partition_id.0);
            trace!(
                "Serializing offset for topic:{} partition:{} offset:{}",
                topic, partition_id.0, offset
            );
            serializable_offsets.insert(key, *offset);
        }

        let data = ConsumerGroupData {
            group_id: self.group_id.clone(),
            partition_offsets: serializable_offsets,
        };

        let json_data = serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        debug!(
            "Writing {} bytes to consumer group file: {}",
            json_data.len(),
            self.file_path.display()
        );

        let mut file_handle = FileIo::create_with_write_truncate_permissions(&self.file_path)
            .map_err(std::io::Error::other)?;
        FileIo::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0)
            .map_err(std::io::Error::other)?;

        if self.sync_mode == SyncMode::Immediate {
            debug!("Synchronizing consumer group file to disk");
            FileIo::synchronize_to_disk(&mut file_handle).map_err(std::io::Error::other)?;
        }

        debug!("Successfully persisted consumer group: {}", self.group_id);
        Ok(())
    }
}

// ================================================================================================
// CONSUMER GROUP TRAIT IMPLEMENTATION
// ================================================================================================

impl ConsumerGroup for FileConsumerGroup {
    #[tracing::instrument(level = "trace", skip(self), fields(group_id = %self.group_id, topic = %topic, partition = %partition_id.0))]
    fn get_offset_partition(&self, topic: &str, partition_id: PartitionId) -> u64 {
        let offset = self
            .partition_offsets
            .get(&(topic.to_string(), partition_id))
            .copied()
            .unwrap_or(0);

        trace!(
            "Retrieved offset {} for topic:{} partition:{} in group:{}",
            offset, topic, partition_id.0, self.group_id
        );

        offset
    }

    #[tracing::instrument(level = "debug", skip(self), fields(group_id = %self.group_id, topic = %topic, partition = %partition_id.0, offset = %offset))]
    fn set_offset_partition(&mut self, topic: String, partition_id: PartitionId, offset: u64) {
        debug!(
            "Setting offset {} for topic:{} partition:{} in group:{}",
            offset, topic, partition_id.0, self.group_id
        );

        self.partition_offsets.insert((topic, partition_id), offset);

        if let Err(e) = self.persist_to_disk() {
            warn!("Failed to persist consumer group state: {e}");
        } else {
            debug!("Successfully updated offset and persisted to disk");
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(group_id = %self.group_id))]
    fn get_all_offsets_partitioned(&self) -> HashMap<(String, PartitionId), u64> {
        trace!(
            "Retrieving all {} partition offsets for group:{}",
            self.partition_offsets.len(),
            self.group_id
        );

        self.partition_offsets.clone()
    }

    fn group_id(&self) -> &str {
        &self.group_id
    }
}
