use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use log::{info, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::StorageError;
use crate::storage::{
    ConsumerOffsetStore, PartitionId,
    file::{SyncMode, common::ensure_directory_exists, file_io::FileIo},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetStoreData {
    group_id: String,
    offsets: HashMap<String, u64>,
}

pub struct FileConsumerOffsetStore {
    group_id: String,
    file_path: PathBuf,
    sync_mode: SyncMode,
    snapshots: RwLock<HashMap<(String, PartitionId), u64>>,
}

impl FileConsumerOffsetStore {
    #[tracing::instrument(level = "info", skip_all, fields(group_id = %group_id))]
    pub fn new<P: AsRef<Path>>(
        group_id: &str,
        sync_mode: SyncMode,
        data_dir: P,
    ) -> Result<Self, std::io::Error> {
        let file_path = Self::create_offset_store_file_path(data_dir, group_id)?;
        let snapshots = Self::load_snapshots_from_disk(&file_path)?;

        info!(
            "Loaded {} offset snapshots for consumer group: {}",
            snapshots.len(),
            group_id
        );

        let store = FileConsumerOffsetStore {
            group_id: group_id.to_string(),
            file_path,
            sync_mode,
            snapshots: RwLock::new(snapshots),
        };

        store.persist_to_disk()?;
        info!(
            "Successfully created file consumer offset store: {}",
            group_id
        );
        Ok(store)
    }

    pub fn new_default(group_id: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
        Self::new(group_id, sync_mode, "./data")
    }

    fn create_offset_store_file_path<P: AsRef<Path>>(
        data_dir: P,
        group_id: &str,
    ) -> Result<PathBuf, std::io::Error> {
        let consumer_groups_dir = data_dir.as_ref().join("consumer_groups");
        ensure_directory_exists(&consumer_groups_dir)?;
        Ok(consumer_groups_dir.join(format!("{group_id}.json")))
    }

    fn load_snapshots_from_disk(
        file_path: &Path,
    ) -> Result<HashMap<(String, PartitionId), u64>, std::io::Error> {
        if !file_path.exists() {
            return Ok(HashMap::new());
        }

        let contents = std::fs::read_to_string(file_path)?;
        if contents.trim().is_empty() {
            return Ok(HashMap::new());
        }

        Self::parse_snapshot_data(&contents, file_path)
    }

    fn parse_snapshot_data(
        contents: &str,
        file_path: &Path,
    ) -> Result<HashMap<(String, PartitionId), u64>, std::io::Error> {
        match serde_json::from_str::<OffsetStoreData>(contents) {
            Ok(data) => Ok(Self::convert_to_snapshot_format(data.offsets)),
            Err(e) => {
                warn!(
                    "Failed to parse offset store file {}: {}",
                    file_path.display(),
                    e
                );
                Ok(HashMap::new())
            }
        }
    }

    fn convert_to_snapshot_format(
        offsets: HashMap<String, u64>,
    ) -> HashMap<(String, PartitionId), u64> {
        offsets
            .into_iter()
            .filter_map(|(key, offset)| {
                Self::parse_snapshot_key(&key)
                    .map(|(topic, partition_id)| ((topic, partition_id), offset))
            })
            .collect()
    }

    fn parse_snapshot_key(key: &str) -> Option<(String, PartitionId)> {
        key.split_once("--").and_then(|(topic, partition_str)| {
            partition_str
                .parse::<u32>()
                .ok()
                .map(|partition_id| (topic.to_string(), PartitionId(partition_id)))
        })
    }

    fn persist_to_disk(&self) -> Result<(), std::io::Error> {
        let snapshots = self.snapshots.read();
        let serializable_data = Self::convert_to_serializable_format(&snapshots, &self.group_id)?;
        self.write_to_file(&serializable_data)
    }

    fn convert_to_serializable_format(
        snapshots: &HashMap<(String, PartitionId), u64>,
        group_id: &str,
    ) -> Result<String, std::io::Error> {
        let offsets: HashMap<String, u64> = snapshots
            .iter()
            .map(|((topic, partition_id), offset)| {
                (format!("{}--{}", topic, partition_id.0), *offset)
            })
            .collect();

        let data = OffsetStoreData {
            group_id: group_id.to_string(),
            offsets,
        };

        serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    fn write_to_file(&self, json_data: &str) -> Result<(), std::io::Error> {
        let mut file_handle = FileIo::create_with_write_truncate_permissions(&self.file_path)
            .map_err(std::io::Error::other)?;
        FileIo::write_data_at_offset(&mut file_handle, json_data.as_bytes(), 0)
            .map_err(std::io::Error::other)?;

        if self.sync_mode == SyncMode::Immediate {
            FileIo::synchronize_to_disk(&mut file_handle).map_err(std::io::Error::other)?;
        }

        Ok(())
    }

    fn get_current_offset(&self, topic: &str, partition_id: PartitionId) -> u64 {
        self.snapshots
            .read()
            .get(&(topic.to_string(), partition_id))
            .copied()
            .unwrap_or(0)
    }

    fn should_persist_offset(
        &self,
        topic: &str,
        partition_id: PartitionId,
        new_offset: u64,
    ) -> bool {
        new_offset >= self.get_current_offset(topic, partition_id)
    }

    fn update_snapshot_in_memory(&self, topic: String, partition_id: PartitionId, offset: u64) {
        self.snapshots.write().insert((topic, partition_id), offset);
    }
}

impl ConsumerOffsetStore for FileConsumerOffsetStore {
    fn load_snapshot(&self, topic: &str, partition_id: PartitionId) -> Result<u64, StorageError> {
        Ok(self.get_current_offset(topic, partition_id))
    }

    fn persist_snapshot(
        &self,
        topic: String,
        partition_id: PartitionId,
        offset: u64,
    ) -> Result<bool, StorageError> {
        if !self.should_persist_offset(&topic, partition_id, offset) {
            return Ok(false);
        }

        self.update_snapshot_in_memory(topic, partition_id, offset);
        self.persist_to_disk()
            .map_err(|e| StorageError::from_io_error(e, "Failed to persist offset snapshot"))?;

        Ok(true)
    }

    fn get_all_snapshots(&self) -> Result<HashMap<(String, PartitionId), u64>, StorageError> {
        Ok(self.snapshots.read().clone())
    }

    fn group_id(&self) -> &str {
        &self.group_id
    }
}
