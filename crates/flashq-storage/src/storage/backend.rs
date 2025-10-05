use crate::error::StorageError;
use crate::storage::file::{FileConsumerGroup, FileConsumerOffsetStore, FileTopicLog};
use crate::storage::{
    ConsumerGroup, ConsumerOffsetStore, InMemoryConsumerGroup, InMemoryConsumerOffsetStore,
    InMemoryTopicLog, TopicLog,
};
use fs4::fs_std::FileExt;
use log::{debug, warn};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use sysinfo::{ProcessesToUpdate, System};

#[derive(Debug)]
pub enum StorageBackend {
    Memory {
        batch_bytes: usize,
    },
    File {
        sync_mode: crate::storage::file::SyncMode,
        data_dir: std::path::PathBuf,
        wal_commit_threshold: usize,
        segment_size_bytes: u64,
        batch_bytes: usize,
        indexing_config: crate::storage::file::IndexingConfig,
        _directory_lock: File,
    },
}
impl Drop for StorageBackend {
    fn drop(&mut self) {
        if let StorageBackend::File { data_dir, .. } = self {
            let lock_path = data_dir.join(".flashq.lock");
            if lock_path.exists() {
                if let Err(e) = std::fs::remove_file(&lock_path) {
                    warn!("Failed to remove lock file {lock_path:?}: {e}");
                }
            }
        }
    }
}

impl StorageBackend {
    pub fn new_memory() -> Self {
        StorageBackend::Memory {
            batch_bytes: crate::storage::batching_heuristics::default_batch_bytes(),
        }
    }

    pub fn new_memory_with_batch_bytes(batch_bytes: usize) -> Self {
        StorageBackend::Memory { batch_bytes }
    }

    pub fn new_file(sync_mode: crate::storage::file::SyncMode) -> Result<Self, StorageError> {
        Self::new_file_with_path(sync_mode, "./data")
    }

    pub fn new_file_with_path<P: AsRef<Path>>(
        sync_mode: crate::storage::file::SyncMode,
        data_dir: P,
    ) -> Result<Self, StorageError> {
        const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
        Self::new_file_with_config(sync_mode, data_dir, 1000, DEFAULT_SEGMENT_SIZE)
    }

    pub fn new_file_with_config<P: AsRef<Path>>(
        sync_mode: crate::storage::file::SyncMode,
        data_dir: P,
        wal_commit_threshold: usize,
        segment_size_bytes: u64,
    ) -> Result<Self, StorageError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let directory_lock = acquire_directory_lock(&data_dir)?;
        Ok(StorageBackend::File {
            sync_mode,
            data_dir,
            wal_commit_threshold,
            segment_size_bytes,
            batch_bytes: crate::storage::batching_heuristics::default_batch_bytes(),
            indexing_config: crate::storage::file::IndexingConfig::default(),
            _directory_lock: directory_lock,
        })
    }

    pub fn new_file_with_path_and_batch_bytes<P: AsRef<Path>>(
        sync_mode: crate::storage::file::SyncMode,
        data_dir: P,
        batch_bytes: usize,
    ) -> Result<Self, StorageError> {
        const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
        let data_dir = data_dir.as_ref().to_path_buf();
        let directory_lock = acquire_directory_lock(&data_dir)?;
        Ok(StorageBackend::File {
            sync_mode,
            data_dir,
            wal_commit_threshold: 1000,
            segment_size_bytes: DEFAULT_SEGMENT_SIZE,
            batch_bytes,
            indexing_config: crate::storage::file::IndexingConfig::default(),
            _directory_lock: directory_lock,
        })
    }

    /// Override time-seek-back-bytes for file backend; no-op for memory backend.
    pub fn with_time_seek_back_bytes(mut self, bytes: u32) -> Self {
        if bytes == 0 {
            return self;
        }
        if let StorageBackend::File {
            indexing_config, ..
        } = &mut self
        {
            let mut cfg = indexing_config.clone();
            cfg.time_seek_back_bytes = bytes;
            *indexing_config = cfg;
        }
        self
    }

    pub fn create(
        &self,
        topic: &str,
    ) -> Result<Arc<RwLock<dyn TopicLog + Send + Sync>>, std::io::Error> {
        match self {
            StorageBackend::Memory { batch_bytes } => Ok(Arc::new(RwLock::new(
                InMemoryTopicLog::new_with_batch_bytes(*batch_bytes),
            ))),
            StorageBackend::File {
                sync_mode,
                data_dir,
                segment_size_bytes,
                batch_bytes,
                indexing_config,
                ..
            } => {
                let file_log = FileTopicLog::new_with_batch_bytes_and_indexing_config(
                    topic,
                    *sync_mode,
                    data_dir,
                    *segment_size_bytes,
                    *batch_bytes,
                    indexing_config.clone(),
                )?;
                Ok(Arc::new(RwLock::new(file_log)))
            }
        }
    }

    pub fn create_consumer_group(
        &self,
        group_id: &str,
    ) -> Result<Arc<RwLock<dyn ConsumerGroup>>, Box<dyn std::error::Error>> {
        match self {
            StorageBackend::Memory { .. } => Ok(Arc::new(RwLock::new(InMemoryConsumerGroup::new(
                group_id.to_string(),
            )))),
            StorageBackend::File {
                sync_mode,
                data_dir,
                ..
            } => {
                let consumer_group = FileConsumerGroup::new(group_id, *sync_mode, data_dir)?;
                Ok(Arc::new(RwLock::new(consumer_group)))
            }
        }
    }

    pub fn create_consumer_offset_store(
        &self,
        group_id: &str,
    ) -> Result<Arc<dyn ConsumerOffsetStore>, Box<dyn std::error::Error>> {
        match self {
            StorageBackend::Memory { .. } => Ok(Arc::new(InMemoryConsumerOffsetStore::new(
                group_id.to_string(),
            ))),
            StorageBackend::File {
                sync_mode,
                data_dir,
                ..
            } => {
                let offset_store = FileConsumerOffsetStore::new(group_id, *sync_mode, data_dir)?;
                Ok(Arc::new(offset_store))
            }
        }
    }

    /// Discover all existing topics in the storage backend
    #[tracing::instrument(level = "debug")]
    pub fn discover_topics(&self) -> Result<Vec<String>, std::io::Error> {
        match self {
            StorageBackend::Memory { .. } => {
                // Memory storage doesn't persist topics, so always empty
                Ok(Vec::new())
            }
            StorageBackend::File { data_dir, .. } => {
                let mut topics = Vec::new();
                tracing::debug!(?data_dir, "Checking data directory for existing topics");

                // Check if data directory exists
                if !data_dir.exists() {
                    tracing::debug!("Data directory does not exist, returning empty topics");
                    return Ok(topics);
                }

                // Read all entries in the data directory
                let topic_log_entries = std::fs::read_dir(data_dir)?;

                for topic_log_entry in topic_log_entries.flatten() {
                    let entry_path = topic_log_entry.path();
                    tracing::trace!(?entry_path, "Processing directory entry");

                    if topic_log_entry.file_type()?.is_dir() {
                        if let Some(topic_name) = topic_log_entry.file_name().to_str() {
                            // Skip system directories
                            if topic_name == "consumer_groups" || topic_name.starts_with('.') {
                                tracing::trace!(topic_name, "Skipping system directory");
                                continue;
                            }

                            tracing::debug!(topic_name, "Found potential topic directory");
                            let partition_dirs = std::fs::read_dir(topic_log_entry.path())?;
                            let mut found_partitions = false;

                            for partition_dir in partition_dirs {
                                let partition_path = partition_dir?.path();
                                tracing::trace!(?partition_path, "Checking partition directory");

                                if has_log_files(&partition_path)? {
                                    tracing::debug!(
                                        topic_name,
                                        ?partition_path,
                                        "Found partition with log files"
                                    );
                                    topics.push(topic_name.to_string());
                                    found_partitions = true;
                                    break; // Found at least one partition with log files
                                }
                            }

                            if !found_partitions {
                                tracing::debug!(topic_name, "No partitions with log files found");
                            }
                        }
                    }
                }

                tracing::info!(
                    topic_count = topics.len(),
                    ?topics,
                    "Topic discovery completed"
                );
                debug!("Found {} topics: {:?}", topics.len(), topics);
                Ok(topics)
            }
        }
    }
}

fn acquire_directory_lock<P: AsRef<Path>>(data_dir: P) -> Result<File, StorageError> {
    let data_dir = data_dir.as_ref();

    ensure_data_directory_exists(data_dir)?;
    let lock_path = data_dir.join(".flashq.lock");
    let lock_file = create_lock_file(&lock_path)?;

    match attempt_to_acquire_lock(&lock_file) {
        Ok(()) => {
            write_lock_metadata(&lock_file)?;
            Ok(lock_file)
        }
        Err(StorageError::LockAcquisitionFailed) => handle_lock_conflict(&lock_path, data_dir),
        Err(e) => Err(e),
    }
}
fn ensure_data_directory_exists(data_dir: &Path) -> Result<(), StorageError> {
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| StorageError::from_io_error(e, "Failed to create data directory"))?;
    }
    Ok(())
}

fn create_lock_file(lock_path: &Path) -> Result<File, StorageError> {
    if lock_path.exists() {
        OpenOptions::new()
            .write(true)
            .open(lock_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open existing lock file"))
    } else {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(lock_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to create lock file"))
    }
}

fn attempt_to_acquire_lock(lock_file: &File) -> Result<(), StorageError> {
    match lock_file.try_lock_exclusive() {
        Ok(true) => Ok(()),
        Ok(false) | Err(_) => Err(StorageError::LockAcquisitionFailed),
    }
}

fn write_lock_metadata(lock_file: &File) -> Result<(), StorageError> {
    let pid = std::process::id();
    let timestamp = chrono::Utc::now().to_rfc3339();
    let lock_info = format!("PID: {pid}\nTimestamp: {timestamp}\n");

    let _ = lock_file.set_len(0);
    (&*lock_file)
        .write_all(lock_info.as_bytes())
        .map_err(|e| StorageError::from_io_error(e, "Failed to write lock metadata"))
}

fn handle_lock_conflict<P: AsRef<Path>>(
    lock_path: &Path,
    data_dir: P,
) -> Result<File, StorageError> {
    let existing_pid = extract_pid_from_lock_file(lock_path);

    match existing_pid {
        Some(pid) if is_process_alive(pid) => Err(StorageError::DirectoryLocked {
            context: "Storage directory is already in use by another FlashQ instance".to_string(),
            pid: Some(pid),
        }),
        Some(_) | None => {
            if std::fs::remove_file(lock_path).is_ok() {
                acquire_directory_lock(data_dir)
            } else {
                Err(StorageError::DirectoryLocked {
                    context: "Storage directory is already in use by another FlashQ instance"
                        .to_string(),
                    pid: None,
                })
            }
        }
    }
}

fn extract_pid_from_lock_file(lock_path: &Path) -> Option<u32> {
    std::fs::read_to_string(lock_path).ok().and_then(|content| {
        content
            .lines()
            .find(|line| line.starts_with("PID:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|pid_str| pid_str.parse::<u32>().ok())
    })
}

fn is_process_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, false);
    system
        .processes()
        .get(&sysinfo::Pid::from(pid as usize))
        .is_some()
}

/// Check if a directory contains .log files (indicating it's a topic directory)
fn has_log_files(dir_path: &Path) -> Result<bool, std::io::Error> {
    let entries = std::fs::read_dir(dir_path)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".log") {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use std::collections::HashMap;

    #[test]
    fn test_storage_backend_memory() {
        let backend = StorageBackend::new_memory();
        let storage = backend.create("test_topic").unwrap();

        assert_eq!(storage.read().len(), 0);
        assert!(storage.read().is_empty());
        assert_eq!(storage.read().next_offset(), 0);

        let record = Record::new(None, "test".to_string(), None);
        let offset = storage.write().append(record).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(storage.read().len(), 1);
        assert_eq!(storage.read().next_offset(), 1);
    }

    #[test]
    fn test_memory_backend_creation() {
        // Test memory backend creation (no filesystem involved)
        let backend = StorageBackend::new_memory();
        let _storage = backend.create("test_topic").unwrap();
        let _consumer_group = backend.create_consumer_group("test_group").unwrap();
    }

    #[test]
    fn test_trait_object_usage() {
        let mut storage: Box<dyn TopicLog> = Box::new(InMemoryTopicLog::new());

        let record1 = Record::new(Some("key1".to_string()), "value1".to_string(), None);
        let record2 = Record::new(Some("key2".to_string()), "value2".to_string(), None);

        let offset1 = storage.append(record1).unwrap();
        let offset2 = storage.append(record2).unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(storage.len(), 2);

        let records = storage.get_records_from_offset(0, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record.value, "value1");
        assert_eq!(records[1].record.value, "value2");
    }

    #[test]
    fn test_storage_abstraction_with_headers() {
        let mut storage = InMemoryTopicLog::new();
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());
        headers.insert("priority".to_string(), "high".to_string());

        let record = Record::new(
            Some("user123".to_string()),
            "record with headers".to_string(),
            Some(headers.clone()),
        );

        let offset = storage.append(record).unwrap();
        assert_eq!(offset, 0);

        let records = storage.get_records_from_offset(0, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]
                .record
                .key
                .as_ref()
                .expect("record should have key"),
            "user123"
        );
        assert_eq!(records[0].record.value, "record with headers");
        assert_eq!(
            records[0]
                .record
                .headers
                .as_ref()
                .expect("record should have headers"),
            &headers
        );
    }
}
