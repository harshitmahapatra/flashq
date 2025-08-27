use crate::error::StorageError;
use crate::storage::{ConsumerGroup, InMemoryConsumerGroup, InMemoryTopicLog, TopicLog};
use fs4::fs_std::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use sysinfo::{ProcessesToUpdate, System};

#[derive(Debug)]
pub enum StorageBackend {
    Memory,
    File {
        sync_mode: crate::storage::file::SyncMode,
        _directory_lock: std::fs::File, // Directory lock for "./data"
    },
    FileWithPath {
        sync_mode: crate::storage::file::SyncMode,
        data_dir: std::path::PathBuf,
        _directory_lock: std::fs::File, // Directory lock for custom path
    },
}

// Helper function to acquire directory lock
fn acquire_directory_lock<P: AsRef<Path>>(data_dir: P) -> Result<File, StorageError> {
    let data_dir = data_dir.as_ref();
    let lock_path = data_dir.join(".flashq.lock");

    // Create data directory if it doesn't exist
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| StorageError::from_io_error(e, "Failed to create data directory"))?;
    }

    // Try to create and lock the file
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(|e| StorageError::from_io_error(e, "Failed to create lock file"))?;

    // Try to acquire exclusive lock (non-blocking)
    match lock_file.try_lock_exclusive() {
        Ok(true) => {
            // Write current process info to lock file for diagnostics
            let pid = std::process::id();
            let timestamp = chrono::Utc::now().to_rfc3339();
            let lock_info = format!("PID: {pid}\nTimestamp: {timestamp}\n");

            if lock_file.set_len(0).is_err() {
                // Ignore errors - lock_info write is best effort
            }
            if (&lock_file).write_all(lock_info.as_bytes()).is_err() {
                // Ignore errors - lock_info write is best effort
            }

            Ok(lock_file)
        }
        Ok(false) | Err(_) => {
            // Try to read existing lock file to get PID info
            let existing_pid = std::fs::read_to_string(&lock_path)
                .ok()
                .and_then(|content| {
                    content
                        .lines()
                        .find(|line| line.starts_with("PID:"))
                        .and_then(|line| line.split_whitespace().nth(1))
                        .and_then(|pid_str| pid_str.parse::<u32>().ok())
                });

            // Check if the process is still running
            let pid = match existing_pid {
                Some(pid) => {
                    let mut system = System::new();
                    system.refresh_processes(ProcessesToUpdate::All, false);
                    if system
                        .processes()
                        .get(&sysinfo::Pid::from(pid as usize))
                        .is_some()
                    {
                        Some(pid)
                    } else {
                        // Stale lock - try to remove it and retry once
                        if std::fs::remove_file(&lock_path).is_ok() {
                            // Retry lock acquisition once for stale lock
                            return acquire_directory_lock(data_dir);
                        }
                        None
                    }
                }
                None => None,
            };

            Err(StorageError::DirectoryLocked {
                context: "Storage directory is already in use by another FlashQ instance"
                    .to_string(),
                pid,
            })
        }
    }
}
impl StorageBackend {
    pub fn new_memory() -> Self {
        StorageBackend::Memory
    }

    pub fn new_file(sync_mode: crate::storage::file::SyncMode) -> Result<Self, StorageError> {
        let data_dir = PathBuf::from("./data");
        let directory_lock = acquire_directory_lock(&data_dir)?;
        Ok(StorageBackend::File {
            sync_mode,
            _directory_lock: directory_lock,
        })
    }

    pub fn new_file_with_path<P: AsRef<Path>>(
        sync_mode: crate::storage::file::SyncMode,
        data_dir: P,
    ) -> Result<Self, StorageError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let directory_lock = acquire_directory_lock(&data_dir)?;
        Ok(StorageBackend::FileWithPath {
            sync_mode,
            data_dir,
            _directory_lock: directory_lock,
        })
    }

    pub fn create(&self, topic: &str) -> Result<Box<dyn TopicLog>, Box<dyn std::error::Error>> {
        match self {
            StorageBackend::Memory => Ok(Box::new(InMemoryTopicLog::new())),
            StorageBackend::File { sync_mode, .. } => {
                let file_log = crate::storage::file::FileTopicLog::new_default(topic, *sync_mode)?;
                Ok(Box::new(file_log))
            }
            StorageBackend::FileWithPath {
                sync_mode,
                data_dir,
                ..
            } => {
                let file_log =
                    crate::storage::file::FileTopicLog::new(topic, *sync_mode, data_dir)?;
                Ok(Box::new(file_log))
            }
        }
    }

    pub fn create_consumer_group(
        &self,
        group_id: &str,
    ) -> Result<Box<dyn ConsumerGroup>, Box<dyn std::error::Error>> {
        match self {
            StorageBackend::Memory => {
                Ok(Box::new(InMemoryConsumerGroup::new(group_id.to_string())))
            }
            StorageBackend::File { sync_mode, .. } => {
                let consumer_group =
                    crate::storage::file::FileConsumerGroup::new_default(group_id, *sync_mode)?;
                Ok(Box::new(consumer_group))
            }
            StorageBackend::FileWithPath {
                sync_mode,
                data_dir,
                ..
            } => {
                let consumer_group =
                    crate::storage::file::FileConsumerGroup::new(group_id, *sync_mode, data_dir)?;
                Ok(Box::new(consumer_group))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use std::collections::HashMap;

    #[test]
    fn test_storage_backend_memory() {
        let backend = StorageBackend::new_memory();
        let mut storage = backend.create("test_topic").unwrap();

        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
        assert_eq!(storage.next_offset(), 0);

        let record = Record::new(None, "test".to_string(), None);
        let offset = storage.append(record).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.next_offset(), 1);
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
