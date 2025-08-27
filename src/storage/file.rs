use crate::error::StorageError;
use crate::storage::r#trait::{ConsumerGroup, TopicLog};
use crate::{Record, RecordWithOffset};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// Constants for binary record format
const LENGTH_SIZE: usize = 4;
const OFFSET_SIZE: usize = 8;
const RECORD_HEADER_SIZE: usize = LENGTH_SIZE + OFFSET_SIZE;

// Binary record format: length(4) + offset(8) + json_payload
#[derive(Debug, Clone)]
struct RecordHeader {
    length: u32,
    offset: u64,
}

// Helper functions for binary record format
fn read_record_header(buffer: &[u8], cursor: usize) -> Option<RecordHeader> {
    if cursor + RECORD_HEADER_SIZE > buffer.len() {
        return None;
    }

    let length_bytes = &buffer[cursor..cursor + LENGTH_SIZE];
    let length = u32::from_le_bytes([
        length_bytes[0],
        length_bytes[1],
        length_bytes[2],
        length_bytes[3],
    ]);

    let offset_bytes = &buffer[cursor + LENGTH_SIZE..cursor + RECORD_HEADER_SIZE];
    let offset = u64::from_le_bytes([
        offset_bytes[0],
        offset_bytes[1],
        offset_bytes[2],
        offset_bytes[3],
        offset_bytes[4],
        offset_bytes[5],
        offset_bytes[6],
        offset_bytes[7],
    ]);

    Some(RecordHeader { length, offset })
}

fn write_record_header(buffer: &mut Vec<u8>, length: u32, offset: u64) {
    buffer.extend_from_slice(&length.to_le_bytes());
    buffer.extend_from_slice(&offset.to_le_bytes());
}

fn ensure_directory_exists<P: AsRef<Path>>(dir: P) -> Result<(), std::io::Error> {
    let dir = dir.as_ref();
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

fn parse_record_from_bytes(
    json_bytes: &[u8],
    offset: u64,
) -> Result<RecordWithOffset, StorageError> {
    let record = serde_json::from_slice::<Record>(json_bytes).map_err(|e| {
        StorageError::from_serialization_error(e, &format!("record parsing at offset {offset}"))
    })?;
    Ok(RecordWithOffset::from_record(record, offset))
}

pub struct FileTopicLog {
    file_path: PathBuf,
    file: File,
    wal_file: File,
    next_offset: u64,
    record_count: usize,
    wal_record_count: usize,
    sync_mode: SyncMode,
    wal_commit_threshold: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    None,
    Immediate,
    Periodic,
}

impl FileTopicLog {
    pub fn new<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        wal_commit_threshold: usize,
    ) -> Result<Self, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_directory_exists(&data_dir)?;

        let file_path = data_dir.join(format!("{topic}.log"));
        let wal_file_path = data_dir.join(format!("{topic}.wal"));

        let file = Self::open_log_file(&file_path)?;
        let wal_file = Self::open_wal_file(&wal_file_path)?;

        let mut log = FileTopicLog {
            file_path: file_path.clone(),
            file,
            wal_file,
            next_offset: 0,
            record_count: 0,
            wal_record_count: 0,
            sync_mode,
            wal_commit_threshold,
        };

        log.recover_from_file()?;
        Ok(log)
    }

    fn open_log_file(file_path: &PathBuf) -> Result<File, std::io::Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_path)
    }

    fn open_wal_file(file_path: &PathBuf) -> Result<File, std::io::Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            
            .append(true)
            .open(file_path)
    }

    pub fn new_default(
        topic: &str,
        sync_mode: SyncMode,
        wal_commit_threshold: usize,
    ) -> Result<Self, std::io::Error> {
        Self::new(topic, sync_mode, "./data", wal_commit_threshold)
    }

    fn recover_from_file(&mut self) -> Result<(), std::io::Error> {
        // First commit any pending WAL entries
        self.recover_wal()?;

        if !self.file_path.exists() {
            return Ok(());
        }

        let buffer = self.read_entire_file()?;
        let (offset_counter, record_count) = self.process_recovery_records(&buffer)?;

        self.next_offset = offset_counter;
        self.record_count = record_count;
        Ok(())
    }

    fn recover_wal(&mut self) -> Result<(), std::io::Error> {
        let wal_path = self.get_wal_path();
        if !wal_path.exists() {
            return Ok(());
        }

        let wal_metadata = std::fs::metadata(&wal_path)?;

        if wal_metadata.len() > 0 {
            // Force commit any existing WAL content during recovery
            // regardless of record count threshold
            self.wal_record_count = 1; // Ensure non-zero to bypass empty check
            self.commit_wal_to_main().map_err(|_| {
                std::io::Error::other("WAL recovery failed")
            })?;
            // Don't update counters here - process_recovery_records() will count everything
        }
        Ok(())
    }

    fn read_entire_file(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut read_file = File::open(&self.file_path)?;
        let mut buffer = Vec::new();
        read_file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn process_recovery_records(&mut self, buffer: &[u8]) -> Result<(u64, usize), std::io::Error> {
        let mut cursor = 0;
        let mut offset_counter = 0;
        let mut record_count = 0;

        while cursor + RECORD_HEADER_SIZE <= buffer.len() {
            let Some(header) = read_record_header(buffer, cursor) else {
                break;
            };

            cursor += RECORD_HEADER_SIZE;
            let length = header.length as usize;
            let offset = header.offset;

            if cursor + length > buffer.len() {
                self.truncate_at_position((cursor - RECORD_HEADER_SIZE) as u64)?;
                break;
            }

            let json_bytes = &buffer[cursor..cursor + length];
            cursor += length;

            if self.validate_record_json(json_bytes) {
                offset_counter = offset_counter.max(offset + 1);
                record_count += 1;
            } else {
                self.truncate_at_position((cursor - length - RECORD_HEADER_SIZE) as u64)?;
                break;
            }
        }

        Ok((offset_counter, record_count))
    }

    fn validate_record_json(&self, json_bytes: &[u8]) -> bool {
        serde_json::from_slice::<Record>(json_bytes).is_ok()
    }

    fn truncate_at_position(&mut self, position: u64) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().write(true).open(&self.file_path)?;
        file.set_len(position)?;

        // Reopen the file in append mode
        self.file = Self::open_log_file(&self.file_path)?;
        Ok(())
    }

    fn serialize_record(&self, record: &Record) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(record)
            .map_err(|e| StorageError::from_serialization_error(e, "record serialization"))
    }

    fn create_record_buffer(&self, json_payload: &[u8], offset: u64) -> Vec<u8> {
        let length = json_payload.len() as u32;
        let mut write_buffer = Vec::with_capacity(RECORD_HEADER_SIZE + json_payload.len());

        write_record_header(&mut write_buffer, length, offset);
        write_buffer.extend_from_slice(json_payload);
        write_buffer
    }

    fn write_record_atomically(&mut self, buffer: &[u8]) -> Result<(), StorageError> {
        self.wal_file
            .write_all(buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write record to WAL"))?;

        // Use sync_mode to control fsync behavior instead of always flushing
        self.sync_if_needed()?;

        self.wal_record_count += 1;

        if self.wal_record_count >= self.wal_commit_threshold {
            self.commit_wal_to_main()?;
        }

        Ok(())
    }

    fn commit_wal_to_main(&mut self) -> Result<(), StorageError> {
        if self.wal_record_count == 0 {
            return Ok(());
        }

        // Always fsync WAL before merging to ensure durability
        self.wal_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL before commit"))?;

        let wal_path = self.get_wal_path();
        let temp_path = self.get_temp_path();

        self.create_merged_temp_file(&wal_path, &temp_path)?;
        self.atomic_replace_main_file(&temp_path)?;
        self.clear_wal_file()?;

        Ok(())
    }

    fn get_wal_path(&self) -> PathBuf {
        self.file_path.with_extension("wal")
    }

    fn get_temp_path(&self) -> PathBuf {
        self.file_path.with_extension("tmp")
    }

    fn create_merged_temp_file(
        &mut self,
        wal_path: &PathBuf,
        temp_path: &PathBuf,
    ) -> Result<(), StorageError> {
        let mut temp_file = File::create(temp_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to create temp file"))?;

        if self.file_path.exists() {
            let main_content = std::fs::read(&self.file_path)
                .map_err(|e| StorageError::from_io_error(e, "Failed to read main file"))?;
            temp_file.write_all(&main_content).map_err(|e| {
                StorageError::from_io_error(e, "Failed to write main content to temp")
            })?;
        }

        let wal_content = std::fs::read(wal_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read WAL file"))?;
        temp_file
            .write_all(&wal_content)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write WAL content to temp"))?;

        temp_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync temp file"))?;

        Ok(())
    }

    fn atomic_replace_main_file(&mut self, temp_path: &PathBuf) -> Result<(), StorageError> {
        std::fs::rename(temp_path, &self.file_path).map_err(|e| {
            StorageError::from_io_error(e, "Failed to atomically replace main file")
        })?;

        self.file = Self::open_log_file(&self.file_path).map_err(|e| {
            StorageError::from_io_error(e, "Failed to reopen main file after commit")
        })?;

        Ok(())
    }

    fn clear_wal_file(&mut self) -> Result<(), StorageError> {
        let wal_path = self.get_wal_path();

        // First truncate the WAL file to clear its contents
        {
            let _truncate_file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&wal_path)
                .map_err(|e| StorageError::from_io_error(e, "Failed to truncate WAL file"))?;
        }

        // Then reopen in append mode for future writes
        self.wal_file = Self::open_wal_file(&wal_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to reopen WAL file"))?;

        self.wal_record_count = 0;
        Ok(())
    }

    fn sync_if_needed(&mut self) -> Result<(), StorageError> {
        if matches!(self.sync_mode, SyncMode::Immediate) {
            self.wal_file
                .sync_all()
                .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL file"))
        } else {
            Ok(())
        }
    }

    fn read_file_for_query(&self) -> Result<Vec<u8>, StorageError> {
        let mut read_file = File::open(&self.file_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open file for reading"))?;

        let mut buffer = Vec::new();
        read_file
            .read_to_end(&mut buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read file"))?;
        Ok(buffer)
    }

    fn read_wal_for_query(&self) -> Result<Vec<u8>, StorageError> {
        let wal_path = self.get_wal_path();
        if !wal_path.exists() {
            return Ok(Vec::new());
        }

        let mut wal_file = File::open(&wal_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open WAL file for reading"))?;

        let mut buffer = Vec::new();
        wal_file
            .read_to_end(&mut buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read WAL file"))?;
        Ok(buffer)
    }

    fn extract_matching_records(
        &self,
        buffer: &[u8],
        start_offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let mut records = Vec::new();
        let mut cursor = 0;
        let target_count = count.unwrap_or(usize::MAX);
        let mut found_count = 0;

        while cursor + RECORD_HEADER_SIZE <= buffer.len() && found_count < target_count {
            let Some(header) = read_record_header(buffer, cursor) else {
                break;
            };

            cursor += RECORD_HEADER_SIZE;
            let length = header.length as usize;
            let record_offset = header.offset;

            if cursor + length > buffer.len() {
                return Err(StorageError::DataCorruption {
                    context: "file read".to_string(),
                    details: format!("Partial record detected at cursor {cursor}"),
                });
            }

            if record_offset >= start_offset {
                let json_bytes = &buffer[cursor..cursor + length];
                let record_with_offset = parse_record_from_bytes(json_bytes, record_offset)?;
                records.push(record_with_offset);
                found_count += 1;
            }

            cursor += length;
        }

        Ok(records)
    }
}

impl TopicLog for FileTopicLog {
    fn append(&mut self, record: Record) -> Result<u64, StorageError> {
        let offset = self.next_offset;
        let json_payload = self.serialize_record(&record)?;
        let write_buffer = self.create_record_buffer(&json_payload, offset);

        self.write_record_atomically(&write_buffer)?;

        self.next_offset += 1;
        self.record_count += 1;
        Ok(offset)
    }

    fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let mut combined_buffer = Vec::new();

        if self.file_path.exists() {
            let main_buffer = self.read_file_for_query()?;
            combined_buffer.extend_from_slice(&main_buffer);
        }

        let wal_buffer = self.read_wal_for_query()?;
        combined_buffer.extend_from_slice(&wal_buffer);

        self.extract_matching_records(&combined_buffer, offset, count)
    }

    fn len(&self) -> usize {
        self.record_count
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    fn next_offset(&self) -> u64 {
        self.next_offset
    }
}
impl FileTopicLog {
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.wal_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL file"))?;

        self.file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync main file"))
    }
}

impl Drop for FileTopicLog {
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

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
                eprintln!(
                    "Warning: Failed to parse consumer group file {}: {}",
                    file_path.display(),
                    e
                );
                Ok(HashMap::new())
            }
        }
    }

    pub fn new_default(group_id: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
        Self::new(group_id, sync_mode, "./data")
    }

    fn persist_to_disk(&self) -> Result<(), std::io::Error> {
        let json_data = self.serialize_consumer_group_data()?;
        let mut file = self.open_consumer_group_file()?;

        file.write_all(json_data.as_bytes())?;
        self.sync_file_if_needed(&file)?;

        Ok(())
    }

    fn serialize_consumer_group_data(&self) -> Result<String, std::io::Error> {
        let data = ConsumerGroupData {
            group_id: self.group_id.clone(),
            topic_offsets: self.topic_offsets.clone(),
        };

        serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    fn open_consumer_group_file(&self) -> Result<File, std::io::Error> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)
    }

    fn sync_file_if_needed(&self, file: &File) -> Result<(), std::io::Error> {
        if self.sync_mode == SyncMode::Immediate {
            file.sync_all()
        } else {
            Ok(())
        }
    }
}

impl ConsumerGroup for FileConsumerGroup {
    fn get_offset(&self, topic: &str) -> u64 {
        self.topic_offsets.get(topic).copied().unwrap_or(0)
    }

    fn set_offset(&mut self, topic: String, offset: u64) {
        self.topic_offsets.insert(topic, offset);

        // Persist to disk immediately on every change
        if let Err(e) = self.persist_to_disk() {
            eprintln!("Warning: Failed to persist consumer group state: {e}");
        }
    }

    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn get_all_offsets(&self) -> HashMap<String, u64> {
        self.topic_offsets.clone()
    }
}
