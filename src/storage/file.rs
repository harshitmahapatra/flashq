use crate::error::StorageError;
use crate::storage::r#trait::{ConsumerGroup, TopicLog};
use crate::{Record, RecordWithOffset, warn};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// Constants and Types

const LENGTH_SIZE: usize = 4;
const OFFSET_SIZE: usize = 8;
const RECORD_HEADER_SIZE: usize = LENGTH_SIZE + OFFSET_SIZE;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    None,
    Immediate,
    Periodic,
}

#[derive(Debug, Clone)]
struct RecordHeader {
    length: u32,
    offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerGroupData {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

// Binary Format Utilities

fn read_record_header(buffer: &[u8], cursor: usize) -> Option<RecordHeader> {
    if cursor + RECORD_HEADER_SIZE > buffer.len() {
        return None;
    }

    let length = u32::from_le_bytes(buffer[cursor..cursor + LENGTH_SIZE].try_into().ok()?);
    let offset = u64::from_le_bytes(
        buffer[cursor + LENGTH_SIZE..cursor + RECORD_HEADER_SIZE]
            .try_into()
            .ok()?,
    );

    Some(RecordHeader { length, offset })
}

fn read_record_header_from_buffer(buffer: &[u8; RECORD_HEADER_SIZE]) -> Option<RecordHeader> {
    let length = u32::from_le_bytes(buffer[0..LENGTH_SIZE].try_into().ok()?);
    let offset = u64::from_le_bytes(buffer[LENGTH_SIZE..RECORD_HEADER_SIZE].try_into().ok()?);

    Some(RecordHeader { length, offset })
}

fn write_record_header(buffer: &mut Vec<u8>, length: u32, offset: u64) {
    buffer.extend_from_slice(&length.to_le_bytes());
    buffer.extend_from_slice(&offset.to_le_bytes());
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

// File I/O Utilities

fn ensure_directory_exists<P: AsRef<Path>>(dir: P) -> Result<(), std::io::Error> {
    let dir = dir.as_ref();
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

fn open_file_for_append(file_path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(file_path)
}

fn sync_file_if_needed(file: &File, sync_mode: SyncMode) -> Result<(), std::io::Error> {
    if sync_mode == SyncMode::Immediate {
        file.sync_all()
    } else {
        Ok(())
    }
}

fn read_file_contents(file_path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}

// FileTopicLog Implementation

pub struct FileTopicLog {
    file_path: PathBuf,
    file: File,
    wal_file: File,
    next_offset: u64,
    record_count: usize,
    wal_record_count: usize,
    sync_mode: SyncMode,
    wal_commit_threshold: usize,

    memory_cache: LruCache<u64, RecordWithOffset>,
    offset_index: std::collections::BTreeMap<u64, u64>,
}

impl FileTopicLog {
    pub fn new<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        wal_commit_threshold: usize,
        cache_size: usize,
    ) -> Result<Self, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_directory_exists(&data_dir)?;

        let file_path = data_dir.join(format!("{topic}.log"));
        let wal_file_path = data_dir.join(format!("{topic}.wal"));

        let file = open_file_for_append(&file_path)?;
        let wal_file = open_file_for_append(&wal_file_path)?;

        let mut log = FileTopicLog {
            file_path: file_path.clone(),
            file,
            wal_file,
            next_offset: 0,
            record_count: 0,
            wal_record_count: 0,
            sync_mode,
            wal_commit_threshold,

            memory_cache: LruCache::new(
                std::num::NonZero::new(cache_size)
                    .unwrap_or_else(|| std::num::NonZero::new(1000).unwrap()),
            ),
            offset_index: std::collections::BTreeMap::new(),
        };

        log.recover_from_file()?;
        Ok(log)
    }

    pub fn new_default(
        topic: &str,
        sync_mode: SyncMode,
        wal_commit_threshold: usize,
    ) -> Result<Self, std::io::Error> {
        Self::new(topic, sync_mode, "./data", wal_commit_threshold, 1000)
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.wal_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL file"))?;

        self.file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync main file"))
    }
}

impl FileTopicLog {
    fn recover_from_file(&mut self) -> Result<(), std::io::Error> {
        self.recover_wal()?;

        if !self.file_path.exists() {
            return Ok(());
        }

        let buffer = read_file_contents(&self.file_path)?;
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
            self.wal_record_count = 1;
            self.commit_wal_to_main()
                .map_err(|_| std::io::Error::other("WAL recovery failed"))?;
        }
        Ok(())
    }

    fn process_recovery_records(&mut self, buffer: &[u8]) -> Result<(u64, usize), std::io::Error> {
        let mut cursor = 0;
        let mut offset_counter = 0;
        let mut record_count = 0;

        while cursor + RECORD_HEADER_SIZE <= buffer.len() {
            let record_start_position = cursor as u64;

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

                self.offset_index.insert(offset, record_start_position);
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
        self.file = open_file_for_append(&self.file_path)?;
        Ok(())
    }
}

impl FileTopicLog {
    fn write_record_atomically(&mut self, buffer: &[u8]) -> Result<(), StorageError> {
        use std::io::Seek;

        let current_wal_position = self
            .wal_file
            .stream_position()
            .map_err(|e| StorageError::from_io_error(e, "Failed to get WAL file position"))?;

        self.wal_file
            .write_all(buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write record to WAL"))?;

        sync_file_if_needed(&self.wal_file, self.sync_mode)
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL file"))?;

        let offset = self.next_offset;
        self.offset_index.insert(offset, current_wal_position);

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

    fn create_merged_temp_file(
        &mut self,
        wal_path: &Path,
        temp_path: &PathBuf,
    ) -> Result<(), StorageError> {
        let mut temp_file = File::create(temp_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to create temp file"))?;

        if self.file_path.exists() {
            let main_content = read_file_contents(&self.file_path)
                .map_err(|e| StorageError::from_io_error(e, "Failed to read main file"))?;
            temp_file.write_all(&main_content).map_err(|e| {
                StorageError::from_io_error(e, "Failed to write main content to temp")
            })?;
        }

        let wal_content = read_file_contents(wal_path)
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

        self.file = open_file_for_append(&self.file_path).map_err(|e| {
            StorageError::from_io_error(e, "Failed to reopen main file after commit")
        })?;

        Ok(())
    }

    fn clear_wal_file(&mut self) -> Result<(), StorageError> {
        let wal_path = self.get_wal_path();

        {
            let _truncate_file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&wal_path)
                .map_err(|e| StorageError::from_io_error(e, "Failed to truncate WAL file"))?;
        }

        self.wal_file = open_file_for_append(&wal_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to reopen WAL file"))?;

        self.wal_record_count = 0;
        Ok(())
    }

    fn get_wal_path(&self) -> PathBuf {
        self.file_path.with_extension("wal")
    }

    fn get_temp_path(&self) -> PathBuf {
        self.file_path.with_extension("tmp")
    }
}

impl FileTopicLog {
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

    fn get_records_from_cache_or_disk(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let mut results = self.try_cache_first(offset, count);

        if self.cache_satisfied_request(&results, count) {
            if let Some(requested_count) = count {
                results.truncate(requested_count);
            }
            return Ok(results);
        }

        let remaining_count = count.map(|requested| requested.saturating_sub(results.len()));
        if remaining_count == Some(0) {
            return Ok(results);
        }

        let disk_start_offset = offset + results.len() as u64;
        let mut disk_results = self.read_records_streaming(disk_start_offset, remaining_count)?;
        results.append(&mut disk_results);
        Ok(results)
    }

    fn try_cache_first(&self, offset: u64, count: Option<usize>) -> Vec<RecordWithOffset> {
        let mut results = Vec::new();
        let cache_capacity = self.memory_cache.cap().into();
        let cache_search_limit = count.unwrap_or(cache_capacity).min(cache_capacity);

        for i in 0..cache_search_limit {
            let current_offset = offset + i as u64;
            if let Some(cached_record) = self.memory_cache.peek(&current_offset) {
                results.push(cached_record.clone());
            } else {
                break;
            }
        }
        results
    }

    fn cache_satisfied_request(&self, results: &[RecordWithOffset], count: Option<usize>) -> bool {
        if let Some(requested_count) = count {
            results.len() >= requested_count
        } else {
            false
        }
    }

    fn read_records_streaming(
        &self,
        start_offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let mut results = Vec::new();
        let target_count = count.unwrap_or(usize::MAX);
        let mut current_offset = 0u64;

        // First, read from main file if it exists
        if self.file_path.exists() {
            let main_results = self.stream_records_from_file(
                &self.file_path,
                start_offset,
                target_count,
                current_offset,
            )?;

            // Update current_offset based on records found in main file
            if let Some(last_record) = main_results.last() {
                current_offset = last_record.offset + 1;
            }

            results.extend(main_results);
        }

        // If we need more records and haven't reached target, read from WAL
        if results.len() < target_count {
            let wal_path = self.get_wal_path();
            if wal_path.exists() {
                let remaining_count = target_count - results.len();
                let wal_results = self.stream_records_from_file(
                    &wal_path,
                    start_offset,
                    remaining_count,
                    current_offset,
                )?;
                results.extend(wal_results);
            }
        }

        // Sort by offset to maintain consistency (main + WAL may have overlapping offsets)
        results.sort_by_key(|r| r.offset);

        // Apply final count limit
        if let Some(requested_count) = count {
            results.truncate(requested_count);
        }

        Ok(results)
    }

    fn stream_records_from_file(
        &self,
        file_path: &Path,
        start_offset: u64,
        max_count: usize,
        _file_offset_base: u64,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = File::open(file_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open file for streaming"))?;

        self.seek_to_offset(&mut file, start_offset)?;

        let mut results = Vec::new();
        let mut header_buffer = [0u8; RECORD_HEADER_SIZE];

        loop {
            // Try to read record header
            match file.read_exact(&mut header_buffer) {
                Ok(_) => {
                    // Successfully read header
                    let Some(header) = read_record_header_from_buffer(&header_buffer) else {
                        // Invalid header, stop reading
                        break;
                    };

                    let record_length = header.length as usize;
                    let record_offset = header.offset;

                    if record_offset < start_offset {
                        file.seek(SeekFrom::Current(record_length as i64))
                            .map_err(|e| {
                                StorageError::from_io_error(e, "Failed to seek past record")
                            })?;
                        continue;
                    }

                    if results.len() >= max_count {
                        break;
                    }

                    let mut record_data = vec![0u8; record_length];
                    file.read_exact(&mut record_data).map_err(|e| {
                        StorageError::from_io_error(e, "Failed to read record data")
                    })?;

                    match parse_record_from_bytes(&record_data, record_offset) {
                        Ok(record_with_offset) => {
                            results.push(record_with_offset);
                        }
                        Err(e) => {
                            warn!("Warning: Failed to parse record at offset {record_offset}: {e}");
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    return Err(StorageError::from_io_error(
                        e,
                        "Failed to read record header",
                    ));
                }
            }
        }

        Ok(results)
    }

    fn populate_cache(&mut self, record_with_offset: RecordWithOffset) {
        // LruCache automatically handles eviction on insert
        self.memory_cache
            .put(record_with_offset.offset, record_with_offset);
    }

    fn seek_to_offset(&self, file: &mut File, target_offset: u64) -> Result<(), StorageError> {
        use std::io::{Seek, SeekFrom};

        let file_position =
            if let Some((&_, &position)) = self.offset_index.range(..=target_offset).last() {
                position
            } else {
                0
            };

        file.seek(SeekFrom::Start(file_position))
            .map_err(|e| StorageError::from_io_error(e, "Failed to seek to indexed position"))?;

        Ok(())
    }
}

// TopicLog Trait Implementation

impl TopicLog for FileTopicLog {
    fn append(&mut self, record: Record) -> Result<u64, StorageError> {
        let offset = self.next_offset;
        let json_payload = self.serialize_record(&record)?;
        let write_buffer = self.create_record_buffer(&json_payload, offset);

        self.write_record_atomically(&write_buffer)?;

        let record_with_offset = RecordWithOffset::from_record(record, offset);
        self.populate_cache(record_with_offset);

        self.next_offset += 1;
        self.record_count += 1;
        Ok(offset)
    }

    fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        self.get_records_from_cache_or_disk(offset, count)
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

impl Drop for FileTopicLog {
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

// FileConsumerGroup Implementation

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

// ConsumerGroup Trait Implementation

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

// Unit Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use std::collections::HashMap;

    #[test]
    fn test_record_header_creation() {
        let header = RecordHeader {
            length: 42,
            offset: 1337,
        };

        assert_eq!(header.length, 42);
        assert_eq!(header.offset, 1337);
    }

    #[test]
    fn test_write_record_header() {
        let mut buffer = Vec::new();
        let length = 256u32;
        let offset = 1024u64;

        write_record_header(&mut buffer, length, offset);

        assert_eq!(buffer.len(), RECORD_HEADER_SIZE);
        assert_eq!(buffer[0..4], length.to_le_bytes());
        assert_eq!(buffer[4..12], offset.to_le_bytes());
    }

    #[test]
    fn test_read_record_header_valid() {
        let mut buffer = Vec::new();
        let length = 512u32;
        let offset = 2048u64;
        write_record_header(&mut buffer, length, offset);

        let header = read_record_header(&buffer, 0).unwrap();

        assert_eq!(header.length, 512);
        assert_eq!(header.offset, 2048);
    }

    #[test]
    fn test_read_record_header_insufficient_data() {
        let buffer = vec![0u8; 5];

        let header = read_record_header(&buffer, 0);

        assert!(header.is_none());
    }

    #[test]
    fn test_read_record_header_cursor_beyond_buffer() {
        let buffer = vec![0u8; RECORD_HEADER_SIZE];

        let header = read_record_header(&buffer, RECORD_HEADER_SIZE);

        assert!(header.is_none());
    }

    #[test]
    fn test_parse_record_from_bytes_valid_record() {
        let record = Record::new(Some("key1".to_string()), "value1".to_string(), None);
        let json_bytes = serde_json::to_vec(&record).unwrap();
        let offset = 42;

        let result = parse_record_from_bytes(&json_bytes, offset).unwrap();

        assert_eq!(result.offset, 42);
        assert_eq!(result.record.key, Some("key1".to_string()));
        assert_eq!(result.record.value, "value1");
    }

    #[test]
    fn test_parse_record_from_bytes_invalid_json() {
        let invalid_json = b"invalid json data";
        let offset = 42;

        let result = parse_record_from_bytes(invalid_json, offset);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_record_from_bytes_with_headers() {
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());
        let record = Record::new(
            Some("key1".to_string()),
            "value1".to_string(),
            Some(headers),
        );
        let json_bytes = serde_json::to_vec(&record).unwrap();
        let offset = 100;

        let result = parse_record_from_bytes(&json_bytes, offset).unwrap();

        assert_eq!(result.offset, 100);
        assert_eq!(result.record.key, Some("key1".to_string()));
        assert_eq!(result.record.value, "value1");
        assert!(result.record.headers.is_some());
    }

    #[test]
    fn test_consumer_group_data_creation() {
        let mut offsets = HashMap::new();
        offsets.insert("topic1".to_string(), 42);
        offsets.insert("topic2".to_string(), 84);

        let data = ConsumerGroupData {
            group_id: "test_group".to_string(),
            topic_offsets: offsets.clone(),
        };

        assert_eq!(data.group_id, "test_group");
        assert_eq!(data.topic_offsets, offsets);
    }

    #[test]
    fn test_sync_mode_enum_values() {
        let none = SyncMode::None;
        let immediate = SyncMode::Immediate;
        let periodic = SyncMode::Periodic;

        assert_eq!(none, SyncMode::None);
        assert_eq!(immediate, SyncMode::Immediate);
        assert_eq!(periodic, SyncMode::Periodic);
    }

    #[test]
    fn test_read_record_header_from_buffer() {
        let mut buffer = [0u8; RECORD_HEADER_SIZE];
        let expected_length = 256u32;
        let expected_offset = 1024u64;

        // Manually construct the header buffer
        buffer[0..4].copy_from_slice(&expected_length.to_le_bytes());
        buffer[4..12].copy_from_slice(&expected_offset.to_le_bytes());

        let header = read_record_header_from_buffer(&buffer).unwrap();

        assert_eq!(header.length, expected_length);
        assert_eq!(header.offset, expected_offset);
    }

    #[test]
    fn test_read_record_header_from_buffer_invalid() {
        // Test with invalid buffer content that might cause parsing issues
        let buffer = [0xFFu8; RECORD_HEADER_SIZE];

        // Should still parse (even if nonsensical values)
        let header = read_record_header_from_buffer(&buffer);
        assert!(header.is_some()); // Binary format should always parse if buffer size is correct
    }

    #[test]
    fn test_sync_mode_copy_clone() {
        let original = SyncMode::Immediate;
        let copied = original;
        let cloned = original;

        assert_eq!(original, copied);
        assert_eq!(original, cloned);
    }
}
