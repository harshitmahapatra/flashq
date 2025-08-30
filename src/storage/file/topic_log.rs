use crate::error::StorageError;
use crate::storage::file::SyncMode;
use crate::storage::file::common::{
    ensure_directory_exists, open_file_for_append, read_file_contents, sync_file_if_needed,
};
use crate::storage::r#trait::TopicLog;
use crate::{Record, RecordWithOffset, warn};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

// ================================================================================================
// CONSTANTS & TYPES
// ================================================================================================

const LENGTH_SIZE: usize = 4;
const OFFSET_SIZE: usize = 8;
const RECORD_HEADER_SIZE: usize = LENGTH_SIZE + OFFSET_SIZE;
const STREAMING_READER_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone)]
struct RecordHeader {
    length: u32,
    offset: u64,
}

// ================================================================================================
// BINARY FORMAT UTILITIES
// ================================================================================================

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

// ================================================================================================
// FILE TOPIC LOG IMPLEMENTATION
// ================================================================================================

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
        };

        log.recover_from_file()?;
        Ok(log)
    }

    pub fn new_default(
        topic: &str,
        sync_mode: SyncMode,
        wal_commit_threshold: usize,
    ) -> Result<Self, std::io::Error> {
        Self::new(topic, sync_mode, "./data", wal_commit_threshold)
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

// Recovery Implementation
impl FileTopicLog {
    fn recover_from_file(&mut self) -> Result<(), std::io::Error> {
        self.recover_wal()?;

        if !self.file_path.exists() {
            return Ok(());
        }

        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::with_capacity(STREAMING_READER_BUFFER_SIZE, file);
        let (offset_counter, record_count) = self.process_recovery_records(&mut reader)?;

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

    fn process_recovery_records(
        &mut self,
        reader: &mut BufReader<File>,
    ) -> Result<(u64, usize), std::io::Error> {
        let mut offset_counter = 0;
        let mut record_count = 0;
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];

        loop {
            // Try to read header
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            // Parse header using existing function
            let Some(header) = read_record_header(&header_buf, 0) else {
                // Invalid header, truncate and break
                let current_pos = self.get_current_file_position(reader)?;
                self.truncate_at_position(current_pos - RECORD_HEADER_SIZE as u64)?;
                break;
            };

            // Read record data
            let length = header.length as usize;
            let mut record_buf = vec![0u8; length];

            match reader.read_exact(&mut record_buf) {
                Ok(()) => {}
                Err(_) => {
                    // Incomplete record, truncate and break
                    let current_pos = self.get_current_file_position(reader)?;
                    self.truncate_at_position(current_pos - RECORD_HEADER_SIZE as u64)?;
                    break;
                }
            }

            // Validate using existing function
            if self.validate_record_json(&record_buf) {
                offset_counter = offset_counter.max(header.offset + 1);
                record_count += 1;
            } else {
                // Invalid JSON, truncate and break
                let current_pos = self.get_current_file_position(reader)?;
                self.truncate_at_position(current_pos - length as u64 - RECORD_HEADER_SIZE as u64)?;
                break;
            }
        }

        Ok((offset_counter, record_count))
    }

    // Helper function to get current file position
    fn get_current_file_position(
        &self,
        reader: &mut BufReader<File>,
    ) -> Result<u64, std::io::Error> {
        use std::io::Seek;
        reader.stream_position()
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

// Write-Ahead Logging Implementation
impl FileTopicLog {
    fn write_record_atomically(&mut self, buffer: &[u8]) -> Result<(), StorageError> {
        self.wal_file
            .write_all(buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write record to WAL"))?;

        sync_file_if_needed(&self.wal_file, self.sync_mode)
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync WAL file"))?;

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

        // Sync WAL before commit
        self.wal_file.sync_all().map_err(|e| {
            StorageError::from_io_error(
                e,
                "Failed to sync WAL before 
  commit",
            )
        })?;

        // Get current main file position for rollback
        let original_size = self
            .file
            .metadata()
            .map_err(|e| StorageError::from_io_error(e, "Failed to get main file size"))?
            .len();

        // Attempt to append WAL to main file
        match self.append_wal_to_main(original_size) {
            Ok(()) => {
                self.clear_wal_file()?;
                Ok(())
            }
            Err(e) => {
                // Rollback main file on failure
                if let Err(rollback_err) = self.rollback_main_file(original_size) {
                    warn!("Failed to rollback main file: {rollback_err}");
                }
                Err(e)
            }
        }
    }

    fn append_wal_to_main(&mut self, _original_size: u64) -> Result<(), StorageError> {
        let wal_path = self.get_wal_path();

        let mut wal_reader = File::open(&wal_path).map_err(|e| {
            StorageError::from_io_error(
                e,
                "Failed to open WAL for 
  reading",
            )
        })?;

        // Use std::io::copy for efficient streaming
        std::io::copy(&mut wal_reader, &mut self.file).map_err(|e| {
            StorageError::from_io_error(
                e,
                "Failed to copy WAL to main 
  file",
            )
        })?;

        // Ensure data is written to disk
        self.file.sync_all().map_err(|e| {
            StorageError::from_io_error(
                e,
                "Failed to sync main file after WAL
   append",
            )
        })?;

        Ok(())
    }

    fn rollback_main_file(&mut self, original_size: u64) -> Result<(), StorageError> {
        // Truncate main file back to original size
        self.file
            .set_len(original_size)
            .map_err(|e| StorageError::from_io_error(e, "Failed to truncate main file"))?;

        // Sync the truncation
        self.file.sync_all().map_err(|e| {
            StorageError::from_io_error(
                e,
                "Failed to sync main file after 
  rollback",
            )
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
}

// Record Processing Implementation
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

    fn load_file_to_buffer(
        &self,
        file_path: PathBuf,
        file_name: &str,
    ) -> Result<Vec<u8>, StorageError> {
        if file_path.exists() {
            read_file_contents(&file_path).map_err(|e| {
                StorageError::from_io_error(e, &format!("Failed to read {file_name} file"))
            })
        } else {
            Ok(Vec::new())
        }
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

// ================================================================================================
// TOPIC LOG TRAIT IMPLEMENTATION
// ================================================================================================

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
        let wal_buffer = self.load_file_to_buffer(self.get_wal_path(), "WAL")?;

        if offset >= self.next_offset - self.wal_record_count as u64 {
            return self.extract_matching_records(&wal_buffer, offset, count);
        }

        let mut buffer = self.load_file_to_buffer(self.file_path.clone(), "main log")?;
        buffer.extend_from_slice(&wal_buffer);

        self.extract_matching_records(&buffer, offset, count)
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
