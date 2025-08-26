use crate::storage::r#trait::TopicLog;
use crate::{Record, RecordWithOffset};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// File-based topic log implementation
///
/// Stores records in append-only log files using a simple binary format:
/// - Record length (4 bytes, little-endian u32)
/// - Offset (8 bytes, little-endian u64)
/// - JSON payload (variable length)
///
/// Files are stored in {data_dir}/{topic_name}.log
pub struct FileTopicLog {
    file_path: PathBuf,
    file: File,
    next_offset: u64,
    record_count: usize,
    sync_mode: SyncMode,
}

/// Synchronization modes for writes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    /// No explicit sync (rely on OS)
    None,
    /// Sync after every write
    Immediate,
    /// Sync periodically (not implemented in MVP)
    Periodic,
}

impl FileTopicLog {
    /// Create a new FileTopicLog for the given topic with configurable data directory
    pub fn new<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
    ) -> Result<Self, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create data directory if it doesn't exist
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)?;
        }

        let file_path = data_dir.join(format!("{topic}.log"));

        // Open file in append mode, create if it doesn't exist
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&file_path)?;

        let mut log = FileTopicLog {
            file_path: file_path.clone(),
            file,
            next_offset: 0,
            record_count: 0,
            sync_mode,
        };

        // Recover existing records and rebuild offset counters
        log.recover_from_file()?;

        Ok(log)
    }

    /// Create a new FileTopicLog for the given topic using default data directory (./data)
    pub fn new_default(topic: &str, sync_mode: SyncMode) -> Result<Self, std::io::Error> {
        Self::new(topic, sync_mode, "./data")
    }

    /// Recover records from existing file and rebuild state
    fn recover_from_file(&mut self) -> Result<(), std::io::Error> {
        // Check if file exists - if not, start with empty state
        if !self.file_path.exists() {
            return Ok(());
        }

        // Reset file position to beginning for reading
        let mut read_file = File::open(&self.file_path)?;

        let mut buffer = Vec::new();
        let mut offset_counter = 0;
        let mut record_count = 0;

        // Read the entire file into buffer
        read_file.read_to_end(&mut buffer)?;

        let mut cursor = 0;

        while cursor + 12 <= buffer.len() {
            // Need at least 12 bytes for length + offset
            // Read length (4 bytes)
            let length_bytes = &buffer[cursor..cursor + 4];
            let length = u32::from_le_bytes([
                length_bytes[0],
                length_bytes[1],
                length_bytes[2],
                length_bytes[3],
            ]) as usize;
            cursor += 4;

            // Read offset (8 bytes)
            let offset_bytes = &buffer[cursor..cursor + 8];
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
            cursor += 8;

            // Check if we have enough bytes for the payload
            if cursor + length > buffer.len() {
                // Partial write detected - truncate the file at this point
                self.truncate_at_position((cursor - 12) as u64)?;
                break;
            }

            // Read JSON payload
            let json_bytes = &buffer[cursor..cursor + length];
            cursor += length;

            // Validate that JSON can be parsed (basic corruption check)
            match serde_json::from_slice::<Record>(json_bytes) {
                Ok(_record) => {
                    offset_counter = offset_counter.max(offset + 1);
                    record_count += 1;
                }
                Err(_) => {
                    // Corrupted record - truncate here and stop recovery
                    self.truncate_at_position((cursor - length - 12) as u64)?;
                    break;
                }
            }
        }

        self.next_offset = offset_counter;
        self.record_count = record_count;

        Ok(())
    }

    /// Truncate file at given position to handle partial writes
    fn truncate_at_position(&mut self, position: u64) -> Result<(), std::io::Error> {
        use std::fs::OpenOptions;

        let file = OpenOptions::new().write(true).open(&self.file_path)?;

        file.set_len(position)?;

        // Reopen the file in append mode
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.file_path)?;

        Ok(())
    }
}

impl TopicLog for FileTopicLog {
    fn append(&mut self, record: Record) -> u64 {
        let offset = self.next_offset;

        // Serialize record to JSON
        let json_payload = match serde_json::to_vec(&record) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Failed to serialize record: {e}");
                return offset; // Return current offset without incrementing on error
            }
        };

        // Write binary format: length(4) + offset(8) + json_payload
        let length = json_payload.len() as u32;
        let mut write_buffer = Vec::with_capacity(12 + json_payload.len());

        // Length (4 bytes, little-endian)
        write_buffer.extend_from_slice(&length.to_le_bytes());

        // Offset (8 bytes, little-endian)
        write_buffer.extend_from_slice(&offset.to_le_bytes());

        // JSON payload
        write_buffer.extend_from_slice(&json_payload);

        // Write to file
        if let Err(e) = self.file.write_all(&write_buffer) {
            eprintln!("Failed to write record to file: {e}");
            return offset; // Return current offset without incrementing on error
        }

        // Sync based on configuration
        if matches!(self.sync_mode, SyncMode::Immediate) {
            if let Err(e) = self.file.sync_all() {
                eprintln!("Failed to sync file: {e}");
                // Continue anyway - data is written, just not synced
            }
        }

        self.next_offset += 1;
        self.record_count += 1;
        offset
    }

    fn get_records_from_offset(&self, offset: u64, count: Option<usize>) -> Vec<RecordWithOffset> {
        let mut records = Vec::new();

        // Check if file exists - if not, return empty records
        if !self.file_path.exists() {
            return records;
        }

        // Open a new file handle for reading to avoid interfering with writes
        let read_file = match File::open(&self.file_path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to open file for reading: {e}");
                return records;
            }
        };

        let mut buffer = Vec::new();
        let mut read_file = read_file;
        if let Err(e) = read_file.read_to_end(&mut buffer) {
            eprintln!("Failed to read file: {e}");
            return records;
        }

        let mut cursor = 0;
        let target_count = count.unwrap_or(usize::MAX);
        let mut found_count = 0;

        while cursor + 12 <= buffer.len() && found_count < target_count {
            // Read length (4 bytes)
            let length_bytes = &buffer[cursor..cursor + 4];
            let length = u32::from_le_bytes([
                length_bytes[0],
                length_bytes[1],
                length_bytes[2],
                length_bytes[3],
            ]) as usize;
            cursor += 4;

            // Read offset (8 bytes)
            let offset_bytes = &buffer[cursor..cursor + 8];
            let record_offset = u64::from_le_bytes([
                offset_bytes[0],
                offset_bytes[1],
                offset_bytes[2],
                offset_bytes[3],
                offset_bytes[4],
                offset_bytes[5],
                offset_bytes[6],
                offset_bytes[7],
            ]);
            cursor += 8;

            // Check bounds
            if cursor + length > buffer.len() {
                break; // Partial record
            }

            // If this record's offset is >= the requested offset, include it
            if record_offset >= offset {
                let json_bytes = &buffer[cursor..cursor + length];

                match serde_json::from_slice::<Record>(json_bytes) {
                    Ok(record) => {
                        let record_with_offset =
                            RecordWithOffset::from_record(record, record_offset);
                        records.push(record_with_offset);
                        found_count += 1;
                    }
                    Err(e) => {
                        eprintln!("Failed to parse record at offset {record_offset}: {e}");
                        break; // Stop on corruption
                    }
                }
            }

            cursor += length;
        }

        records
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
    /// Manually sync the file to disk (for testing)
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.file.sync_all()
    }
}
