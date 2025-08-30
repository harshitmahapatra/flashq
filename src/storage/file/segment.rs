use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::error::StorageError;
use crate::storage::file::common::SyncMode;
use crate::storage::file::index::{IndexEntry, SparseIndex};
use crate::{Record, RecordWithOffset};

/// Configuration for sparse index creation
#[derive(Debug, Clone)]
pub struct IndexingConfig {
    /// Create index entry every N bytes (Kafka default: 4096)
    pub index_interval_bytes: u32,
    /// Create index entry every N records (fallback)
    pub index_interval_records: u32,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            index_interval_bytes: 4096,  // 4KB like Kafka
            index_interval_records: 100, // Every 100 records as fallback
        }
    }
}

/// A log segment with sparse indexing, following Kafka's architecture
pub struct LogSegment {
    /// First offset in this segment
    base_offset: u64,
    /// Last offset written to this segment
    max_offset: u64,
    /// Path to the .log file
    pub log_path: PathBuf,
    /// Path to the .index file  
    pub index_path: PathBuf,
    /// Log file handle
    log_file: File,
    /// Index file handle
    index_file: File,
    /// In-memory sparse index
    index: SparseIndex,
    /// Bytes written since last index entry
    bytes_since_last_index: u32,
    /// Records written since last index entry
    records_since_last_index: u32,
    /// Sync mode for file operations
    sync_mode: SyncMode,
    /// Indexing configuration
    indexing_config: IndexingConfig,
}

impl LogSegment {
    /// Create a new log segment
    pub fn new(
        base_offset: u64,
        log_path: PathBuf,
        index_path: PathBuf,
        sync_mode: SyncMode,
        indexing_config: IndexingConfig,
    ) -> Result<Self, StorageError> {
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open log file"))?;

        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&index_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open index file"))?;

        Ok(LogSegment {
            base_offset,
            max_offset: base_offset.saturating_sub(1), // No records yet
            log_path,
            index_path,
            log_file,
            index_file,
            index: SparseIndex::new(),
            bytes_since_last_index: 0,
            records_since_last_index: 0,
            sync_mode,
            indexing_config,
        })
    }

    /// Open an existing log segment and load its index
    pub fn open(
        base_offset: u64,
        log_path: PathBuf,
        index_path: PathBuf,
        sync_mode: SyncMode,
        indexing_config: IndexingConfig,
    ) -> Result<Self, StorageError> {
        let mut segment = Self::new(
            base_offset,
            log_path,
            index_path,
            sync_mode,
            indexing_config,
        )?;

        // Load existing index if it exists
        if segment.index_path.exists() {
            let index_file_copy = std::fs::File::open(&segment.index_path).map_err(|e| {
                StorageError::from_io_error(e, "Failed to open index file for reading")
            })?;
            let mut index_reader = BufReader::new(index_file_copy);
            segment
                .index
                .read_from_file(&mut index_reader, base_offset)?;
        }

        // Determine max_offset by seeking to end and scanning backwards if needed
        // For now, we'll use a simple approach - this could be optimized
        segment.determine_max_offset()?;

        Ok(segment)
    }

    /// Recover an existing log segment, loading its state from persisted files
    pub fn recover(
        base_offset: u64,
        log_path: PathBuf,
        index_path: PathBuf,
        sync_mode: SyncMode,
        indexing_config: IndexingConfig,
    ) -> Result<Self, StorageError> {
        // Use open method which handles loading existing index and determining max_offset
        Self::open(base_offset, log_path, index_path, sync_mode, indexing_config)
    }

    /// Append a record to this segment with sparse indexing
    pub fn append_record(&mut self, record: &Record, offset: u64) -> Result<(), StorageError> {
        // 1. Serialize and write record to log file
        let serialized = self.serialize_record(record, offset)?;
        let start_position = self
            .log_file
            .seek(SeekFrom::End(0))
            .map_err(|e| StorageError::from_io_error(e, "Failed to seek to end of log file"))?
            as u32;
        self.log_file
            .write_all(&serialized)
            .map_err(|e| StorageError::from_io_error(e, "Failed to write record to log file"))?;

        // 2. Update segment metadata
        self.max_offset = offset;
        let record_size = serialized.len() as u32;
        self.bytes_since_last_index += record_size;
        self.records_since_last_index += 1;

        // 3. Sparse index creation logic
        if self.should_add_index_entry() {
            let index_entry = IndexEntry::new(offset, start_position);

            // Add to in-memory index
            self.index.add_entry(index_entry.clone());

            // Persist to .index file
            let index_file_copy = std::fs::OpenOptions::new()
                .append(true)
                .open(&self.index_path)
                .map_err(|e| {
                    StorageError::from_io_error(e, "Failed to open index file for writing")
                })?;
            let mut index_writer = BufWriter::new(index_file_copy);
            self.index
                .write_entry_to_file(&mut index_writer, &index_entry, self.base_offset)?;
            index_writer
                .flush()
                .map_err(|e| StorageError::from_io_error(e, "Failed to flush index writer"))?;

            // Reset counters
            self.bytes_since_last_index = 0;
            self.records_since_last_index = 0;
        }

        // 4. Sync based on sync_mode
        if matches!(self.sync_mode, SyncMode::Immediate) {
            self.log_file
                .sync_all()
                .map_err(|e| StorageError::from_io_error(e, "Failed to sync log file"))?;
            self.index_file
                .sync_all()
                .map_err(|e| StorageError::from_io_error(e, "Failed to sync index file"))?;
        }

        Ok(())
    }

    /// Check if we should add an index entry based on configured intervals
    fn should_add_index_entry(&self) -> bool {
        self.bytes_since_last_index >= self.indexing_config.index_interval_bytes
            || self.records_since_last_index >= self.indexing_config.index_interval_records
    }

    /// Find the byte position to start reading from for a given offset
    pub fn find_position_for_offset(&self, offset: u64) -> Option<u32> {
        self.index.find_position_for_offset(offset)
    }

    /// Read records from a given byte position
    pub fn read_records_from_position(
        &mut self,
        start_position: u32,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let mut log_file_copy = std::fs::File::open(&self.log_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open log file for reading"))?;
        log_file_copy
            .seek(SeekFrom::Start(start_position as u64))
            .map_err(|e| {
                StorageError::from_io_error(e, "Failed to seek to position in log file")
            })?;

        let mut reader = BufReader::new(log_file_copy);
        let mut records = Vec::new();
        let max_records = count.unwrap_or(usize::MAX);

        while records.len() < max_records {
            match self.deserialize_record(&mut reader) {
                Ok(record_with_offset) => {
                    // Only include records within this segment's range
                    if record_with_offset.offset >= self.base_offset {
                        records.push(record_with_offset);
                    }
                }
                Err(_) => break, // End of file or error
            }
        }

        Ok(records)
    }

    /// Get segment metadata
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn max_offset(&self) -> u64 {
        self.max_offset
    }

    pub fn size_bytes(&self) -> Result<u64, StorageError> {
        Ok(self
            .log_file
            .metadata()
            .map_err(|e| StorageError::from_io_error(e, "Failed to get log file metadata"))?
            .len())
    }

    pub fn record_count(&self) -> usize {
        // This could be tracked more efficiently, but for now we estimate
        if self.max_offset >= self.base_offset {
            (self.max_offset - self.base_offset + 1) as usize
        } else {
            0
        }
    }

    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.base_offset && offset <= self.max_offset
    }

    /// Force sync all files
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.log_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync log file"))?;
        self.index_file
            .sync_all()
            .map_err(|e| StorageError::from_io_error(e, "Failed to sync index file"))?;
        Ok(())
    }

    fn serialize_record(&self, record: &Record, offset: u64) -> Result<Vec<u8>, StorageError> {
        // Serialize the record to JSON
        let json_payload = serde_json::to_vec(record).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to serialize record to JSON")
        })?;

        // Generate ISO 8601 timestamp
        let timestamp = chrono::Utc::now().to_rfc3339();
        let timestamp_bytes = timestamp.as_bytes();

        // Calculate total payload size (JSON + timestamp length + timestamp)
        let timestamp_len = timestamp_bytes.len() as u32;
        let payload_size = json_payload.len() as u32 + 4 + timestamp_len; // 4 bytes for timestamp length

        // Create buffer: [payload_size(4)] [offset(8)] [timestamp_len(4)] [timestamp] [json_payload]
        let mut buffer = Vec::with_capacity(4 + 8 + 4 + timestamp_bytes.len() + json_payload.len());

        // Write payload size (4 bytes, big-endian)
        buffer.extend_from_slice(&payload_size.to_be_bytes());

        // Write offset (8 bytes, big-endian)
        buffer.extend_from_slice(&offset.to_be_bytes());

        // Write timestamp length (4 bytes, big-endian)
        buffer.extend_from_slice(&timestamp_len.to_be_bytes());

        // Write timestamp
        buffer.extend_from_slice(timestamp_bytes);

        // Write JSON payload
        buffer.extend_from_slice(&json_payload);

        Ok(buffer)
    }

    fn deserialize_record(
        &self,
        reader: &mut BufReader<File>,
    ) -> Result<RecordWithOffset, StorageError> {
        use std::io::Read;

        // Read payload size (4 bytes, big-endian)
        let mut payload_size_bytes = [0u8; 4];
        reader
            .read_exact(&mut payload_size_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read payload size"))?;
        let payload_size = u32::from_be_bytes(payload_size_bytes);

        // Read offset (8 bytes, big-endian)
        let mut offset_bytes = [0u8; 8];
        reader
            .read_exact(&mut offset_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read offset"))?;
        let offset = u64::from_be_bytes(offset_bytes);

        // Read timestamp length (4 bytes, big-endian)
        let mut timestamp_len_bytes = [0u8; 4];
        reader
            .read_exact(&mut timestamp_len_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp length"))?;
        let timestamp_len = u32::from_be_bytes(timestamp_len_bytes);

        // Read timestamp
        let mut timestamp_bytes = vec![0u8; timestamp_len as usize];
        reader
            .read_exact(&mut timestamp_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp"))?;
        let timestamp = String::from_utf8(timestamp_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to parse timestamp as UTF-8")
        })?;

        // Calculate JSON payload size
        let json_len = payload_size - 4 - timestamp_len; // subtract timestamp_len field + timestamp

        // Read JSON payload
        let mut json_bytes = vec![0u8; json_len as usize];
        reader
            .read_exact(&mut json_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read JSON payload"))?;

        // Deserialize Record from JSON
        let record: Record = serde_json::from_slice(&json_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to deserialize record from JSON")
        })?;

        // Create RecordWithOffset
        Ok(RecordWithOffset {
            record,
            offset,
            timestamp,
        })
    }

    // Determine the maximum offset in this segment by scanning
    fn determine_max_offset(&mut self) -> Result<(), StorageError> {
        // Start with assumption that no records exist
        self.max_offset = self.base_offset.saturating_sub(1);

        // Check if log file exists and has content
        let log_file = match std::fs::File::open(&self.log_path) {
            Ok(file) => file,
            Err(_) => return Ok(()), // File doesn't exist, no records
        };

        let metadata = log_file.metadata().map_err(|e| {
            StorageError::from_io_error(e, "Failed to get log file metadata")
        })?;

        if metadata.len() == 0 {
            return Ok(()); // Empty file, no records
        }

        // Scan through the file to find all records and determine max offset
        let mut reader = BufReader::new(log_file);
        let mut last_valid_offset = self.base_offset.saturating_sub(1);

        loop {
            match self.deserialize_record(&mut reader) {
                Ok(record_with_offset) => {
                    last_valid_offset = record_with_offset.offset;
                },
                Err(_) => break, // End of file or corrupted data
            }
        }

        self.max_offset = last_valid_offset;
        Ok(())
    }
}

/// Manager for multiple log segments, implementing segment rolling
pub struct SegmentManager {
    /// All segments indexed by their base offset
    segments: BTreeMap<u64, LogSegment>,
    /// Currently active (writable) segment
    active_segment: Option<LogSegment>,
    /// Base directory for segment files
    base_dir: PathBuf,
    /// Maximum size per segment before rolling
    segment_size_bytes: u64,
    /// Sync mode for all segments
    sync_mode: SyncMode,
    /// Indexing configuration
    indexing_config: IndexingConfig,
}

impl SegmentManager {
    /// Helper method to deserialize a record from a BufReader (static method)
    fn deserialize_record_from_reader(
        reader: &mut BufReader<File>,
    ) -> Result<RecordWithOffset, StorageError> {
        use std::io::Read;

        // Read payload size (4 bytes, big-endian)
        let mut payload_size_bytes = [0u8; 4];
        reader
            .read_exact(&mut payload_size_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read payload size"))?;
        let payload_size = u32::from_be_bytes(payload_size_bytes);

        // Read offset (8 bytes, big-endian)
        let mut offset_bytes = [0u8; 8];
        reader
            .read_exact(&mut offset_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read offset"))?;
        let offset = u64::from_be_bytes(offset_bytes);

        // Read timestamp length (4 bytes, big-endian)
        let mut timestamp_len_bytes = [0u8; 4];
        reader
            .read_exact(&mut timestamp_len_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp length"))?;
        let timestamp_len = u32::from_be_bytes(timestamp_len_bytes);

        // Read timestamp
        let mut timestamp_bytes = vec![0u8; timestamp_len as usize];
        reader
            .read_exact(&mut timestamp_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp"))?;
        let timestamp = String::from_utf8(timestamp_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to parse timestamp as UTF-8")
        })?;

        // Calculate JSON payload size
        let json_len = payload_size - 4 - timestamp_len; // subtract timestamp_len field + timestamp

        // Read JSON payload
        let mut json_bytes = vec![0u8; json_len as usize];
        reader
            .read_exact(&mut json_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read JSON payload"))?;

        // Deserialize Record from JSON
        let record: Record = serde_json::from_slice(&json_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to deserialize record from JSON")
        })?;

        // Create RecordWithOffset
        Ok(RecordWithOffset {
            record,
            offset,
            timestamp,
        })
    }
    pub fn new(
        base_dir: PathBuf,
        segment_size_bytes: u64,
        sync_mode: SyncMode,
        indexing_config: IndexingConfig,
    ) -> Self {
        Self {
            segments: BTreeMap::new(),
            active_segment: None,
            base_dir,
            segment_size_bytes,
            sync_mode,
            indexing_config,
        }
    }

    /// Find the segment that contains the given offset (immutable access)
    pub fn find_segment_for_offset(&self, offset: u64) -> Option<&LogSegment> {
        // Check active segment first
        if let Some(active) = &self.active_segment {
            if active.contains_offset(offset) {
                return Some(active);
            }
        }

        // Search in archived segments using BTreeMap for efficient lookup
        self.segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .filter(|segment| segment.contains_offset(offset))
    }

    /// Read records from a specific offset (immutable method)
    pub fn read_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        // Find the segment containing the target offset
        let segment = self.find_segment_for_offset(offset).ok_or_else(|| {
            StorageError::from_io_error(
                std::io::Error::new(std::io::ErrorKind::NotFound, "Offset not found"),
                &format!("No segment found containing offset {}", offset),
            )
        })?;

        // Use sparse index to find starting position
        let start_position = segment.find_position_for_offset(offset).unwrap_or(0);

        // Create a new file handle for reading (immutable operation)
        let log_file = std::fs::File::open(&segment.log_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open log file for reading"))?;

        let mut log_file = log_file;
        log_file
            .seek(SeekFrom::Start(start_position as u64))
            .map_err(|e| StorageError::from_io_error(e, "Failed to seek to position"))?;

        let mut reader = BufReader::new(log_file);
        let mut records = Vec::new();
        let max_records = count.unwrap_or(usize::MAX);

        // Deserialize records from the reader using the same format as LogSegment
        while records.len() < max_records {
            match Self::deserialize_record_from_reader(&mut reader) {
                Ok(record_with_offset) => {
                    // Only include records that match our target offset or higher
                    if record_with_offset.offset >= offset {
                        records.push(record_with_offset);
                    }
                }
                Err(_) => break, // End of file or read error
            }
        }

        Ok(records)
    }

    /// Check if we should roll to a new segment
    pub fn should_roll_segment(&self) -> bool {
        if let Some(active) = &self.active_segment {
            if let Ok(size) = active.size_bytes() {
                return size >= self.segment_size_bytes;
            }
        }
        false
    }

    /// Roll to a new segment
    pub fn roll_to_new_segment(&mut self, next_offset: u64) -> Result<(), StorageError> {
        // Archive current active segment if it exists
        if let Some(active) = self.active_segment.take() {
            let base_offset = active.base_offset();
            self.segments.insert(base_offset, active);
        }

        // Create new active segment
        let log_path = self.base_dir.join(format!("{:020}.log", next_offset));
        let index_path = self.base_dir.join(format!("{:020}.index", next_offset));

        let new_segment = LogSegment::new(
            next_offset,
            log_path,
            index_path,
            self.sync_mode,
            self.indexing_config.clone(),
        )?;

        self.active_segment = Some(new_segment);
        Ok(())
    }

    pub fn active_segment_mut(&mut self) -> Option<&mut LogSegment> {
        self.active_segment.as_mut()
    }

    /// Get an iterator over all segments (both active and archived)
    pub fn all_segments(&self) -> impl Iterator<Item = &LogSegment> {
        self.segments.values().chain(self.active_segment.iter())
    }

    /// Recover segment manager state from existing files in the directory
    pub fn recover_from_directory(&mut self) -> Result<(), StorageError> {
        use std::fs;

        // Clear existing state
        self.segments.clear();
        self.active_segment = None;

        // Read all .log files in the directory
        let entries = fs::read_dir(&self.base_dir)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read segment directory"))?;

        let mut segment_offsets = Vec::new();

        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::from_io_error(e, "Failed to read directory entry"))?;
            let path = entry.path();
            
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".log") {
                    // Parse offset from filename (format: 00000000000000000000.log)
                    if let Some(offset_str) = file_name.strip_suffix(".log") {
                        if let Ok(offset) = offset_str.parse::<u64>() {
                            segment_offsets.push(offset);
                        }
                    }
                }
            }
        }

        // Sort offsets to process segments in order
        segment_offsets.sort_unstable();

        // Recover each segment
        for &base_offset in &segment_offsets {
            let log_path = self.base_dir.join(format!("{base_offset:020}.log"));
            let index_path = self.base_dir.join(format!("{base_offset:020}.index"));

            // Only recover if both log and index files exist
            if log_path.exists() && index_path.exists() {
                let segment = LogSegment::recover(
                    base_offset,
                    log_path,
                    index_path,
                    self.sync_mode,
                    self.indexing_config.clone(),
                )?;
                
                self.segments.insert(base_offset, segment);
            }
        }

        // The most recent segment becomes the active segment
        if let Some(&latest_offset) = segment_offsets.last() {
            if let Some(latest_segment) = self.segments.remove(&latest_offset) {
                self.active_segment = Some(latest_segment);
            }
        }

        Ok(())
    }
}
