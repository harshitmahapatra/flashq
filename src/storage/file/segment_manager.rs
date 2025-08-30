use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::{collections::BTreeMap, io::BufReader};

use crate::error::StorageError;
use crate::storage::file::{IndexingConfig, LogSegment, SyncMode};
use crate::{Record, RecordWithOffset};

/// Manager for multiple log segments, implementing segment rolling
pub struct SegmentManager {
    segments: BTreeMap<u64, LogSegment>,
    active_segment: Option<LogSegment>,
    base_dir: PathBuf,
    segment_size_bytes: u64,
    sync_mode: SyncMode,
    indexing_config: IndexingConfig,
}

impl SegmentManager {
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

    fn deserialize_record_from_reader(
        reader: &mut BufReader<File>,
    ) -> Result<RecordWithOffset, StorageError> {
        use std::io::Read;

        let mut payload_size_bytes = [0u8; 4];
        reader
            .read_exact(&mut payload_size_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read payload size"))?;
        let payload_size = u32::from_be_bytes(payload_size_bytes);

        let mut offset_bytes = [0u8; 8];
        reader
            .read_exact(&mut offset_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read offset"))?;
        let offset = u64::from_be_bytes(offset_bytes);

        let mut timestamp_len_bytes = [0u8; 4];
        reader
            .read_exact(&mut timestamp_len_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp length"))?;
        let timestamp_len = u32::from_be_bytes(timestamp_len_bytes);

        let mut timestamp_bytes = vec![0u8; timestamp_len as usize];
        reader
            .read_exact(&mut timestamp_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp"))?;
        let timestamp = String::from_utf8(timestamp_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to parse timestamp as UTF-8")
        })?;

        let json_len = payload_size - 4 - timestamp_len; // subtract timestamp_len field + timestamp

        let mut json_bytes = vec![0u8; json_len as usize];
        reader
            .read_exact(&mut json_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read JSON payload"))?;

        let record: Record = serde_json::from_slice(&json_bytes).map_err(|e| {
            StorageError::from_serialization_error(e, "Failed to deserialize record from JSON")
        })?;

        Ok(RecordWithOffset {
            record,
            offset,
            timestamp,
        })
    }

    pub fn find_segment_for_offset(&self, offset: u64) -> Option<&LogSegment> {
        if let Some(active) = &self.active_segment {
            if active.contains_offset(offset) {
                return Some(active);
            }
        }

        self.segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .filter(|segment| segment.contains_offset(offset))
    }

    pub fn read_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let segment = self.find_segment_for_offset(offset).ok_or_else(|| {
            StorageError::from_io_error(
                std::io::Error::new(std::io::ErrorKind::NotFound, "Offset not found"),
                &format!("No segment found containing offset {}", offset),
            )
        })?;

        let start_position = segment.find_position_for_offset(offset).unwrap_or(0);

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

    pub fn should_roll_segment(&self) -> bool {
        if let Some(active) = &self.active_segment {
            if let Ok(size) = active.size_bytes() {
                return size >= self.segment_size_bytes;
            }
        }
        false
    }

    pub fn roll_to_new_segment(&mut self, next_offset: u64) -> Result<(), StorageError> {
        if let Some(active) = self.active_segment.take() {
            let base_offset = active.base_offset;
            self.segments.insert(base_offset, active);
        }

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

    pub fn all_segments(&self) -> impl Iterator<Item = &LogSegment> {
        self.segments.values().chain(self.active_segment.iter())
    }

    pub fn recover_from_directory(&mut self) -> Result<(), StorageError> {
        self.segments.clear();
        self.active_segment = None;

        // Read all .log files in the directory
        let entries = std::fs::read_dir(&self.base_dir)
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

        if let Some(&latest_offset) = segment_offsets.last() {
            if let Some(latest_segment) = self.segments.remove(&latest_offset) {
                self.active_segment = Some(latest_segment);
            }
        }

        Ok(())
    }
}
