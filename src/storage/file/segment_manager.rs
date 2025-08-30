use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
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
                &format!("No segment found containing offset {offset}"),
            )
        })?;

        let start_position = segment.find_position_for_offset(offset).unwrap_or(0);

        let mut reader = open_log_reader_at_position(segment, start_position as u64)?;

        let max_records = count.unwrap_or(usize::MAX);

        Ok(collect_records(&mut reader, offset, max_records))
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

        let log_path = self.base_dir.join(format!("{next_offset:020}.log"));
        let index_path = self.base_dir.join(format!("{next_offset:020}.index"));

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

        let segment_offsets = get_segment_offsets(&self.base_dir)?;

        self.recover_segments(&segment_offsets)?;
        self.set_active_segment(&segment_offsets);

        Ok(())
    }

    fn recover_segments(&mut self, segment_offsets: &[u64]) -> Result<(), StorageError> {
        for &base_offset in segment_offsets {
            let log_path = self.base_dir.join(format!("{base_offset:020}.log"));
            let index_path = self.base_dir.join(format!("{base_offset:020}.index"));

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
        Ok(())
    }

    fn set_active_segment(&mut self, segment_offsets: &[u64]) {
        if let Some(&latest_offset) = segment_offsets.last() {
            if let Some(latest_segment) = self.segments.remove(&latest_offset) {
                self.active_segment = Some(latest_segment);
            }
        }
    }
}

fn get_segment_offsets(base_dir: &PathBuf) -> Result<Vec<u64>, StorageError> {
    let entries = std::fs::read_dir(base_dir)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read segment directory"))?;

    let mut segment_offsets = Vec::new();

    for entry in entries {
        let entry =
            entry.map_err(|e| StorageError::from_io_error(e, "Failed to read directory entry"))?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if file_name.ends_with(".log") {
                if let Some(offset_str) = file_name.strip_suffix(".log") {
                    if let Ok(offset) = offset_str.parse::<u64>() {
                        segment_offsets.push(offset);
                    }
                }
            }
        }
    }

    segment_offsets.sort_unstable();

    Ok(segment_offsets)
}

fn open_log_reader_at_position(
    segment: &LogSegment,
    position: u64,
) -> Result<BufReader<File>, StorageError> {
    let mut log_file = std::fs::File::open(&segment.log_path)
        .map_err(|e| StorageError::from_io_error(e, "Failed to open log file for reading"))?;

    log_file
        .seek(SeekFrom::Start(position))
        .map_err(|e| StorageError::from_io_error(e, "Failed to seek to position"))?;

    Ok(BufReader::new(log_file))
}

fn collect_records(
    reader: &mut BufReader<File>,
    start_offset: u64,
    max_records: usize,
) -> Vec<RecordWithOffset> {
    let mut records = Vec::new();

    while records.len() < max_records {
        match deserialize_record_from_reader(reader) {
            Ok(record_with_offset) => {
                if record_with_offset.offset >= start_offset {
                    records.push(record_with_offset);
                }
            }
            Err(_) => break, // End of file or read error
        }
    }

    records
}

fn deserialize_record_from_reader(
    reader: &mut BufReader<File>,
) -> Result<RecordWithOffset, StorageError> {
    let payload_size = read_u32(reader, "Failed to read payload size")?;
    let offset = read_u64(reader, "Failed to read offset")?;

    let (timestamp, timestamp_len) = read_timestamp(reader)?;

    let record = read_json_payload(reader, payload_size, timestamp_len)?;

    Ok(RecordWithOffset {
        record,
        offset,
        timestamp,
    })
}

fn read_bytes<const N: usize>(
    reader: &mut impl Read,
    error_context: &str,
) -> Result<[u8; N], StorageError> {
    let mut bytes = [0u8; N];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| StorageError::from_io_error(e, error_context))?;
    Ok(bytes)
}

fn read_u32(reader: &mut impl Read, error_context: &str) -> Result<u32, StorageError> {
    read_bytes::<4>(reader, error_context).map(u32::from_be_bytes)
}

fn read_u64(reader: &mut impl Read, error_context: &str) -> Result<u64, StorageError> {
    read_bytes::<8>(reader, error_context).map(u64::from_be_bytes)
}

fn read_timestamp(reader: &mut impl Read) -> Result<(String, u32), StorageError> {
    let timestamp_len = read_u32(reader, "Failed to read timestamp length")?;
    let mut timestamp_bytes = vec![0u8; timestamp_len as usize];
    reader
        .read_exact(&mut timestamp_bytes)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp"))?;
    let timestamp = String::from_utf8(timestamp_bytes).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to parse timestamp as UTF-8")
    })?;
    Ok((timestamp, timestamp_len))
}

fn read_json_payload(
    reader: &mut impl Read,
    payload_size: u32,
    timestamp_len: u32,
) -> Result<Record, StorageError> {
    let json_len = payload_size - 4 - timestamp_len; // subtract timestamp_len field + timestamp
    let mut json_bytes = vec![0u8; json_len as usize];
    reader
        .read_exact(&mut json_bytes)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read JSON payload"))?;
    serde_json::from_slice(&json_bytes).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to deserialize record from JSON")
    })
}
