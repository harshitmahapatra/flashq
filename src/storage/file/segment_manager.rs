use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::{collections::BTreeMap, io::BufReader};

use crate::RecordWithOffset;
use crate::error::StorageError;
use crate::storage::file::common::deserialize_record;
use crate::storage::file::{IndexingConfig, LogSegment, SyncMode};

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
        match deserialize_record(reader) {
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
