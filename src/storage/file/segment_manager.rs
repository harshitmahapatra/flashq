use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::{collections::BTreeMap, io::BufReader};

use crate::RecordWithOffset;
use crate::error::StorageError;
use crate::storage::file::common::deserialize_record;
use crate::storage::file::{IndexingConfig, LogSegment, SyncMode};

use log::warn;

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
        let max_records_to_read = count.unwrap_or(usize::MAX);
        let mut collected_records = Vec::new();

        let segments_sorted_by_offset = self.get_segments_sorted_by_offset();
        let starting_segment_index =
            self.find_starting_segment_index(&segments_sorted_by_offset, offset)?;

        for segment in &segments_sorted_by_offset[starting_segment_index..] {
            if collected_records.len() >= max_records_to_read {
                break;
            }

            let records_from_segment = self.read_records_from_single_segment(
                segment,
                offset,
                max_records_to_read - collected_records.len(),
            );

            collected_records.extend(records_from_segment);

            if collected_records.len() >= max_records_to_read {
                break;
            }
        }

        Ok(collected_records)
    }

    /// Streaming read that keeps a single reader per segment and iterates sequentially across segments
    pub fn read_records_streaming(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let max_records = count.unwrap_or(usize::MAX);
        if max_records == 0 {
            return Ok(Vec::new());
        }

        let segments_sorted_by_offset = self.get_segments_sorted_by_offset();
        let start_idx = self.find_starting_segment_index(&segments_sorted_by_offset, offset)?;

        let mut results: Vec<RecordWithOffset> = Vec::new();
        let mut need_offset = offset;

        for segment in &segments_sorted_by_offset[start_idx..] {
            if results.len() >= max_records {
                break;
            }

            let file_pos = self.calculate_file_position_for_segment(segment, need_offset);
            let mut reader = match create_segment_reader(segment, file_pos) {
                Ok(r) => r,
                Err(e) => {
                    log_read_error(&e);
                    continue;
                }
            };

            while results.len() < max_records {
                match deserialize_record(&mut reader) {
                    Ok(r) => {
                        if r.offset >= need_offset {
                            results.push(r);
                        }
                    }
                    Err(_) => break, // next segment
                }
            }

            if let Some(last) = results.last() {
                need_offset = last.offset + 1;
            }
        }

        Ok(results)
    }

    /// Streaming read starting from the first record whose timestamp is >= `ts_rfc3339`.
    /// Uses each segment's sparse time index to compute a near position and then streams forward.
    pub fn read_records_from_timestamp(
        &self,
        ts_rfc3339: &str,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        let max_records = count.unwrap_or(usize::MAX);
        if max_records == 0 {
            return Ok(Vec::new());
        }

        // Parse the target timestamp to milliseconds since epoch
        let ts_ms_i64 = match chrono::DateTime::parse_from_rfc3339(ts_rfc3339) {
            Ok(dt) => dt.timestamp_millis(),
            Err(e) => {
                return Err(StorageError::DataCorruption {
                    context: "parse from_time".to_string(),
                    details: e.to_string(),
                });
            }
        };
        let target_ts_ms: u64 = if ts_ms_i64 < 0 { 0 } else { ts_ms_i64 as u64 };

        let segments_sorted_by_offset = self.get_segments_sorted_by_offset();
        let mut results: Vec<RecordWithOffset> = Vec::new();

        // Conservative backseek window in bytes (bounded extra scan per segment)
        const TIME_SEEK_BACK_BYTES: u64 = 16 * 1024; // 16KB default

        for segment in &segments_sorted_by_offset {
            if results.len() >= max_records {
                break;
            }

            // Compute starting position in this segment using time index
            let pos_time = segment
                .find_position_for_timestamp(target_ts_ms)
                .unwrap_or(0) as u64;

            // Backseek by a bounded window to avoid missing equality-run boundaries
            let back = std::cmp::min(pos_time, TIME_SEEK_BACK_BYTES);
            let start_guess = pos_time.saturating_sub(back);

            // Round down to previous offset-index anchor (two-step seek)
            let pos_anchor = segment
                .find_floor_position_for_file_position(start_guess as u32)
                .unwrap_or(0) as u64;

            // Start from the earlier of guess and anchor to be conservative
            let start_pos = std::cmp::min(start_guess, pos_anchor);

            let mut reader = match create_segment_reader(segment, start_pos) {
                Ok(r) => r,
                Err(e) => {
                    log_read_error(&e);
                    continue;
                }
            };

            while results.len() < max_records {
                match deserialize_record(&mut reader) {
                    Ok(r) => {
                        // Parse record ts to ms and compare
                        let rec_ts_ms: u64 = chrono::DateTime::parse_from_rfc3339(&r.timestamp)
                            .map(|dt| dt.timestamp_millis())
                            .map(|v| if v < 0 { 0 } else { v as u64 })
                            .unwrap_or(0);
                        if rec_ts_ms >= target_ts_ms {
                            results.push(r);
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        Ok(results)
    }

    fn get_segments_sorted_by_offset(&self) -> Vec<&LogSegment> {
        let mut segments: Vec<&LogSegment> = self.all_segments().collect();
        segments.sort_by_key(|s| s.base_offset);
        segments
    }

    fn find_starting_segment_index(
        &self,
        segments: &[&LogSegment],
        offset: u64,
    ) -> Result<usize, StorageError> {
        segments
            .iter()
            .position(|segment| segment.contains_offset(offset) || segment.base_offset > offset)
            .ok_or_else(|| {
                StorageError::from_io_error(
                    std::io::Error::new(std::io::ErrorKind::NotFound, "Offset not found"),
                    &format!("No segment found containing offset {offset}"),
                )
            })
    }

    fn read_records_from_single_segment(
        &self,
        segment: &LogSegment,
        start_offset: u64,
        max_records: usize,
    ) -> Vec<RecordWithOffset> {
        let file_position = self.calculate_file_position_for_segment(segment, start_offset);

        match create_segment_reader(segment, file_position) {
            Ok(mut reader) => collect_records(&mut reader, start_offset, max_records),
            Err(error) => {
                log_read_error(&error);
                Vec::new()
            }
        }
    }

    fn calculate_file_position_for_segment(&self, segment: &LogSegment, start_offset: u64) -> u64 {
        if segment.contains_offset(start_offset) {
            segment.find_position_for_offset(start_offset).unwrap_or(0) as u64
        } else {
            0
        }
    }

    pub fn should_roll_segment(&mut self) -> bool {
        if let Some(active) = &mut self.active_segment {
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

fn get_segment_offsets(segment_directory: &PathBuf) -> Result<Vec<u64>, StorageError> {
    let directory_entries = std::fs::read_dir(segment_directory)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read segment directory"))?;

    let mut discovered_offsets = Vec::new();

    for directory_entry in directory_entries {
        let entry = directory_entry
            .map_err(|e| StorageError::from_io_error(e, "Failed to read directory entry"))?;

        if let Some(offset) = extract_offset_from_log_file(&entry.path()) {
            discovered_offsets.push(offset);
        }
    }

    discovered_offsets.sort_unstable();
    Ok(discovered_offsets)
}

fn extract_offset_from_log_file(file_path: &std::path::Path) -> Option<u64> {
    let file_name = file_path.file_name()?.to_str()?;

    if !file_name.ends_with(".log") {
        return None;
    }

    let offset_string = file_name.strip_suffix(".log")?;
    offset_string.parse::<u64>().ok()
}

fn create_segment_reader(
    segment: &LogSegment,
    file_position: u64,
) -> Result<BufReader<File>, StorageError> {
    let mut segment_file = std::fs::File::open(&segment.log_path)
        .map_err(|e| StorageError::from_io_error(e, "Failed to open segment file"))?;

    segment_file
        .seek(SeekFrom::Start(file_position))
        .map_err(|e| StorageError::from_io_error(e, "Failed to seek to file position"))?;

    Ok(BufReader::new(segment_file))
}

fn collect_records(
    segment_reader: &mut BufReader<File>,
    minimum_offset: u64,
    maximum_records: usize,
) -> Vec<RecordWithOffset> {
    let mut collected_records = Vec::new();

    while collected_records.len() < maximum_records {
        match deserialize_record(segment_reader) {
            Ok(record_with_offset) => {
                if record_with_offset.offset >= minimum_offset {
                    collected_records.push(record_with_offset);
                }
            }
            Err(deserialization_error) => {
                log_read_error(&deserialization_error);
                break;
            }
        }
    }

    collected_records
}

fn log_read_error(storage_error: &StorageError) {
    match storage_error {
        StorageError::ReadFailed { source, .. } => {
            if !is_expected_end_of_file_error(source) {
                warn!(
                    "IO error while reading records, continuing with partial data: {storage_error}"
                );
            }
        }
        StorageError::DataCorruption { .. } => {
            warn!(
                "Data corruption detected while reading records, continuing with partial data: {storage_error}"
            );
        }
        _ => {
            warn!("Error while reading records, continuing with partial data: {storage_error}");
        }
    }
}

fn is_expected_end_of_file_error(error_source: &crate::error::StorageErrorSource) -> bool {
    let error_message = error_source.to_string();
    error_message.contains("UnexpectedEof") || error_message.contains("failed to fill whole buffer")
}
