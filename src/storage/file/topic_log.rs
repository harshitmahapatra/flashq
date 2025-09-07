use crate::error::StorageError;
use crate::storage::file::common::ensure_directory_exists;
use crate::storage::file::{IndexingConfig, SegmentManager, SyncMode};
use crate::storage::r#trait::TopicLog;
use crate::{Record, RecordWithOffset};
use std::path::Path;

/// File-based topic log implementation using Kafka-aligned segment architecture
pub struct FileTopicLog {
    segment_manager: SegmentManager,
    next_offset: u64,
    record_count: usize,
    batch_bytes: usize,
}

impl FileTopicLog {
    pub fn new<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        segment_size_bytes: u64,
    ) -> Result<Self, std::io::Error> {
        Self::new_with_batch_bytes(
            topic,
            sync_mode,
            data_dir,
            segment_size_bytes,
            crate::storage::batching_heuristics::default_batch_bytes(),
        )
    }

    pub fn new_with_batch_bytes<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        segment_size_bytes: u64,
        batch_bytes: usize,
    ) -> Result<Self, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_directory_exists(&data_dir)?;

        let base_dir = data_dir.join(topic);
        ensure_directory_exists(&base_dir)?;
        let indexing_config = IndexingConfig::default();

        let segment_manager = SegmentManager::new(
            base_dir.clone(),
            segment_size_bytes,
            sync_mode,
            indexing_config.clone(),
        );

        let mut log = FileTopicLog {
            segment_manager,
            next_offset: 0,
            record_count: 0,
            batch_bytes,
        };

        log.recover_from_segments()
            .map_err(|e| std::io::Error::other(format!("Recovery failed: {e}")))?;

        Ok(log)
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        if let Some(active_segment) = self.segment_manager.active_segment_mut() {
            active_segment.sync()?;
        }
        Ok(())
    }

    fn recover_from_segments(&mut self) -> Result<(), StorageError> {
        self.segment_manager.recover_from_directory()?;

        let mut total_records = 0;
        let mut highest_offset = 0;

        for segment in self.segment_manager.all_segments() {
            let segment_record_count = segment.record_count();
            total_records += segment_record_count;

            if segment_record_count > 0 {
                if let Some(segment_highest_offset) = segment.max_offset {
                    highest_offset = highest_offset.max(segment_highest_offset);
                }
            }
        }

        self.record_count = total_records;
        self.next_offset = if total_records > 0 {
            highest_offset + 1
        } else {
            0
        };

        if self.segment_manager.active_segment_mut().is_none() {
            self.segment_manager.roll_to_new_segment(self.next_offset)?;
        }

        Ok(())
    }
}

impl TopicLog for FileTopicLog {
    fn append(&mut self, record: Record) -> Result<u64, StorageError> {
        let offset = self.next_offset;

        if self.segment_manager.should_roll_segment() {
            self.segment_manager.roll_to_new_segment(offset)?;
        }

        let active_segment = self.segment_manager.active_segment_mut().ok_or_else(|| {
            StorageError::from_io_error(
                std::io::Error::other("No active segment"),
                "No active segment available for writing",
            )
        })?;

        active_segment.append_record(&record, offset)?;

        self.next_offset += 1;
        self.record_count += 1;
        Ok(offset)
    }

    fn append_batch(&mut self, records: Vec<Record>) -> Result<u64, StorageError> {
        if records.is_empty() {
            return Ok(self.next_offset);
        }
        // Chunk by approximate serialized size to keep each write under batch_bytes

        let mut start = 0usize;
        let mut acc = 0usize;
        let mut last_offset = self.next_offset.saturating_sub(1);
        // Process in approx-size-bounded chunks
        for i in 0..records.len() {
            let est = crate::storage::batching_heuristics::estimate_record_size(&records[i]);
            if acc > 0 && acc + est > self.batch_bytes {
                if self.segment_manager.should_roll_segment() {
                    self.segment_manager.roll_to_new_segment(self.next_offset)?;
                }
                let active = self.segment_manager.active_segment_mut().ok_or_else(|| {
                    StorageError::from_io_error(
                        std::io::Error::other("No active segment"),
                        "No active segment available for bulk writing",
                    )
                })?;

                let start_off = self.next_offset;
                last_offset = active.append_records_bulk(&records[start..i], start_off)?;
                let appended = (last_offset - start_off + 1) as usize;
                self.next_offset = last_offset + 1;
                self.record_count += appended;
                let _ = appended;
                start = i;
                acc = 0;
            }
            acc += est;
        }

        if start < records.len() {
            if self.segment_manager.should_roll_segment() {
                self.segment_manager.roll_to_new_segment(self.next_offset)?;
            }
            let active = self.segment_manager.active_segment_mut().ok_or_else(|| {
                StorageError::from_io_error(
                    std::io::Error::other("No active segment"),
                    "No active segment available for bulk writing",
                )
            })?;
            let start_off = self.next_offset;
            last_offset = active.append_records_bulk(&records[start..], start_off)?;
            let appended = (last_offset - start_off + 1) as usize;
            self.next_offset = last_offset + 1;
            self.record_count += appended;
            let _ = appended;
        }

        Ok(last_offset)
    }

    fn get_records_from_offset(
        &self,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        // Use streaming reader to minimize per-chunk reopens/seeks
        self.segment_manager.read_records_streaming(offset, count)
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
