use crate::error::StorageError;
use crate::storage::file::common::ensure_directory_exists;
use crate::storage::file::{IndexingConfig, SegmentManager, SyncMode};
use crate::storage::r#trait::{PartitionId, TopicLog};
use crate::{Record, RecordWithOffset};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// File-based topic log implementation using Kafka-aligned segment architecture
pub struct FileTopicLog {
    topic: String,
    base_dir: PathBuf,
    pub partitions: HashMap<PartitionId, PartitionData>,
    sync_mode: SyncMode,
    segment_size_bytes: u64,
    batch_bytes: usize,
    indexing_config: IndexingConfig,
}

pub struct PartitionData {
    pub segment_manager: SegmentManager,
    pub next_offset: u64,
    pub record_count: usize,
}

impl FileTopicLog {
    pub fn new<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        segment_size_bytes: u64,
    ) -> Result<Self, std::io::Error> {
        Self::new_with_batch_bytes_and_indexing_config(
            topic,
            sync_mode,
            data_dir,
            segment_size_bytes,
            crate::storage::batching_heuristics::default_batch_bytes(),
            IndexingConfig::default(),
        )
    }

    pub fn new_with_batch_bytes<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        segment_size_bytes: u64,
        batch_bytes: usize,
    ) -> Result<Self, std::io::Error> {
        Self::new_with_batch_bytes_and_indexing_config(
            topic,
            sync_mode,
            data_dir,
            segment_size_bytes,
            batch_bytes,
            IndexingConfig::default(),
        )
    }

    #[tracing::instrument(level = "info", skip_all, fields(topic = %topic))]
    pub fn new_with_batch_bytes_and_indexing_config<P: AsRef<Path>>(
        topic: &str,
        sync_mode: SyncMode,
        data_dir: P,
        segment_size_bytes: u64,
        batch_bytes: usize,
        indexing_config: IndexingConfig,
    ) -> Result<Self, std::io::Error> {
        let base_dir = Self::setup_topic_directory(&data_dir, topic)?;

        let mut log = FileTopicLog {
            topic: topic.to_string(),
            base_dir,
            partitions: HashMap::new(),
            sync_mode,
            segment_size_bytes,
            batch_bytes,
            indexing_config,
        };

        log.recover_all_partitions()?;
        Ok(log)
    }

    fn setup_topic_directory<P: AsRef<Path>>(
        data_dir: P,
        topic: &str,
    ) -> Result<PathBuf, std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_directory_exists(&data_dir)?;

        let base_dir = data_dir.join(topic);
        ensure_directory_exists(&base_dir)?;

        Ok(base_dir)
    }

    fn sync_all_partitions(&mut self) -> Result<(), StorageError> {
        for partition_data in self.partitions.values_mut() {
            if let Some(active_segment) = partition_data.segment_manager.active_segment_mut() {
                active_segment.sync()?;
            }
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.sync_all_partitions()
    }

    #[tracing::instrument(level = "info", skip(self), fields(topic = %self.topic))]
    fn recover_all_partitions(&mut self) -> Result<(), std::io::Error> {
        let partition_ids = self.scan_for_existing_partitions()?;

        info!(
            "Found {} existing partitions for topic {}: {:?}",
            partition_ids.len(),
            self.topic,
            partition_ids
        );

        for partition_id in partition_ids {
            self.recover_single_partition(partition_id)?;
        }

        info!("Completed partition recovery for topic: {}", self.topic);
        Ok(())
    }

    fn scan_for_existing_partitions(&self) -> Result<Vec<PartitionId>, std::io::Error> {
        let entries = std::fs::read_dir(&self.base_dir).map_err(|e| {
            error!(
                "Failed to read topic directory {}: {}",
                self.base_dir.display(),
                e
            );
            e
        })?;

        let mut partition_ids = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if let Some(partition_id) = self.extract_partition_id_from_path(&path) {
                partition_ids.push(partition_id);
            }
        }

        partition_ids.sort_by_key(|id| id.0);
        Ok(partition_ids)
    }

    fn extract_partition_id_from_path(&self, path: &std::path::Path) -> Option<PartitionId> {
        if !path.is_dir() {
            return None;
        }

        let dir_name = path.file_name()?.to_str()?;
        match dir_name.parse::<u32>() {
            Ok(partition_id) => {
                debug!(
                    "Found partition directory: {} for partition {}",
                    path.display(),
                    partition_id
                );
                Some(PartitionId(partition_id))
            }
            Err(_) => {
                debug!("Skipping non-numeric directory: {}", path.display());
                None
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(partition = %partition_id.0))]
    fn recover_single_partition(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<(), std::io::Error> {
        let partition_dir = self.base_dir.join(partition_id.to_string());

        let mut segment_manager = self.create_segment_manager(&partition_dir);
        segment_manager.recover_from_directory().map_err(|e| {
            error!(
                "Failed to recover partition {} from {}: {}",
                partition_id.0,
                partition_dir.display(),
                e
            );
            std::io::Error::other(format!("Partition recovery failed: {e}"))
        })?;

        let (next_offset, record_count) = self.calculate_metadata_from_segments(&segment_manager);

        info!(
            "Recovered partition {}: next_offset={}, record_count={}",
            partition_id.0, next_offset, record_count
        );

        self.partitions.insert(
            partition_id,
            PartitionData {
                segment_manager,
                next_offset,
                record_count,
            },
        );

        Ok(())
    }

    fn create_segment_manager(&self, partition_dir: &std::path::Path) -> SegmentManager {
        SegmentManager::new(
            partition_dir.to_path_buf(),
            self.segment_size_bytes,
            self.sync_mode,
            self.indexing_config.clone(),
        )
    }

    fn calculate_metadata_from_segments(&self, segment_manager: &SegmentManager) -> (u64, usize) {
        let mut total_records = 0;
        let mut highest_offset = 0;

        for segment in segment_manager.all_segments() {
            let segment_record_count = segment.record_count();
            total_records += segment_record_count;

            if segment_record_count > 0 {
                if let Some(segment_max_offset) = segment.max_offset {
                    highest_offset = highest_offset.max(segment_max_offset);
                }
            }
        }

        let next_offset = if total_records > 0 {
            highest_offset + 1
        } else {
            0
        };

        debug!("Calculated metadata: next_offset={next_offset}, record_count={total_records}");
        (next_offset, total_records)
    }

    fn get_or_create_partition(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<&mut PartitionData, StorageError> {
        if self.partitions.contains_key(&partition_id) {
            return Ok(self.partitions.get_mut(&partition_id).unwrap());
        }

        let partition_data = self.create_new_partition(partition_id)?;
        self.partitions.insert(partition_id, partition_data);
        Ok(self.partitions.get_mut(&partition_id).unwrap())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(topic = %self.topic, partition = %partition_id.0), name = "create_partition")]
    fn create_new_partition(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<PartitionData, StorageError> {
        debug!(
            "Creating new partition {} for topic {}",
            partition_id.0, self.topic
        );

        let partition_dir = self.setup_partition_directory(partition_id)?;
        let mut segment_manager = self.create_segment_manager(&partition_dir);

        segment_manager.recover_from_directory().map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(format!("Recovery failed: {e}")),
                &format!(
                    "recover partition directory {}/{}",
                    self.topic, partition_id
                ),
            )
        })?;

        let (next_offset, total_records) = self.calculate_partition_state(&mut segment_manager)?;

        debug!(
            "Successfully created partition {}: next_offset={}, record_count={}",
            partition_id.0, next_offset, total_records
        );

        Ok(PartitionData {
            segment_manager,
            next_offset,
            record_count: total_records,
        })
    }

    fn setup_partition_directory(
        &self,
        partition_id: PartitionId,
    ) -> Result<PathBuf, StorageError> {
        let partition_dir = self.base_dir.join(partition_id.to_string());
        ensure_directory_exists(&partition_dir).map_err(|e| {
            StorageError::from_io_error(
                e,
                &format!("create partition directory {}/{}", self.topic, partition_id),
            )
        })?;
        Ok(partition_dir)
    }

    fn calculate_partition_state(
        &self,
        segment_manager: &mut SegmentManager,
    ) -> Result<(u64, usize), StorageError> {
        let (next_offset, total_records) = self.calculate_metadata_from_segments(segment_manager);

        if segment_manager.active_segment_mut().is_none() {
            segment_manager.roll_to_new_segment(next_offset)?;
        }

        Ok((next_offset, total_records))
    }

    fn find_partition(&self, partition_id: PartitionId) -> Option<&PartitionData> {
        self.partitions.get(&partition_id)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(topic = %self.topic, partition = %partition_id.0, count = records.len()), name = "write_batch")]
    fn write_batch_to_partition(
        &mut self,
        partition_id: PartitionId,
        records: &[Record],
    ) -> Result<u64, StorageError> {
        let partition_data = self.get_or_create_partition(partition_id)?;

        debug!(
            "Writing {} records to partition {} starting at offset {}",
            records.len(),
            partition_id.0,
            partition_data.next_offset
        );

        if partition_data.segment_manager.should_roll_segment() {
            info!("Rolling to new segment for partition {}", partition_id.0);
            partition_data
                .segment_manager
                .roll_to_new_segment(partition_data.next_offset)?;
        }

        let active_segment = partition_data
            .segment_manager
            .active_segment_mut()
            .ok_or_else(|| {
                StorageError::from_io_error(
                    std::io::Error::other("No active segment"),
                    "No active segment available for bulk writing",
                )
            })?;

        let start_offset = partition_data.next_offset;
        let last_offset = active_segment.append_records_bulk(records, start_offset)?;
        let appended_count = (last_offset - start_offset + 1) as usize;

        partition_data.next_offset += appended_count as u64;
        partition_data.record_count += appended_count;

        debug!(
            "Successfully wrote {} records to partition {}, offsets {}-{}, total records: {}",
            appended_count, partition_id.0, start_offset, last_offset, partition_data.record_count
        );

        Ok(last_offset)
    }
}

impl TopicLog for FileTopicLog {
    #[tracing::instrument(level = "debug", skip_all, fields(topic = %self.topic, partition = %partition_id.0), name = "append")]
    fn append_partition(
        &mut self,
        partition_id: PartitionId,
        record: Record,
    ) -> Result<u64, StorageError> {
        // Get the initial state we need
        let next_offset = {
            let partition_data = self.get_or_create_partition(partition_id)?;
            partition_data.next_offset
        };

        debug!(
            "Appending single record to partition {} at offset {}",
            partition_id.0, next_offset
        );

        // Now work with a fresh mutable borrow
        let partition_data = self.get_or_create_partition(partition_id)?;

        if partition_data.segment_manager.should_roll_segment() {
            info!("Rolling to new segment for partition {}", partition_id.0);
            partition_data
                .segment_manager
                .roll_to_new_segment(partition_data.next_offset)?;
        }

        let active_segment = partition_data
            .segment_manager
            .active_segment_mut()
            .ok_or_else(|| {
                StorageError::from_io_error(
                    std::io::Error::other("No active segment"),
                    "No active segment available for writing",
                )
            })?;

        active_segment.append_record(&record, next_offset)?;
        partition_data.next_offset += 1;
        partition_data.record_count += 1;

        debug!(
            "Successfully appended record to partition {} at offset {}, total records: {}",
            partition_id.0, next_offset, partition_data.record_count
        );

        Ok(next_offset)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(topic = %self.topic, partition = %partition_id.0, count = records.len()), name = "append_batch")]
    fn append_batch_partition(
        &mut self,
        partition_id: PartitionId,
        records: Vec<Record>,
    ) -> Result<u64, StorageError> {
        if records.is_empty() {
            return Ok(self.get_or_create_partition(partition_id)?.next_offset);
        }

        let mut last_offset = 0;
        let mut start = 0;

        for i in 0..records.len() {
            // Check if we should flush this batch
            let should_flush = {
                if i <= start {
                    false
                } else {
                    let accumulated_size: usize = records[start..i]
                        .iter()
                        .map(crate::storage::batching_heuristics::estimate_record_size)
                        .sum();

                    let next_record_size =
                        crate::storage::batching_heuristics::estimate_record_size(&records[i]);

                    accumulated_size > 0 && accumulated_size + next_record_size > self.batch_bytes
                }
            };

            if should_flush {
                last_offset = self.write_batch_to_partition(partition_id, &records[start..i])?;
                start = i;
            }
        }

        if start < records.len() {
            last_offset = self.write_batch_to_partition(partition_id, &records[start..])?;
        }

        Ok(last_offset)
    }

    fn read_from_partition(
        &self,
        partition_id: PartitionId,
        from_offset: u64,
        max_bytes: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        match self.find_partition(partition_id) {
            Some(partition_data) => partition_data
                .segment_manager
                .read_records_streaming(from_offset, max_bytes),
            None => Ok(Vec::new()),
        }
    }

    fn read_from_partition_timestamp(
        &self,
        partition_id: PartitionId,
        ts_rfc3339: &str,
        max_bytes: Option<usize>,
    ) -> Result<Vec<RecordWithOffset>, StorageError> {
        match self.find_partition(partition_id) {
            Some(partition_data) => partition_data
                .segment_manager
                .read_records_from_timestamp(ts_rfc3339, max_bytes),
            None => Ok(Vec::new()),
        }
    }

    fn partition_len(&self, partition_id: PartitionId) -> usize {
        self.find_partition(partition_id)
            .map(|p| p.record_count)
            .unwrap_or(0)
    }

    fn partition_is_empty(&self, partition_id: PartitionId) -> bool {
        self.find_partition(partition_id)
            .map(|p| p.record_count == 0)
            .unwrap_or(true)
    }

    fn partition_next_offset(&self, partition_id: PartitionId) -> u64 {
        self.find_partition(partition_id)
            .map(|p| p.next_offset)
            .unwrap_or(0)
    }
}
