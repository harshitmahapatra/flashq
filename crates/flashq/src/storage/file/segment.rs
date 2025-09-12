use crate::Record;
use crate::error::StorageError;
use crate::storage::file::common::{
    SyncMode, deserialize_record, serialize_record, serialize_record_into_buffer,
};
use crate::storage::file::file_io::FileIo;
use crate::storage::file::index::{IndexEntry, SparseIndex};
use crate::storage::file::time_index::{SparseTimeIndex, TimeIndexEntry};
use log::warn;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct IndexingConfig {
    pub index_interval_bytes: u32,
    pub index_interval_records: u32,
    pub time_seek_back_bytes: u32,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            index_interval_bytes: 4096,
            index_interval_records: 100,
            time_seek_back_bytes: 4096, // sensible default equals interval bytes
        }
    }
}

pub struct LogSegment {
    pub base_offset: u64,
    pub max_offset: Option<u64>,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    log_file: File,
    index_file: File,
    index_buffer: Vec<u8>,
    index: SparseIndex,
    // Time-based sparse index (parallel to offset index)
    pub time_index_path: PathBuf,
    time_index_file: File,
    time_index_buffer: Vec<u8>,
    time_index: SparseTimeIndex,
    bytes_since_last_index: u32,
    records_since_last_index: u32,
    sync_mode: SyncMode,
    indexing_config: IndexingConfig,
    // Cached timestamp range for pruning during time-based reads
    pub min_ts_ms: Option<u64>,
    pub max_ts_ms: Option<u64>,
}

impl LogSegment {
    #[tracing::instrument(level = "info", fields(base_offset, log = %log_path.display(), index = %index_path.display()))]
    pub fn new(
        base_offset: u64,
        log_path: PathBuf,
        index_path: PathBuf,
        sync_mode: SyncMode,
        indexing_config: IndexingConfig,
    ) -> Result<Self, StorageError> {
        // Use StdFileIO for log file operations
        let log_file = FileIo::create_with_append_and_read_permissions(&log_path).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to open log file",
            )
        })?;

        // Use StdFileIO for index file operations
        let index_file =
            FileIo::create_with_append_and_read_permissions(&index_path).map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to open index file",
                )
            })?;

        // Prepare time index alongside
        let time_index_path = index_path.with_extension("timeindex");
        let time_index_file = FileIo::create_with_append_and_read_permissions(&time_index_path)
            .map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to open time index file",
                )
            })?;

        Ok(LogSegment {
            base_offset,
            max_offset: None,
            log_path,
            index_path,
            log_file,
            index_file,
            index_buffer: Vec::new(),
            index: SparseIndex::new(),
            time_index_path,
            time_index_file,
            time_index_buffer: Vec::new(),
            time_index: SparseTimeIndex::new(),
            bytes_since_last_index: 0,
            records_since_last_index: 0,
            sync_mode,
            indexing_config,
            min_ts_ms: None,
            max_ts_ms: None,
        })
    }

    #[tracing::instrument(level = "info", fields(base_offset, log = %log_path.display(), index = %index_path.display()))]
    pub fn recover(
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

        if segment.index_path.exists() {
            let index_file = std::fs::File::open(&segment.index_path).map_err(|e| {
                StorageError::from_io_error(e, "Failed to open index file for reading")
            })?;
            let mut index_reader = BufReader::new(index_file);
            // Bound the maximum number of entries we accept from an index file to
            // avoid excessive allocations from corrupted or malformed files. On error,
            // rebuild the offset index from the log to degrade gracefully.
            match segment
                .index
                .read_from_file(&mut index_reader, base_offset, Some(1_000_000))
            {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        "Offset index read failed ({}); rebuilding from log {:?}",
                        err, segment.log_path
                    );
                    segment.rebuild_offset_index_from_log()?;
                }
            }

            // If offset index appears empty while log has data, rebuild it (treat as corrupt)
            let log_len = match std::fs::metadata(&segment.log_path) {
                Ok(m) => m.len(),
                Err(_) => 0,
            };
            if segment.index.last_entry().is_none() && log_len > 0 {
                warn!(
                    "Offset index at {:?} appears empty; rebuilding from log {:?}",
                    segment.index_path, segment.log_path
                );
                segment.rebuild_offset_index_from_log()?;
            }
        } else {
            // Missing offset index: build from the log
            segment.rebuild_offset_index_from_log()?;
        }

        if segment.time_index_path.exists() {
            let time_index_file = std::fs::File::open(&segment.time_index_path).map_err(|e| {
                StorageError::from_io_error(e, "Failed to open time index file for reading")
            })?;
            let mut time_index_reader = BufReader::new(time_index_file);
            // Read with a sane bound; if it fails, rebuild the time index from log.
            match segment
                .time_index
                .read_from_file(&mut time_index_reader, Some(1_000_000))
            {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        "Time index read failed ({}); rebuilding from log {:?}",
                        err, segment.log_path
                    );
                    segment.rebuild_time_index_from_log()?;
                }
            }

            // If time index appears empty while log has data, rebuild it (treat as corrupt)
            let log_len = match std::fs::metadata(&segment.log_path) {
                Ok(m) => m.len(),
                Err(_) => 0,
            };
            if segment.time_index.last_entry().is_none() && log_len > 0 {
                warn!(
                    "Time index at {:?} appears empty; rebuilding from log {:?}",
                    segment.time_index_path, segment.log_path
                );
                segment.rebuild_time_index_from_log()?;
            }
        } else {
            // Missing time index: build from the log
            segment.rebuild_time_index_from_log()?;
        }

        // Initialize max_ts_ms from time index if available (min_ts_ms optional)
        segment.max_ts_ms = segment.time_index.last_entry().map(|e| e.timestamp_ms);

        segment.max_offset = determine_max_offset(&segment.log_path, &segment.index)?;

        Ok(segment)
    }

    #[tracing::instrument(level = "debug", skip(self, record), fields(offset))]
    pub fn append_record(&mut self, record: &Record, offset: u64) -> Result<(), StorageError> {
        let serialized = serialize_record(record, offset)?;
        let start_position = self.write_record_to_log(&serialized)?;

        self.update_metadata(offset, serialized.len() as u32);
        // Best-effort timestamp for single append; may differ slightly from serialized value
        let ts_ms = chrono::Utc::now().timestamp_millis();
        let ts_ms = if ts_ms < 0 { 0 } else { ts_ms as u64 };

        // Maintain cached min/max timestamps for pruning
        self.min_ts_ms = Some(self.min_ts_ms.map_or(ts_ms, |v| v.min(ts_ms)));
        self.max_ts_ms = Some(self.max_ts_ms.map_or(ts_ms, |v| v.max(ts_ms)));

        if self.should_add_index_entry() {
            // Offset index update
            let index_entry = IndexEntry {
                offset,
                position: start_position,
            };
            self.index.add_entry(index_entry.clone());
            let serialized_entry = self.index.serialize_entry(&index_entry, self.base_offset);
            self.index_buffer.extend_from_slice(&serialized_entry);
            if self.index_buffer.len() >= self.indexing_config.index_interval_bytes as usize {
                self.flush_index_buffer()?;
            }

            // Time index update (avoid writing duplicates for identical timestamps)
            let write_time_entry = match self.time_index.last_entry() {
                Some(last) => last.timestamp_ms != ts_ms,
                None => true,
            };
            if write_time_entry {
                let tentry = TimeIndexEntry {
                    timestamp_ms: ts_ms,
                    position: start_position,
                };
                self.time_index.add_entry(tentry.clone());
                self.time_index_buffer
                    .extend_from_slice(&tentry.timestamp_ms.to_be_bytes());
                self.time_index_buffer
                    .extend_from_slice(&tentry.position.to_be_bytes());
                if self.time_index_buffer.len()
                    >= self.indexing_config.index_interval_bytes as usize
                {
                    self.flush_time_index_buffer()?;
                }
            }

            self.bytes_since_last_index = 0;
            self.records_since_last_index = 0;
        }
        self.sync_files_if_needed()
    }

    /// Append multiple records in a single I/O operation by coalescing
    /// serialized bytes into one contiguous buffer and writing once.
    #[tracing::instrument(level = "debug", skip(self, records), fields(count = records.len(), start_offset))]
    pub fn append_records_bulk(
        &mut self,
        records: &[Record],
        start_offset: u64,
    ) -> Result<u64, StorageError> {
        if records.is_empty() {
            return Err(StorageError::WriteFailed {
                context: "append_records_bulk: empty input".to_string(),
                source: Box::new(crate::error::StorageErrorSource::Custom(
                    "invalid input: no records".to_string(),
                )),
            });
        }

        // Serialize all records into a single buffer and remember each record's
        // starting position within the buffer as well as its size.
        let mut buf: Vec<u8> = Vec::with_capacity(records.len().saturating_mul(64));
        let mut rel_positions: Vec<u32> = Vec::with_capacity(records.len());
        let mut sizes: Vec<u32> = Vec::with_capacity(records.len());

        let mut next_offset = start_offset;
        let timestamp = chrono::Utc::now().to_rfc3339();
        let ts_ms_i64 = chrono::DateTime::parse_from_rfc3339(&timestamp)
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
        let ts_ms = if ts_ms_i64 < 0 { 0 } else { ts_ms_i64 as u64 };
        // Maintain cached min/max timestamps for pruning
        self.min_ts_ms = Some(self.min_ts_ms.map_or(ts_ms, |v| v.min(ts_ms)));
        self.max_ts_ms = Some(self.max_ts_ms.map_or(ts_ms, |v| v.max(ts_ms)));
        for record in records {
            let before = buf.len();
            // Use a single timestamp for the whole batch
            let rec_size = serialize_record_into_buffer(&mut buf, record, next_offset, &timestamp)?;
            rel_positions.push(before as u32);
            sizes.push(rec_size);
            next_offset += 1;
        }

        // Single write to the log file; this returns the absolute start position in the file.
        let start_position_abs =
            FileIo::append_data_to_end(&mut self.log_file, &buf).map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed bulk append",
                )
            })? as u32;

        // Update metadata and sparse index incrementally for each record.
        let mut current_offset = start_offset;
        for (idx, rec_size) in sizes.iter().enumerate() {
            let rec_pos_abs = start_position_abs + rel_positions[idx];
            self.update_metadata(current_offset, *rec_size);

            if self.should_add_index_entry() {
                let index_entry = IndexEntry {
                    offset: current_offset,
                    position: rec_pos_abs,
                };
                self.index.add_entry(index_entry.clone());
                let serialized_entry = self.index.serialize_entry(&index_entry, self.base_offset);
                self.index_buffer.extend_from_slice(&serialized_entry);
                if self.index_buffer.len() >= self.indexing_config.index_interval_bytes as usize {
                    self.flush_index_buffer()?;
                }
                self.bytes_since_last_index = 0;
                self.records_since_last_index = 0;

                // Time index: add only if timestamp changed from last entry to avoid heavy duplicates
                let write_time_entry = match self.time_index.last_entry() {
                    Some(last) => last.timestamp_ms != ts_ms,
                    None => true,
                };
                if write_time_entry {
                    let tentry = TimeIndexEntry {
                        timestamp_ms: ts_ms,
                        position: rec_pos_abs,
                    };
                    self.time_index.add_entry(tentry.clone());
                    self.time_index_buffer
                        .extend_from_slice(&tentry.timestamp_ms.to_be_bytes());
                    self.time_index_buffer
                        .extend_from_slice(&tentry.position.to_be_bytes());
                    if self.time_index_buffer.len()
                        >= self.indexing_config.index_interval_bytes as usize
                    {
                        self.flush_time_index_buffer()?;
                    }
                }
            }

            current_offset += 1;
        }

        // Sync if needed (Immediate mode flushes index and fsyncs files)
        self.sync_files_if_needed()?;

        Ok(current_offset - 1)
    }

    #[tracing::instrument(level = "debug", skip(self, serialized_record), fields(len = serialized_record.len()))]
    fn write_record_to_log(&mut self, serialized_record: &[u8]) -> Result<u32, StorageError> {
        // Use StdFileIO append method
        let start_position = FileIo::append_data_to_end(&mut self.log_file, serialized_record)
            .map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to append record to log file",
                )
            })? as u32;

        Ok(start_position)
    }

    fn update_metadata(&mut self, offset: u64, record_size: u32) {
        self.max_offset = Some(offset);
        self.bytes_since_last_index += record_size;
        self.records_since_last_index += 1;
    }

    // removed: update_index_if_needed (rolled into append paths)

    fn sync_files_if_needed(&mut self) -> Result<(), StorageError> {
        if matches!(self.sync_mode, SyncMode::Immediate) {
            self.flush_index_buffer()?;
            self.flush_time_index_buffer()?;

            FileIo::synchronize_to_disk(&mut self.log_file).map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to sync log file",
                )
            })?;

            FileIo::synchronize_to_disk(&mut self.index_file).map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to sync index file",
                )
            })?;
            FileIo::synchronize_to_disk(&mut self.time_index_file).map_err(|e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to sync time index file",
                )
            })?;
        }
        Ok(())
    }

    fn should_add_index_entry(&self) -> bool {
        self.bytes_since_last_index >= self.indexing_config.index_interval_bytes
            || self.records_since_last_index >= self.indexing_config.index_interval_records
    }

    #[tracing::instrument(level = "debug", skip(self), fields(bytes = self.index_buffer.len()))]
    fn flush_index_buffer(&mut self) -> Result<(), StorageError> {
        if self.index_buffer.is_empty() {
            return Ok(());
        }

        FileIo::append_data_to_end(&mut self.index_file, &self.index_buffer).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to write to index file",
            )
        })?;

        self.index_buffer.clear();
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(bytes = self.time_index_buffer.len()))]
    fn flush_time_index_buffer(&mut self) -> Result<(), StorageError> {
        if self.time_index_buffer.is_empty() {
            return Ok(());
        }

        FileIo::append_data_to_end(&mut self.time_index_file, &self.time_index_buffer).map_err(
            |e| {
                StorageError::from_io_error(
                    std::io::Error::other(e.to_string()),
                    "Failed to write to time index file",
                )
            },
        )?;

        self.time_index_buffer.clear();
        Ok(())
    }

    pub fn find_position_for_offset(&self, offset: u64) -> Option<u32> {
        self.index.find_position_for_offset(offset)
    }

    pub fn find_position_for_timestamp(&self, ts_ms: u64) -> Option<u32> {
        self.time_index.find_position_for_timestamp(ts_ms)
    }

    /// Find the nearest offset index anchor at or before the given file position.
    pub fn find_floor_position_for_file_position(&self, file_pos: u32) -> Option<u32> {
        self.index.find_floor_position_for_position(file_pos)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn size_bytes(&mut self) -> Result<u64, StorageError> {
        FileIo::get_file_size(&self.log_file).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to get log file size",
            )
        })
    }

    pub fn record_count(&self) -> usize {
        if let Some(max_offset) = self.max_offset {
            (max_offset - self.base_offset + 1) as usize
        } else {
            0
        }
    }

    pub fn contains_offset(&self, offset: u64) -> bool {
        if let Some(max_offset) = self.max_offset {
            offset >= self.base_offset && offset <= max_offset
        } else {
            false
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.flush_index_buffer()?;
        self.flush_time_index_buffer()?;

        FileIo::synchronize_to_disk(&mut self.log_file).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to sync log file",
            )
        })?;
        FileIo::synchronize_to_disk(&mut self.index_file).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to sync index file",
            )
        })?;
        FileIo::synchronize_to_disk(&mut self.time_index_file).map_err(|e| {
            StorageError::from_io_error(
                std::io::Error::other(e.to_string()),
                "Failed to sync time index file",
            )
        })?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    fn rebuild_time_index_from_log(&mut self) -> Result<(), StorageError> {
        self.time_index = SparseTimeIndex::new();
        self.time_index_buffer.clear();

        // Open the log file and iterate records from the beginning
        let mut log_file = std::fs::File::open(&self.log_path).map_err(|e| {
            StorageError::from_io_error(e, "Failed to open log for time index rebuild")
        })?;
        let mut reader = BufReader::new(&mut log_file);
        reader.seek(SeekFrom::Start(0)).map_err(|e| {
            StorageError::from_io_error(e, "Failed to seek log for time index rebuild")
        })?;

        let mut bytes_since: u32 = 0;
        let mut records_since: u32 = 0;
        let mut last_ts_ms: Option<u64> = None;

        loop {
            let start_pos = reader
                .stream_position()
                .map_err(|e| StorageError::from_io_error(e, "Failed to get stream position"))?;
            match deserialize_record(&mut reader) {
                Ok(r) => {
                    let end_pos = reader.stream_position().map_err(|e| {
                        StorageError::from_io_error(e, "Failed to get stream position after read")
                    })?;
                    let sz = (end_pos - start_pos) as u32;
                    bytes_since = bytes_since.saturating_add(sz);
                    records_since = records_since.saturating_add(1);

                    let ts_ms_i64 = chrono::DateTime::parse_from_rfc3339(&r.timestamp)
                        .map(|dt| dt.timestamp_millis())
                        .unwrap_or(0);
                    let ts_ms = if ts_ms_i64 < 0 { 0 } else { ts_ms_i64 as u64 };

                    if bytes_since >= self.indexing_config.index_interval_bytes
                        || records_since >= self.indexing_config.index_interval_records
                    {
                        let should_write = match last_ts_ms {
                            Some(v) => v != ts_ms,
                            None => true,
                        };
                        if should_write {
                            let pos_u32 = start_pos as u32;
                            let entry = TimeIndexEntry {
                                timestamp_ms: ts_ms,
                                position: pos_u32,
                            };
                            self.time_index.add_entry(entry.clone());
                            self.time_index_buffer
                                .extend_from_slice(&entry.timestamp_ms.to_be_bytes());
                            self.time_index_buffer
                                .extend_from_slice(&entry.position.to_be_bytes());
                            if self.time_index_buffer.len()
                                >= self.indexing_config.index_interval_bytes as usize
                            {
                                self.flush_time_index_buffer()?;
                            }
                            last_ts_ms = Some(ts_ms);
                        }
                        bytes_since = 0;
                        records_since = 0;
                    }
                }
                Err(_) => break, // EOF or read error; best-effort rebuild
            }
        }

        self.flush_time_index_buffer()?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    fn rebuild_offset_index_from_log(&mut self) -> Result<(), StorageError> {
        self.index = SparseIndex::new();
        self.index_buffer.clear();

        // Open the log file and iterate records from the beginning
        let mut log_file = std::fs::File::open(&self.log_path)
            .map_err(|e| StorageError::from_io_error(e, "Failed to open log for index rebuild"))?;
        let mut reader = BufReader::new(&mut log_file);
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| StorageError::from_io_error(e, "Failed to seek log for index rebuild"))?;

        let mut bytes_since: u32 = 0;
        let mut records_since: u32 = 0;

        loop {
            let start_pos = reader
                .stream_position()
                .map_err(|e| StorageError::from_io_error(e, "Failed to get stream position"))?;
            match deserialize_record(&mut reader) {
                Ok(r) => {
                    let end_pos = reader.stream_position().map_err(|e| {
                        StorageError::from_io_error(e, "Failed to get stream position after read")
                    })?;
                    let sz = (end_pos - start_pos) as u32;
                    bytes_since = bytes_since.saturating_add(sz);
                    records_since = records_since.saturating_add(1);

                    if bytes_since >= self.indexing_config.index_interval_bytes
                        || records_since >= self.indexing_config.index_interval_records
                    {
                        let entry = IndexEntry {
                            offset: r.offset,
                            position: start_pos as u32,
                        };
                        self.index.add_entry(entry.clone());
                        let serialized_entry = self.index.serialize_entry(&entry, self.base_offset);
                        self.index_buffer.extend_from_slice(&serialized_entry);
                        if self.index_buffer.len()
                            >= self.indexing_config.index_interval_bytes as usize
                        {
                            self.flush_index_buffer()?;
                        }
                        bytes_since = 0;
                        records_since = 0;
                    }
                }
                Err(_) => break, // EOF or read error; best-effort rebuild
            }
        }

        self.flush_index_buffer()?;
        Ok(())
    }
}

fn determine_max_offset(
    log_path: &PathBuf,
    index: &SparseIndex,
) -> Result<Option<u64>, StorageError> {
    let log_file = match File::open(log_path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StorageError::from_io_error(e, "Failed to open log file")),
    };

    if log_file
        .metadata()
        .map_err(|e| StorageError::from_io_error(e, "Failed to get log file metadata"))?
        .len()
        == 0
    {
        return Ok(None);
    }

    let start_pos = index.last_entry().map_or(0, |entry| entry.position as u64);
    let mut reader = BufReader::new(log_file);
    reader
        .seek(SeekFrom::Start(start_pos))
        .map_err(|e| StorageError::from_io_error(e, "Failed to seek in log file"))?;

    let last_known_offset = index.last_entry().map(|e| e.offset);

    Ok(scan_for_max_offset(&mut reader, last_known_offset))
}

fn scan_for_max_offset(
    reader: &mut BufReader<File>,
    mut last_valid_offset: Option<u64>,
) -> Option<u64> {
    while let Ok(record_with_offset) = deserialize_record(reader) {
        last_valid_offset = Some(record_with_offset.offset);
    }
    last_valid_offset
}
