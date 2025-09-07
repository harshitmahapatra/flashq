use crate::Record;
use crate::error::StorageError;
use crate::storage::file::common::{SyncMode, deserialize_record, serialize_record};
use crate::storage::file::file_io::FileIo;
use crate::storage::file::index::{IndexEntry, SparseIndex};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct IndexingConfig {
    pub index_interval_bytes: u32,
    pub index_interval_records: u32,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            index_interval_bytes: 4096,
            index_interval_records: 100,
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
    bytes_since_last_index: u32,
    records_since_last_index: u32,
    sync_mode: SyncMode,
    indexing_config: IndexingConfig,
}

impl LogSegment {
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

        Ok(LogSegment {
            base_offset,
            max_offset: None,
            log_path,
            index_path,
            log_file,
            index_file,
            index_buffer: Vec::new(),
            index: SparseIndex::new(),
            bytes_since_last_index: 0,
            records_since_last_index: 0,
            sync_mode,
            indexing_config,
        })
    }

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
            segment
                .index
                .read_from_file(&mut index_reader, base_offset)?;
        }

        segment.max_offset = determine_max_offset(&segment.log_path, &segment.index)?;

        Ok(segment)
    }

    pub fn append_record(&mut self, record: &Record, offset: u64) -> Result<(), StorageError> {
        let serialized = serialize_record(record, offset)?;
        let start_position = self.write_record_to_log(&serialized)?;

        self.update_metadata(offset, serialized.len() as u32);
        self.update_index_if_needed(offset, start_position)?;
        self.sync_files_if_needed()
    }

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

    fn update_index_if_needed(
        &mut self,
        offset: u64,
        start_position: u32,
    ) -> Result<(), StorageError> {
        if self.should_add_index_entry() {
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

            self.bytes_since_last_index = 0;
            self.records_since_last_index = 0;
        }
        Ok(())
    }

    fn sync_files_if_needed(&mut self) -> Result<(), StorageError> {
        if matches!(self.sync_mode, SyncMode::Immediate) {
            self.flush_index_buffer()?;

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
        }
        Ok(())
    }

    fn should_add_index_entry(&self) -> bool {
        self.bytes_since_last_index >= self.indexing_config.index_interval_bytes
            || self.records_since_last_index >= self.indexing_config.index_interval_records
    }

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

    pub fn find_position_for_offset(&self, offset: u64) -> Option<u32> {
        self.index.find_position_for_offset(offset)
    }

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

    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.flush_index_buffer()?;

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
