use crate::error::StorageError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub offset: u64,
    pub position: u32,
}

pub struct SparseIndex {
    entries: Vec<IndexEntry>, // sorted by offset
}

impl Default for SparseIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, entry: IndexEntry) {
        match self
            .entries
            .binary_search_by_key(&entry.offset, |e| e.offset)
        {
            Ok(_) => {}
            Err(pos) => {
                self.entries.insert(pos, entry);
            }
        }
    }

    pub fn find_position_for_offset(&self, target_offset: u64) -> Option<u32> {
        if self.entries.is_empty() {
            return Some(0); // Start from beginning if no index
        }

        match self
            .entries
            .binary_search_by_key(&target_offset, |entry| entry.offset)
        {
            Ok(idx) => Some(self.entries[idx].position), // Exact match
            Err(idx) => {
                if idx == 0 {
                    Some(0) // Target is before first indexed offset
                } else {
                    Some(self.entries[idx - 1].position) // Start from previous entry
                }
            }
        }
    }

    pub fn write_entry_to_file(
        &self,
        writer: &mut BufWriter<File>,
        entry: &IndexEntry,
        base_offset: u64,
    ) -> Result<(), StorageError> {
        let relative_offset = (entry.offset - base_offset) as u32;

        writer
            .write_all(&relative_offset.to_be_bytes())
            .map_err(|e| {
                StorageError::from_io_error(e, "Failed to write relative offset to index file")
            })?;
        writer
            .write_all(&entry.position.to_be_bytes())
            .map_err(|e| {
                StorageError::from_io_error(e, "Failed to write position to index file")
            })?;
        Ok(())
    }

    /// Read index from file and populate the sparse index
    pub fn read_from_file(
        &mut self,
        reader: &mut BufReader<File>,
        base_offset: u64,
    ) -> Result<(), StorageError> {
        self.entries.clear();

        let mut buffer = [0u8; 8]; // 4 bytes offset + 4 bytes position

        while reader.read_exact(&mut buffer).is_ok() {
            let relative_offset = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            let position = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);

            let absolute_offset = base_offset + relative_offset as u64;
            self.entries.push(IndexEntry {
                offset: absolute_offset,
                position,
            });
        }

        Ok(())
    }

    pub fn last_entry(&self) -> Option<&IndexEntry> {
        self.entries.last()
    }
}
