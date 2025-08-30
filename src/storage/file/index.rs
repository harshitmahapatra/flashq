use crate::error::StorageError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

pub struct SparseIndex {
    entries: Vec<IndexEntry>, // sorted by offset
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    offset: u64,   // record offset
    position: u32, // byte position in log file
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

    /// Add an index entry, maintaining sorted order by offset
    pub fn add_entry(&mut self, entry: IndexEntry) {
        // Insert in sorted position to maintain offset ordering
        match self
            .entries
            .binary_search_by_key(&entry.offset, |e| e.offset)
        {
            Ok(_) => {
                // Entry with this offset already exists, skip duplicate
            }
            Err(pos) => {
                self.entries.insert(pos, entry);
            }
        }
    }

    /// Find the byte position for a given offset using binary search
    /// Returns the position to start reading from for the given offset
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

    /// Get all entries (for debugging/testing)
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get number of index entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Write an index entry to file in Kafka's binary format
    /// Format: 8 bytes per entry (4 bytes relative offset + 4 bytes position)
    pub fn write_entry_to_file(
        &self,
        writer: &mut BufWriter<File>,
        entry: &IndexEntry,
        base_offset: u64,
    ) -> Result<(), StorageError> {
        // Store relative offset to save space (Kafka format)
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
            self.entries
                .push(IndexEntry::new(absolute_offset, position));
        }

        Ok(())
    }

    /// Write all entries to index file
    pub fn write_to_file(
        &self,
        writer: &mut BufWriter<File>,
        base_offset: u64,
    ) -> Result<(), StorageError> {
        for entry in &self.entries {
            self.write_entry_to_file(writer, entry, base_offset)?;
        }
        writer
            .flush()
            .map_err(|e| StorageError::from_io_error(e, "Failed to flush index writer"))?;
        Ok(())
    }

    /// Rebuild index by scanning log file (used for recovery)
    pub fn rebuild_from_log(
        &mut self,
        log_reader: &mut BufReader<File>,
        base_offset: u64,
        index_interval_bytes: u32,
        index_interval_records: u32,
    ) -> Result<(), StorageError> {
        self.entries.clear();

        let mut position = 0u32;
        let mut bytes_since_last_index = 0u32;
        let mut records_since_last_index = 0u32;
        let mut current_offset = base_offset;

        // This would need to be implemented with actual record parsing logic
        // For now, this is a placeholder that shows the structure
        loop {
            let start_pos = position;

            // TODO: Parse actual record from log_reader
            // This would use the existing record parsing logic from topic_log.rs
            match Self::parse_next_record(log_reader) {
                Ok(record_size) => {
                    bytes_since_last_index += record_size;
                    records_since_last_index += 1;

                    // Check if we should add an index entry
                    if bytes_since_last_index >= index_interval_bytes
                        || records_since_last_index >= index_interval_records
                    {
                        self.add_entry(IndexEntry::new(current_offset, start_pos));
                        bytes_since_last_index = 0;
                        records_since_last_index = 0;
                    }

                    position += record_size;
                    current_offset += 1;
                }
                Err(_) => break, // End of file or error
            }
        }

        Ok(())
    }

    fn parse_next_record(reader: &mut BufReader<File>) -> Result<u32, StorageError> {
        use std::io::Read;

        // Read payload size (4 bytes, big-endian) - this tells us the total record size
        let mut payload_size_bytes = [0u8; 4];
        reader
            .read_exact(&mut payload_size_bytes)
            .map_err(|e| StorageError::from_io_error(e, "Failed to read payload size"))?;
        let payload_size = u32::from_be_bytes(payload_size_bytes);

        // Skip the rest of the record (offset + payload)
        // Total record size = 4 bytes (payload_size) + 8 bytes (offset) + payload_size
        let skip_bytes = 8 + payload_size; // 8 bytes for offset + payload
        let mut skip_buffer = vec![0u8; skip_bytes as usize];
        reader
            .read_exact(&mut skip_buffer)
            .map_err(|e| StorageError::from_io_error(e, "Failed to skip record content"))?;

        // Return total record size including the header
        Ok(4 + 8 + payload_size) // payload_size_field + offset_field + payload
    }
}

impl IndexEntry {
    pub fn new(offset: u64, position: u32) -> Self {
        Self { offset, position }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn position(&self) -> u32 {
        self.position
    }
}
