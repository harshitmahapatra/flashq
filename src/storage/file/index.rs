use crate::error::StorageError;
use std::io::{BufReader, BufWriter, Read, Write};

#[derive(Debug, Clone, PartialEq)]
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

    pub fn write_entry_to_file<W: Write>(
        &self,
        writer: &mut BufWriter<W>,
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
    pub fn read_from_file<R: Read>(
        &mut self,
        reader: &mut BufReader<R>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_new_index_is_empty() {
        let index = SparseIndex::new();
        assert!(index.entries.is_empty());
        assert_eq!(index.last_entry(), None);
    }

    #[test]
    fn test_add_entry_maintains_sorted_order() {
        let mut index = SparseIndex::new();
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        index.add_entry(IndexEntry {
            offset: 5,
            position: 50,
        });
        index.add_entry(IndexEntry {
            offset: 15,
            position: 150,
        });

        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.entries[0].offset, 5);
        assert_eq!(index.entries[1].offset, 10);
        assert_eq!(index.entries[2].offset, 15);
    }

    #[test]
    fn test_add_entry_duplicate_offset_is_ignored() {
        let mut index = SparseIndex::new();
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        index.add_entry(IndexEntry {
            offset: 10,
            position: 200,
        });

        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.entries[0].position, 100);
    }

    #[test]
    fn test_find_position_for_offset_empty_index() {
        let index = SparseIndex::new();
        assert_eq!(index.find_position_for_offset(100), Some(0));
    }

    #[test]
    fn test_find_position_for_offset_exact_match() {
        let mut index = SparseIndex::new();
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        index.add_entry(IndexEntry {
            offset: 20,
            position: 200,
        });
        assert_eq!(index.find_position_for_offset(20), Some(200));
    }

    #[test]
    fn test_find_position_for_offset_no_exact_match() {
        let mut index = SparseIndex::new();
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        index.add_entry(IndexEntry {
            offset: 20,
            position: 200,
        });
        assert_eq!(index.find_position_for_offset(15), Some(100));
    }

    #[test]
    fn test_find_position_for_offset_before_first_entry() {
        let mut index = SparseIndex::new();
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        assert_eq!(index.find_position_for_offset(5), Some(0));
    }

    #[test]
    fn test_last_entry() {
        let mut index = SparseIndex::new();
        assert_eq!(index.last_entry(), None);
        index.add_entry(IndexEntry {
            offset: 10,
            position: 100,
        });
        assert_eq!(
            index.last_entry(),
            Some(&IndexEntry {
                offset: 10,
                position: 100
            })
        );
        index.add_entry(IndexEntry {
            offset: 20,
            position: 200,
        });
        assert_eq!(
            index.last_entry(),
            Some(&IndexEntry {
                offset: 20,
                position: 200
            })
        );
    }

    #[test]
    fn test_read_write_entry_roundtrip() {
        let index = SparseIndex::new();
        let entry1 = IndexEntry {
            offset: 10,
            position: 100,
        };
        let entry2 = IndexEntry {
            offset: 20,
            position: 200,
        };
        let base_offset = 0;

        let mut buffer = Vec::new();
        {
            let mut writer = BufWriter::new(Cursor::new(&mut buffer));
            index
                .write_entry_to_file(&mut writer, &entry1, base_offset)
                .unwrap();
            index
                .write_entry_to_file(&mut writer, &entry2, base_offset)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = BufReader::new(Cursor::new(buffer));
        let mut new_index = SparseIndex::new();
        new_index.read_from_file(&mut reader, base_offset).unwrap();

        assert_eq!(new_index.entries.len(), 2);
        assert_eq!(new_index.entries[0], entry1);
        assert_eq!(new_index.entries[1], entry2);
    }
}
