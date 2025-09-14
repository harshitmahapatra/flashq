use crate::error::StorageError;
use std::io::{BufReader, BufWriter, Read, Write};

#[derive(Debug, Clone, PartialEq)]
pub struct TimeIndexEntry {
    pub timestamp_ms: u64,
    pub position: u32,
}

/// Sparse time index maintained per segment, sorted by `timestamp_ms`.
pub struct SparseTimeIndex {
    entries: Vec<TimeIndexEntry>,
}

impl Default for SparseTimeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseTimeIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Insert a new entry while maintaining sorted order by timestamp.
    /// For duplicate timestamps, keep the earliest position (min position).
    pub fn add_entry(&mut self, entry: TimeIndexEntry) {
        match self
            .entries
            .binary_search_by_key(&entry.timestamp_ms, |e| e.timestamp_ms)
        {
            Ok(idx) => {
                // Duplicate timestamp: keep the earliest position
                if entry.position < self.entries[idx].position {
                    self.entries[idx].position = entry.position;
                }
            }
            Err(pos) => self.entries.insert(pos, entry),
        }
    }

    /// Find a starting file position for the given timestamp (in ms).
    /// Returns:
    /// - Some(0) if the index is empty or the target is before the first entry
    /// - Some(position) of the exact timestamp entry if found
    /// - Some(position) of the closest preceding entry otherwise
    pub fn find_position_for_timestamp(&self, ts_ms: u64) -> Option<u32> {
        if self.entries.is_empty() {
            return Some(0);
        }

        match self
            .entries
            .binary_search_by_key(&ts_ms, |entry| entry.timestamp_ms)
        {
            Ok(idx) => Some(self.entries[idx].position),
            Err(idx) => {
                if idx == 0 {
                    Some(0)
                } else {
                    Some(self.entries[idx - 1].position)
                }
            }
        }
    }

    /// Serialize and write a single entry to file (big-endian):
    /// [8B timestamp_ms][4B position]
    pub fn write_entry_to_file<W: Write>(
        &self,
        writer: &mut BufWriter<W>,
        entry: &TimeIndexEntry,
    ) -> Result<(), StorageError> {
        writer
            .write_all(&entry.timestamp_ms.to_be_bytes())
            .map_err(|e| {
                StorageError::from_io_error(e, "Failed to write timestamp to time index")
            })?;
        writer
            .write_all(&entry.position.to_be_bytes())
            .map_err(|e| {
                StorageError::from_io_error(e, "Failed to write position to time index")
            })?;
        Ok(())
    }

    /// Read the time index from file and populate the sparse index.
    /// An optional `max_entries` bound can be provided to prevent excessive allocations
    /// in case of a corrupted or unexpectedly large file. If the bound is exceeded,
    /// returns a DataCorruption error.
    pub fn read_from_file<R: Read>(
        &mut self,
        reader: &mut BufReader<R>,
        max_entries: Option<usize>,
    ) -> Result<(), StorageError> {
        self.entries.clear();

        let max_allowed = max_entries.unwrap_or(usize::MAX);
        let mut count: usize = 0;

        let mut buffer = [0u8; 12]; // 8 bytes timestamp_ms + 4 bytes position
        while reader.read_exact(&mut buffer).is_ok() {
            if count >= max_allowed {
                return Err(StorageError::DataCorruption {
                    context: "time index read".to_string(),
                    details: format!("time index entry count exceeded limit: {max_allowed}"),
                });
            }

            let ts_ms = u64::from_be_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]);
            let position = u32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
            self.entries.push(TimeIndexEntry {
                timestamp_ms: ts_ms,
                position,
            });
            count += 1;
        }

        Ok(())
    }

    pub fn last_entry(&self) -> Option<&TimeIndexEntry> {
        self.entries.last()
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_new_time_index_is_empty() {
        let idx = SparseTimeIndex::new();
        assert_eq!(idx.len(), 0);
        assert!(idx.last_entry().is_none());
    }

    #[test]
    fn test_add_entry_sorted_and_dedup() {
        let mut idx = SparseTimeIndex::new();
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 20,
            position: 200,
        });
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 10,
            position: 100,
        });
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 15,
            position: 150,
        });
        // duplicate timestamp, ignored
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 10,
            position: 123,
        });

        assert_eq!(idx.len(), 3);
        assert_eq!(
            idx.last_entry(),
            Some(&TimeIndexEntry {
                timestamp_ms: 20,
                position: 200
            })
        );
    }

    #[test]
    fn test_find_position_for_timestamp_behaviour() {
        let mut idx = SparseTimeIndex::new();
        // empty index → position 0
        assert_eq!(idx.find_position_for_timestamp(5), Some(0));

        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 10,
            position: 100,
        });
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 20,
            position: 200,
        });
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 30,
            position: 300,
        });

        // exact match
        assert_eq!(idx.find_position_for_timestamp(20), Some(200));
        // before first
        assert_eq!(idx.find_position_for_timestamp(5), Some(0));
        // between
        assert_eq!(idx.find_position_for_timestamp(25), Some(200));
        // after last should return last known position (scan from here)
        assert_eq!(idx.find_position_for_timestamp(35), Some(300));
    }

    #[test]
    fn test_read_write_roundtrip() {
        let idx = SparseTimeIndex::new();
        let entry1 = TimeIndexEntry {
            timestamp_ms: 1000,
            position: 10,
        };
        let entry2 = TimeIndexEntry {
            timestamp_ms: 2000,
            position: 20,
        };

        let mut buf = Vec::new();
        {
            let mut writer = BufWriter::new(Cursor::new(&mut buf));
            idx.write_entry_to_file(&mut writer, &entry1).unwrap();
            idx.write_entry_to_file(&mut writer, &entry2).unwrap();
            writer.flush().unwrap();
        }

        let mut reader = BufReader::new(Cursor::new(buf));
        let mut idx2 = SparseTimeIndex::new();
        idx2.read_from_file(&mut reader, None).unwrap();

        assert_eq!(idx2.len(), 2);
        assert_eq!(idx2.last_entry(), Some(&entry2));
        assert_eq!(idx2.find_position_for_timestamp(1500), Some(10)); // previous entry position
        assert_eq!(idx2.find_position_for_timestamp(2000), Some(20));
    }

    #[test]
    fn test_duplicate_timestamp_keeps_earliest_position() {
        let mut idx = SparseTimeIndex::new();
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 1000,
            position: 500,
        });
        // Duplicate with later position should not override
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 1000,
            position: 600,
        });
        assert_eq!(idx.find_position_for_timestamp(1000), Some(500));
        // Duplicate with earlier position should override to earliest
        idx.add_entry(TimeIndexEntry {
            timestamp_ms: 1000,
            position: 450,
        });
        assert_eq!(idx.find_position_for_timestamp(1000), Some(450));
    }
}
