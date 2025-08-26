use crate::{Record, RecordWithOffset};

/// Trait for topic log backends
///
/// This trait abstracts storage operations for topic logs, allowing different
/// implementations (in-memory, file-based, database, etc.) while maintaining
/// a consistent interface.
pub trait TopicLog: Send + Sync {
    /// Append a record to the log and return the assigned offset
    fn append(&mut self, record: Record) -> u64;

    /// Get records starting from the given offset, optionally limited by count
    fn get_records_from_offset(&self, offset: u64, count: Option<usize>) -> Vec<RecordWithOffset>;

    /// Get the total number of records in the log
    fn len(&self) -> usize;

    /// Check if the log is empty
    fn is_empty(&self) -> bool;

    /// Get the next offset that will be assigned to a new record
    fn next_offset(&self) -> u64;
}
