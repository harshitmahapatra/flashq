//! Storage abstraction for FlashQ
//!
//! This module provides storage abstraction allowing different persistence backends
//! while maintaining the same interface. Currently supports in-memory storage with
//! future extensibility for file-based or database storage.
//!
//! # Example
//! ```
//! use flashq::storage::{InMemoryTopicLog, TopicLog};
//! use flashq::Record;
//!
//! let mut log = InMemoryTopicLog::new();
//!
//! // Append records and get their offsets
//! let record1 = Record::new(None, "first message".to_string(), None);
//! let record2 = Record::new(Some("key123".to_string()), "second message".to_string(), None);
//!
//! let offset1 = log.append(record1);
//! let offset2 = log.append(record2);
//! assert_eq!(offset1, 0);
//! assert_eq!(offset2, 1);
//!
//! // Retrieve records from a specific offset (returns owned data)
//! let records = log.get_records_from_offset(0, None);
//! assert_eq!(records.len(), 2);
//! assert_eq!(records[0].record.value, "first message");
//! assert_eq!(records[1].record.key.as_ref().unwrap(), "key123");
//!
//! // Get records with count limit
//! let limited_records = log.get_records_from_offset(0, Some(1));
//! assert_eq!(limited_records.len(), 1);
//! ```

pub mod backend;
pub mod memory;
pub mod r#trait;

pub use backend::StorageBackend;
pub use memory::InMemoryTopicLog;
pub use r#trait::TopicLog;
