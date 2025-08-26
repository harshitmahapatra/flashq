//! Storage abstraction for FlashQ
//!
//! This module provides storage abstraction allowing different persistence backends
//! while maintaining the same interface. Currently supports in-memory storage with
//! future extensibility for file-based or database storage.
//!
//! # Example
//! ```
//! use flashq::storage::{InMemoryTopicLog, TopicLogStorage};
//!
//! let mut log = InMemoryTopicLog::new();
//! // Use through trait interface
//! ```

pub mod backend;
pub mod memory;
pub mod r#trait;

pub use backend::StorageBackend;
pub use memory::InMemoryTopicLog;
pub use r#trait::TopicLog;
