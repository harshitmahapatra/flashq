use std::collections::HashMap;

pub mod error;
pub mod storage;

pub use error::{StorageError, StorageErrorSource};
pub use storage::{
    backend::StorageBackend,
    r#trait::{ConsumerGroup, ConsumerOffsetStore, PartitionId, TopicLog},
};

pub mod file {
    pub use crate::storage::file::*;
}

pub use file::SyncMode;

pub mod backend {
    pub use crate::storage::backend::*;
}

pub mod memory {
    pub use crate::storage::memory::*;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RecordWithOffset {
    #[serde(flatten)]
    pub record: Record,
    pub offset: u64,
    pub timestamp: String,
}

impl Record {
    pub fn new(
        key: Option<String>,
        value: String,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
        }
    }
}

impl RecordWithOffset {
    pub fn from_record(record: Record, offset: u64) -> Self {
        let timestamp = chrono::Utc::now().to_rfc3339();
        Self {
            record,
            offset,
            timestamp,
        }
    }
}
