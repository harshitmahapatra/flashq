use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum FlashQError {
    TopicNotFound {
        topic: String,
    },
    ConsumerGroupNotFound {
        group_id: String,
    },
    ConsumerGroupAlreadyExists {
        group_id: String,
    },
    ConsumerGroupCreationFailed {
        group_id: String,
        reason: String,
    },
    InvalidOffset {
        offset: u64,
        topic: String,
        max_offset: u64,
    },
    Storage(StorageError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    ReadFailed {
        context: String,
        source: Box<StorageErrorSource>,
    },
    WriteFailed {
        context: String,
        source: Box<StorageErrorSource>,
    },
    InsufficientSpace {
        context: String,
    },
    PermissionDenied {
        context: String,
    },
    DataCorruption {
        context: String,
        details: String,
    },
    Unavailable {
        context: String,
    },
    DirectoryLocked {
        context: String,
        pid: Option<u32>,
    },
    LockAcquisitionFailed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StorageErrorSource {
    Io(String),
    Serialization(String),
    Network(String),
    Custom(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum HttpError {
    Validation { field: String, message: String },
    Domain(FlashQError),
    Internal { message: String },
}

impl fmt::Display for FlashQError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlashQError::TopicNotFound { topic } => write!(f, "Topic '{topic}' not found"),
            FlashQError::ConsumerGroupNotFound { group_id } => {
                write!(f, "Consumer group '{group_id}' not found")
            }
            FlashQError::ConsumerGroupAlreadyExists { group_id } => {
                write!(f, "Consumer group '{group_id}' already exists")
            }
            FlashQError::ConsumerGroupCreationFailed { group_id, reason } => {
                write!(f, "Failed to create consumer group '{group_id}': {reason}")
            }
            FlashQError::InvalidOffset {
                offset,
                topic,
                max_offset,
            } => {
                write!(
                    f,
                    "Invalid offset {offset} for topic '{topic}', max offset is {max_offset}"
                )
            }
            FlashQError::Storage(err) => write!(f, "Storage error: {err}"),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::ReadFailed { context, source } => {
                write!(f, "Read failed in {context}: {source}")
            }
            StorageError::WriteFailed { context, source } => {
                write!(f, "Write failed in {context}: {source}")
            }
            StorageError::InsufficientSpace { context } => {
                write!(f, "Insufficient space in {context}")
            }
            StorageError::PermissionDenied { context } => {
                write!(f, "Permission denied in {context}")
            }
            StorageError::DataCorruption { context, details } => {
                write!(f, "Data corruption in {context}: {details}")
            }
            StorageError::Unavailable { context } => {
                write!(f, "Storage unavailable in {context}")
            }
            StorageError::DirectoryLocked { context, pid } => match pid {
                Some(pid) => write!(f, "Directory locked in {context} (PID: {pid})"),
                None => write!(f, "Directory locked in {context}"),
            },
            StorageError::LockAcquisitionFailed => {
                write!(f, "Failed to acquire exclusive lock on file")
            }
        }
    }
}

impl fmt::Display for StorageErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageErrorSource::Io(msg) => write!(f, "IO error: {msg}"),
            StorageErrorSource::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            StorageErrorSource::Network(msg) => write!(f, "Network error: {msg}"),
            StorageErrorSource::Custom(msg) => write!(f, "Custom error: {msg}"),
        }
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Validation { field, message } => {
                write!(f, "Validation error in field '{field}': {message}")
            }
            HttpError::Domain(err) => write!(f, "{err}"),
            HttpError::Internal { message } => write!(f, "Internal error: {message}"),
        }
    }
}

impl std::error::Error for FlashQError {}
impl std::error::Error for StorageError {}
impl std::error::Error for StorageErrorSource {}
impl std::error::Error for HttpError {}

impl FlashQError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            FlashQError::TopicNotFound { .. }
                | FlashQError::ConsumerGroupNotFound { .. }
                | FlashQError::InvalidOffset { .. }
        )
    }

    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            FlashQError::TopicNotFound { .. }
                | FlashQError::ConsumerGroupNotFound { .. }
                | FlashQError::ConsumerGroupAlreadyExists { .. }
                | FlashQError::ConsumerGroupCreationFailed { .. }
                | FlashQError::InvalidOffset { .. }
        )
    }
}

impl From<StorageError> for FlashQError {
    fn from(err: StorageError) -> Self {
        FlashQError::Storage(err)
    }
}

impl StorageError {
    pub fn from_io_error(e: std::io::Error, context: &str) -> Self {
        match e.kind() {
            std::io::ErrorKind::PermissionDenied => StorageError::PermissionDenied {
                context: context.to_string(),
            },
            std::io::ErrorKind::OutOfMemory => StorageError::InsufficientSpace {
                context: context.to_string(),
            },
            _ => StorageError::WriteFailed {
                context: context.to_string(),
                source: Box::new(StorageErrorSource::Io(e.to_string())),
            },
        }
    }

    pub fn from_serialization_error(e: impl std::fmt::Display, context: &str) -> Self {
        StorageError::DataCorruption {
            context: context.to_string(),
            details: e.to_string(),
        }
    }
}

impl From<FlashQError> for HttpError {
    fn from(err: FlashQError) -> Self {
        HttpError::Domain(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = FlashQError::TopicNotFound {
            topic: "test".to_string(),
        };
        assert_eq!(error.to_string(), "Topic 'test' not found");

        let storage_error = StorageError::InsufficientSpace {
            context: "disk".to_string(),
        };
        assert_eq!(storage_error.to_string(), "Insufficient space in disk");
    }

    #[test]
    fn test_error_conversions() {
        let storage_error = StorageError::InsufficientSpace {
            context: "disk".to_string(),
        };
        let flashq_error: FlashQError = storage_error.into();

        match flashq_error {
            FlashQError::Storage(StorageError::InsufficientSpace { context }) => {
                assert_eq!(context, "disk");
            }
            _ => panic!("Conversion failed"),
        }
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let storage_error = StorageError::from_io_error(io_error, "file write");

        match storage_error {
            StorageError::PermissionDenied { context } => {
                assert_eq!(context, "file write");
            }
            _ => panic!("IO error conversion failed"),
        }
    }
}
