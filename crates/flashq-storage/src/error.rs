use std::fmt;

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
    InvalidTopic(String),
    LockAcquisitionFailed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StorageErrorSource {
    Io(String),
    Serialization(String),
    Network(String),
    Custom(String),
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
            StorageError::InvalidTopic(topic) => write!(f, "Invalid topic: {topic}"),
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

impl std::error::Error for StorageError {}
impl std::error::Error for StorageErrorSource {}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_storage_error_correctly() {
        let error = StorageError::InsufficientSpace {
            context: "disk".to_string(),
        };
        assert_eq!(error.to_string(), "Insufficient space in disk");
    }

    #[test]
    fn converts_io_error_to_storage_error() {
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
