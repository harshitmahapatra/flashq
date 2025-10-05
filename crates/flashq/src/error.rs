use flashq_storage::StorageError;
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

impl From<FlashQError> for HttpError {
    fn from(err: FlashQError) -> Self {
        HttpError::Domain(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_flashq_error_correctly() {
        let error = FlashQError::TopicNotFound {
            topic: "test".to_string(),
        };
        assert_eq!(error.to_string(), "Topic 'test' not found");
    }

    #[test]
    fn converts_storage_error_to_flashq_error() {
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
}
