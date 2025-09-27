//! Error types for cluster metadata operations.

use std::fmt;

/// Main error type for cluster metadata operations.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterError {
    BrokerNotFound {
        broker_id: u32,
    },
    TopicNotFound {
        topic: String,
    },
    PartitionNotFound {
        topic: String,
        partition_id: u32,
    },
    /// Invalid manifest structure or data.
    InvalidManifest {
        context: String,
        reason: String,
    },
    /// Manifest file I/O error.
    ManifestIo {
        context: String,
        reason: String,
    },
    /// gRPC transport error.
    Transport {
        context: String,
        reason: String,
    },
    /// Invalid leader epoch (must be monotonically increasing).
    InvalidEpoch {
        topic: String,
        partition_id: u32,
        current_epoch: u64,
        new_epoch: u64,
    },
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterError::BrokerNotFound { broker_id } => {
                write!(f, "Broker with ID {broker_id} not found")
            }
            ClusterError::TopicNotFound { topic } => {
                write!(f, "Topic '{topic}' not found")
            }
            ClusterError::PartitionNotFound { topic, partition_id } => {
                write!(f, "Partition {partition_id} not found for topic '{topic}'")
            }
            ClusterError::InvalidManifest { context, reason } => {
                write!(f, "Invalid manifest in {context}: {reason}")
            }
            ClusterError::ManifestIo { context, reason } => {
                write!(f, "Manifest I/O error in {context}: {reason}")
            }
            ClusterError::Transport { context, reason } => {
                write!(f, "Transport error in {context}: {reason}")
            }
            ClusterError::InvalidEpoch {
                topic,
                partition_id,
                current_epoch,
                new_epoch,
            } => {
                write!(
                    f,
                    "Invalid epoch for topic '{topic}' partition {partition_id}: \
                     attempted {new_epoch}, current {current_epoch} (epochs must increase)"
                )
            }
        }
    }
}

impl std::error::Error for ClusterError {}

impl ClusterError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            ClusterError::BrokerNotFound { .. }
                | ClusterError::TopicNotFound { .. }
                | ClusterError::PartitionNotFound { .. }
        )
    }

    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            ClusterError::BrokerNotFound { .. }
                | ClusterError::TopicNotFound { .. }
                | ClusterError::PartitionNotFound { .. }
                | ClusterError::InvalidManifest { .. }
                | ClusterError::InvalidEpoch { .. }
        )
    }

    pub fn from_io_error(e: std::io::Error, context: &str) -> Self {
        ClusterError::ManifestIo {
            context: context.to_string(),
            reason: e.to_string(),
        }
    }

    pub fn from_parse_error(e: impl std::fmt::Display, context: &str) -> Self {
        ClusterError::InvalidManifest {
            context: context.to_string(),
            reason: e.to_string(),
        }
    }

    pub fn from_transport_error(e: impl std::fmt::Display, context: &str) -> Self {
        ClusterError::Transport {
            context: context.to_string(),
            reason: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = ClusterError::BrokerNotFound { broker_id: 42 };
        assert_eq!(error.to_string(), "Broker with ID 42 not found");

        let error = ClusterError::TopicNotFound {
            topic: "orders".to_string(),
        };
        assert_eq!(error.to_string(), "Topic 'orders' not found");

        let error = ClusterError::PartitionNotFound {
            topic: "orders".to_string(),
            partition_id: 3,
        };
        assert_eq!(
            error.to_string(),
            "Partition 3 not found for topic 'orders'"
        );
    }

    #[test]
    fn test_error_classification() {
        let not_found_error = ClusterError::BrokerNotFound { broker_id: 1 };
        assert!(not_found_error.is_not_found());
        assert!(not_found_error.is_client_error());

        let transport_error = ClusterError::Transport {
            context: "heartbeat".to_string(),
            reason: "connection refused".to_string(),
        };
        assert!(!transport_error.is_not_found());
        assert!(!transport_error.is_client_error());
    }

    #[test]
    fn test_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let cluster_error = ClusterError::from_io_error(io_error, "manifest loading");

        match cluster_error {
            ClusterError::ManifestIo { context, reason } => {
                assert_eq!(context, "manifest loading");
                assert!(reason.contains("file not found"));
            }
            _ => panic!("Unexpected error type"),
        }
    }
}