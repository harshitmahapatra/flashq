use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// PRODUCER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct PostRecordRequest {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
pub struct PostRecordResponse {
    pub offset: u64,
    pub timestamp: String,
}

// =============================================================================
// CONSUMER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    pub count: Option<usize>,
    pub from_offset: Option<u64>,
    pub max_records: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub include_headers: Option<bool>,
}

impl PollQuery {
    pub fn effective_limit(&self) -> Option<usize> {
        self.max_records.or(self.count)
    }

    pub fn effective_timeout_ms(&self) -> u64 {
        self.timeout_ms.unwrap_or(1000)
    }

    pub fn should_include_headers(&self) -> bool {
        self.include_headers.unwrap_or(true)
    }
}

#[derive(Serialize, Deserialize)]
pub struct RecordResponse {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
    pub offset: u64,
    pub timestamp: String,
}

#[derive(Serialize, Deserialize)]
pub struct PollRecordsResponse {
    pub records: Vec<RecordResponse>,
    pub count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct FetchResponse {
    pub records: Vec<RecordResponse>,
    pub next_offset: u64,
    pub high_water_mark: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
}

impl FetchResponse {
    pub fn new(records: Vec<RecordResponse>, next_offset: u64, high_water_mark: u64) -> Self {
        let lag = if high_water_mark >= next_offset {
            Some(high_water_mark - next_offset)
        } else {
            None
        };

        Self {
            records,
            next_offset,
            high_water_mark,
            lag,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ConsumerGroupResponse {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateConsumerGroupOffsetRequest {
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct GetConsumerGroupOffsetResponse {
    pub group_id: String,
    pub topic: String,
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct OffsetResponse {
    pub topic: String,
    pub committed_offset: u64,
    pub high_water_mark: u64,
    pub lag: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_time: Option<String>,
}

impl OffsetResponse {
    pub fn new(
        topic: String,
        committed_offset: u64,
        high_water_mark: u64,
        last_commit_time: Option<String>,
    ) -> Self {
        let lag = high_water_mark.saturating_sub(committed_offset);

        Self {
            topic,
            committed_offset,
            high_water_mark,
            lag,
            last_commit_time,
        }
    }
}

// =============================================================================
// SHARED/ERROR TYPES
// =============================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ErrorResponse {
    pub fn new(error: &str, message: &str) -> Self {
        Self {
            error: error.to_string(),
            message: message.to_string(),
            details: None,
        }
    }

    pub fn with_details(error: &str, message: &str, details: serde_json::Value) -> Self {
        Self {
            error: error.to_string(),
            message: message.to_string(),
            details: Some(details),
        }
    }

    // Validation error creation methods
    pub fn validation_error(message: &str) -> Self {
        Self::new("validation_error", message)
    }

    pub fn invalid_parameter(param_name: &str, message: &str) -> Self {
        Self::with_details(
            "invalid_parameter",
            message,
            serde_json::json!({ "parameter": param_name }),
        )
    }

    pub fn topic_not_found(topic: &str) -> Self {
        Self::with_details(
            "topic_not_found",
            &format!("Topic '{topic}' not found"),
            serde_json::json!({ "topic": topic }),
        )
    }

    pub fn group_not_found(group_id: &str) -> Self {
        Self::with_details(
            "group_not_found",
            &format!("Consumer group '{group_id}' not found"),
            serde_json::json!({ "group_id": group_id }),
        )
    }

    pub fn internal_error(message: &str) -> Self {
        Self::new("internal_error", message)
    }

    pub fn record_size_error(field: &str, max_size: usize, actual_size: usize) -> Self {
        Self::with_details(
            "validation_error",
            &format!("{field} exceeds maximum length of {max_size} (got {actual_size})"),
            serde_json::json!({
                "field": field,
                "max_size": max_size,
                "actual_size": actual_size
            }),
        )
    }

    pub fn invalid_topic_name(topic: &str) -> Self {
        Self::with_details(
            "invalid_parameter",
            "Topic name must contain only alphanumeric characters, dots, underscores, and hyphens",
            serde_json::json!({ "parameter": "topic", "value": topic }),
        )
    }

    pub fn invalid_consumer_group_id(group_id: &str) -> Self {
        Self::with_details(
            "invalid_parameter",
            "Consumer group ID must contain only alphanumeric characters, dots, underscores, and hyphens",
            serde_json::json!({ "parameter": "group_id", "value": group_id }),
        )
    }
}
