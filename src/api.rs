use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PostMessageRequest {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
pub struct PostMessageResponse {
    pub offset: u64,       // replaces 'id'
    pub timestamp: String, // ISO 8601 format, replaces u64
}

#[derive(Serialize, Deserialize)]
pub struct MessageResponse {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
    pub offset: u64,       // replaces 'id'
    pub timestamp: String, // ISO 8601 format, replaces u64
}

#[derive(Serialize, Deserialize)]
pub struct PollMessagesResponse {
    pub messages: Vec<MessageResponse>,
    pub count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    // Existing parameters for backward compatibility
    pub count: Option<usize>,
    pub from_offset: Option<u64>,

    // Phase 2: Enhanced query parameters following OpenAPI spec
    pub max_records: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub include_headers: Option<bool>,
}

impl PollQuery {
    /// Get the effective record limit, preferring max_records over count
    pub fn effective_limit(&self) -> Option<usize> {
        self.max_records.or(self.count)
    }

    /// Get timeout with default fallback
    pub fn effective_timeout_ms(&self) -> u64 {
        self.timeout_ms.unwrap_or(1000) // Default: 1 second
    }

    /// Whether to include headers in response
    pub fn should_include_headers(&self) -> bool {
        self.include_headers.unwrap_or(true) // Default: true
    }
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

// Consumer Group API Types
#[derive(Serialize, Deserialize)]
pub struct CreateConsumerGroupRequest {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateConsumerGroupResponse {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetConsumerGroupOffsetResponse {
    pub group_id: String,
    pub topic: String,
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateConsumerGroupOffsetRequest {
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct FetchResponse {
    pub records: Vec<MessageResponse>,
    pub next_offset: u64,
    /// Highest offset available in the topic
    pub high_water_mark: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
}

impl FetchResponse {
    /// Create a new FetchResponse with automatic lag calculation
    pub fn new(records: Vec<MessageResponse>, next_offset: u64, high_water_mark: u64) -> Self {
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
    /// Consumer group identifier
    pub group_id: String,
    /// Current state of the consumer group
    pub state: ConsumerGroupState,
    /// Partition assignment protocol (default: "range")
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// Number of active members in the group
    pub members: u32,
}

/// Consumer group states following Kafka conventions
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerGroupState {
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
}

impl Default for ConsumerGroupState {
    fn default() -> Self {
        ConsumerGroupState::Stable
    }
}

/// Enhanced offset response with lag calculations and metadata
#[derive(Serialize, Deserialize)]
pub struct OffsetResponse {
    /// Topic name
    pub topic: String,
    /// Current committed offset
    pub committed_offset: u64,
    /// Highest available offset in the topic
    pub high_water_mark: u64,
    /// Number of messages behind high water mark
    pub lag: u64,
    /// When the offset was last committed (ISO 8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_time: Option<String>,
}

impl OffsetResponse {
    /// Create a new OffsetResponse with automatic lag calculation
    pub fn new(
        topic: String,
        committed_offset: u64,
        high_water_mark: u64,
        last_commit_time: Option<String>,
    ) -> Self {
        let lag = if high_water_mark >= committed_offset {
            high_water_mark - committed_offset
        } else {
            0
        };

        Self {
            topic,
            committed_offset,
            high_water_mark,
            lag,
            last_commit_time,
        }
    }
}

fn default_protocol() -> String {
    "range".to_string()
}
