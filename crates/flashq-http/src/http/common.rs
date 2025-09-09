//! HTTP API request and response types

use flashq::{FlashQError, Record, RecordWithOffset};
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

pub fn print_record(record: &RecordWithOffset) {
    print!(
        "{} [{}] {}",
        record.timestamp, record.offset, record.record.value
    );

    if let Some(ref key) = record.record.key {
        print!(" (key: {key})");
    }

    if let Some(ref headers) = record.record.headers {
        if !headers.is_empty() {
            print!(" (headers: {headers:?})");
        }
    }

    println!();
}

// =============================================================================
// PRODUCER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct ProduceRequest {
    pub records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
pub struct ProduceResponse {
    pub offset: u64,
    pub timestamp: String,
}

// =============================================================================
// CONSUMER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct ConsumerGroupParams {
    pub group_id: String,
    pub topic: String,
}

#[derive(Serialize, Deserialize)]
pub struct ConsumerGroupIdParams {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct PollOffsetQuery {
    pub from_offset: Option<u64>,
    pub max_records: Option<usize>,
    pub include_headers: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct PollTimeQuery {
    pub from_time: String,
    pub max_records: Option<usize>,
    pub include_headers: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    pub from_offset: Option<u64>,
    pub from_time: Option<String>,
    pub max_records: Option<usize>,
    pub include_headers: Option<bool>,
}

impl PollQuery {
    pub fn effective_limit(&self) -> Option<usize> {
        self.max_records
    }
    pub fn should_include_headers(&self) -> bool {
        self.include_headers.unwrap_or(true)
    }
}

#[derive(Serialize, Deserialize)]
pub struct FetchResponse {
    pub records: Vec<RecordWithOffset>,
    pub next_offset: u64,
    pub high_water_mark: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
}

impl FetchResponse {
    pub fn new(records: Vec<RecordWithOffset>, next_offset: u64, high_water_mark: u64) -> Self {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Utility function to parse header strings into HashMap
pub fn parse_headers(header_strings: Option<Vec<String>>) -> Option<HashMap<String, String>> {
    header_strings.map(|headers| {
        headers
            .iter()
            .filter_map(|h| {
                let mut split = h.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => {
                        warn!("Invalid header format '{h}', expected KEY=VALUE");
                        None
                    }
                }
            })
            .collect()
    })
}

pub mod limits {
    pub const MAX_KEY_SIZE: usize = 1024;
    pub const MAX_VALUE_SIZE: usize = 1_048_576;
    pub const MAX_HEADER_VALUE_SIZE: usize = 1024;
    pub const MAX_BATCH_SIZE: usize = 1000;
    pub const MAX_POLL_RECORDS: usize = 10000;
}

pub fn validate_record(record: &Record, index: usize) -> Result<(), ErrorResponse> {
    if let Some(key) = &record.key {
        if key.len() > limits::MAX_KEY_SIZE {
            return Err(ErrorResponse::with_details(
                "validation_error",
                &format!(
                    "Record at index {} key exceeds maximum length of {} characters (got {})",
                    index,
                    limits::MAX_KEY_SIZE,
                    key.len()
                ),
                serde_json::json!({
                    "field": format!("records[{}].key", index), "max_size": limits::MAX_KEY_SIZE, "actual_size": key.len()
                }),
            ));
        }
    }
    if record.value.len() > limits::MAX_VALUE_SIZE {
        return Err(ErrorResponse::with_details(
            "validation_error",
            &format!(
                "Record at index {} value exceeds maximum length of {} characters (got {})",
                index,
                limits::MAX_VALUE_SIZE,
                record.value.len()
            ),
            serde_json::json!({
                "field": format!("records[{}].value", index), "max_size": limits::MAX_VALUE_SIZE, "actual_size": record.value.len()
            }),
        ));
    }
    if let Some(headers) = &record.headers {
        for (header_key, header_value) in headers {
            if header_value.len() > limits::MAX_HEADER_VALUE_SIZE {
                return Err(ErrorResponse::with_details(
                    "validation_error",
                    &format!(
                        "Record at index {} header '{}' value exceeds maximum length of {} characters (got {})",
                        index,
                        header_key,
                        limits::MAX_HEADER_VALUE_SIZE,
                        header_value.len()
                    ),
                    serde_json::json!({
                        "field": format!("records[{}].headers['{}']", index, header_key), "max_size": limits::MAX_HEADER_VALUE_SIZE, "actual_size": header_value.len()
                    }),
                ));
            }
        }
    }
    Ok(())
}

pub fn validate_produce_request(request: &ProduceRequest) -> Result<(), ErrorResponse> {
    if request.records.is_empty() {
        return Err(ErrorResponse::with_details(
            "validation_error",
            "No records provided in request",
            serde_json::json!({ "field": "records" }),
        ));
    }
    if request.records.len() > limits::MAX_BATCH_SIZE {
        return Err(ErrorResponse::with_details(
            "validation_error",
            &format!(
                "Batch size exceeds maximum of {} records (got {})",
                limits::MAX_BATCH_SIZE,
                request.records.len()
            ),
            serde_json::json!({ "field": "records", "max_size": limits::MAX_BATCH_SIZE, "actual_size": request.records.len() }),
        ));
    }
    for (index, record) in request.records.iter().enumerate() {
        validate_record(record, index)?;
    }
    Ok(())
}

pub fn validate_topic_name(topic: &str) -> Result<(), ErrorResponse> {
    if topic.is_empty() || topic.len() > 255 {
        return Err(ErrorResponse::invalid_parameter(
            "topic",
            "Topic name must be between 1 and 255 characters",
        ));
    }
    let chars: Vec<char> = topic.chars().collect();
    if !chars[0].is_alphanumeric() && chars[0] != '.' && chars[0] != '_' {
        return Err(ErrorResponse::invalid_topic_name(topic));
    }
    for ch in chars.iter().skip(1) {
        if !ch.is_alphanumeric() && *ch != '.' && *ch != '_' && *ch != '-' {
            return Err(ErrorResponse::invalid_topic_name(topic));
        }
    }
    Ok(())
}

pub fn validate_consumer_group_id(group_id: &str) -> Result<(), ErrorResponse> {
    if group_id.is_empty() || group_id.len() > 255 {
        return Err(ErrorResponse::invalid_parameter(
            "group_id",
            "Consumer group ID must be between 1 and 255 characters",
        ));
    }
    let chars: Vec<char> = group_id.chars().collect();
    if !chars[0].is_alphanumeric() && chars[0] != '.' && chars[0] != '_' {
        return Err(ErrorResponse::invalid_consumer_group_id(group_id));
    }
    for ch in chars.iter().skip(1) {
        if !ch.is_alphanumeric() && *ch != '.' && *ch != '_' && *ch != '-' {
            return Err(ErrorResponse::invalid_consumer_group_id(group_id));
        }
    }
    Ok(())
}

pub fn validate_poll_query(query: &PollQuery) -> Result<(), ErrorResponse> {
    if let Some(max_records) = query.max_records {
        if !(1..=limits::MAX_POLL_RECORDS).contains(&max_records) {
            return Err(ErrorResponse::invalid_parameter(
                "max_records",
                &format!(
                    "max_records must be between 1 and {}",
                    limits::MAX_POLL_RECORDS
                ),
            ));
        }
    }
    if query.from_offset.is_some() && query.from_time.is_some() {
        return Err(ErrorResponse::invalid_parameter(
            "from_offset,from_time",
            "from_offset and from_time are mutually exclusive",
        ));
    }
    if let Some(ts) = &query.from_time {
        if let Err(e) = chrono::DateTime::parse_from_rfc3339(ts) {
            return Err(ErrorResponse::with_details(
                "validation_error",
                "from_time must be a valid RFC3339 timestamp",
                serde_json::json!({ "field": "from_time", "error": e.to_string() }),
            ));
        }
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HealthCheckResponse {
    pub status: String,
    pub service: String,
    pub timestamp: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TopicsResponse {
    pub topics: Vec<String>,
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
            serde_json::json!({ "field": field, "max_size": max_size, "actual_size": actual_size }),
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

impl From<FlashQError> for ErrorResponse {
    fn from(err: FlashQError) -> Self {
        match err {
            FlashQError::TopicNotFound { topic } => Self::topic_not_found(&topic),
            FlashQError::ConsumerGroupNotFound { group_id } => Self::group_not_found(&group_id),
            FlashQError::ConsumerGroupAlreadyExists { group_id } => Self::with_details(
                "conflict",
                &format!("Consumer group '{group_id}' already exists"),
                serde_json::json!({ "group_id": group_id }),
            ),
            FlashQError::ConsumerGroupCreationFailed { group_id, reason } => Self::with_details(
                "consumer_group_creation_failed",
                &format!("Failed to create consumer group '{group_id}': {reason}"),
                serde_json::json!({ "group_id": group_id, "reason": reason }),
            ),
            FlashQError::InvalidOffset {
                offset,
                topic,
                max_offset,
            } => Self::with_details(
                "invalid_offset",
                &format!("Invalid offset {offset} for topic '{topic}', max offset is {max_offset}"),
                serde_json::json!({ "offset": offset, "topic": topic, "max_offset": max_offset }),
            ),
            FlashQError::Storage(storage_err) => {
                Self::internal_error(&format!("Storage error: {storage_err}"))
            }
        }
    }
}
