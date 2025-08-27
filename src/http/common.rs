//! HTTP API request and response types

use crate::{FlashQError, Record, RecordWithOffset};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// PRODUCER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct ProduceRequest {
    pub records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
pub struct OffsetInfo {
    pub offset: u64,
    pub timestamp: String,
}

#[derive(Serialize, Deserialize)]
pub struct ProduceResponse {
    pub offsets: Vec<OffsetInfo>,
}

// =============================================================================
// CONSUMER API TYPES
// =============================================================================

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    pub from_offset: Option<u64>,
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

// =============================================================================
// VALIDATION UTILITIES
// ============================================================================="

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
                        eprintln!("Warning: Invalid header format '{h}', expected KEY=VALUE");
                        None
                    }
                }
            })
            .collect()
    })
}

/// Validation limits following OpenAPI specification
///
/// Note: Header count per record is not limited - only individual header value sizes are validated.
pub mod limits {
    /// Maximum length for record keys in bytes (OpenAPI spec limit)
    pub const MAX_KEY_SIZE: usize = 1024;

    /// Maximum length for record values in bytes (OpenAPI spec limit: 1MB)
    pub const MAX_VALUE_SIZE: usize = 1_048_576;

    /// Maximum length for header values in bytes (OpenAPI spec limit)
    pub const MAX_HEADER_VALUE_SIZE: usize = 1024;

    /// Maximum number of records in a batch produce request (OpenAPI spec limit)
    pub const MAX_BATCH_SIZE: usize = 1000;

    /// Maximum number of records that can be requested in a single poll operation
    pub const MAX_POLL_RECORDS: usize = 10000;
}

/// Validates a single record for size limits and structure
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
                    "field": format!("records[{}].key", index),
                    "max_size": limits::MAX_KEY_SIZE,
                    "actual_size": key.len()
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
                "field": format!("records[{}].value", index),
                "max_size": limits::MAX_VALUE_SIZE,
                "actual_size": record.value.len()
            }),
        ));
    }

    // Validate headers if present
    // Note: We validate individual header value size but do not limit the total number of headers per record
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
                        "field": format!("records[{}].headers.{}", index, header_key),
                        "max_size": limits::MAX_HEADER_VALUE_SIZE,
                        "actual_size": header_value.len()
                    }),
                ));
            }
        }
    }

    Ok(())
}

/// Validates a batch produce request
pub fn validate_produce_request(request: &ProduceRequest) -> Result<(), ErrorResponse> {
    // Check batch size limits (1-MAX_BATCH_SIZE records as per OpenAPI spec)
    if request.records.is_empty() {
        return Err(ErrorResponse::invalid_parameter(
            "records",
            "At least one record must be provided",
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
            serde_json::json!({
                "field": "records",
                "max_size": limits::MAX_BATCH_SIZE,
                "actual_size": request.records.len()
            }),
        ));
    }

    // Validate each record in the batch
    for (index, record) in request.records.iter().enumerate() {
        validate_record(record, index)?;
    }

    Ok(())
}

/// Validates a topic name according to OpenAPI specification
pub fn validate_topic_name(topic: &str) -> Result<(), ErrorResponse> {
    // OpenAPI pattern: ^[a-zA-Z0-9._][a-zA-Z0-9._-]*$
    if topic.is_empty() || topic.len() > 255 {
        return Err(ErrorResponse::invalid_parameter(
            "topic",
            "Topic name must be between 1 and 255 characters",
        ));
    }

    let chars: Vec<char> = topic.chars().collect();

    // First character must be alphanumeric, dot, or underscore
    if !chars[0].is_alphanumeric() && chars[0] != '.' && chars[0] != '_' {
        return Err(ErrorResponse::invalid_topic_name(topic));
    }

    // Remaining characters can be alphanumeric, dot, underscore, or hyphen
    for ch in chars.iter().skip(1) {
        if !ch.is_alphanumeric() && *ch != '.' && *ch != '_' && *ch != '-' {
            return Err(ErrorResponse::invalid_topic_name(topic));
        }
    }

    Ok(())
}

/// Validates a consumer group ID according to OpenAPI specification
pub fn validate_consumer_group_id(group_id: &str) -> Result<(), ErrorResponse> {
    // Same pattern as topic names
    if group_id.is_empty() || group_id.len() > 255 {
        return Err(ErrorResponse::invalid_parameter(
            "group_id",
            "Consumer group ID must be between 1 and 255 characters",
        ));
    }

    let chars: Vec<char> = group_id.chars().collect();

    // First character must be alphanumeric, dot, or underscore
    if !chars[0].is_alphanumeric() && chars[0] != '.' && chars[0] != '_' {
        return Err(ErrorResponse::invalid_consumer_group_id(group_id));
    }

    // Remaining characters can be alphanumeric, dot, underscore, or hyphen
    for ch in chars.iter().skip(1) {
        if !ch.is_alphanumeric() && *ch != '.' && *ch != '_' && *ch != '-' {
            return Err(ErrorResponse::invalid_consumer_group_id(group_id));
        }
    }

    Ok(())
}

/// Validates poll query parameters
pub fn validate_poll_query(query: &PollQuery) -> Result<(), ErrorResponse> {
    // Validate max_records parameter
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

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HealthCheckResponse {
    pub status: String,
    pub service: String,
    pub timestamp: u64,
}

// =============================================================================
// ERROR RESPONSE IMPLEMENTATION
// =============================================================================

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
            FlashQError::InvalidOffset {
                offset,
                topic,
                max_offset,
            } => Self::with_details(
                "invalid_offset",
                &format!(
                    "Invalid offset {offset} for topic '{topic}', max offset is {max_offset}"
                ),
                serde_json::json!({
                    "offset": offset,
                    "topic": topic,
                    "max_offset": max_offset
                }),
            ),
            FlashQError::Storage(storage_err) => {
                Self::internal_error(&format!("Storage error: {storage_err}"))
            }
        }
    }
}

// =============================================================================
// UNIT TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_headers_valid() {
        let header_strings = Some(vec![
            "Content-Type=application/json".to_string(),
            "Authorization=Bearer token123".to_string(),
        ]);

        let result = parse_headers(header_strings);
        assert!(result.is_some());

        let headers = result.unwrap();
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
    }

    #[test]
    fn test_parse_headers_invalid_format() {
        let header_strings = Some(vec![
            "Content-Type=application/json".to_string(),
            "InvalidHeader".to_string(),
        ]);

        let result = parse_headers(header_strings);
        assert!(result.is_some());

        let headers = result.unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
    }

    #[test]
    fn test_parse_headers_none() {
        let result = parse_headers(None);
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_topic_name_valid() {
        assert!(validate_topic_name("valid_topic").is_ok());
        assert!(validate_topic_name("topic.with.dots").is_ok());
        assert!(validate_topic_name("topic-with-dashes").is_ok());
        assert!(validate_topic_name("_underscore_start").is_ok());
        assert!(validate_topic_name(".dot_start").is_ok());
        assert!(validate_topic_name("topic123").is_ok());
    }

    #[test]
    fn test_validate_topic_name_invalid() {
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name("-invalid_start").is_err());
        assert!(validate_topic_name("topic with spaces").is_err());
        assert!(validate_topic_name("topic@invalid").is_err());
        assert!(validate_topic_name(&"a".repeat(256)).is_err());
    }

    #[test]
    fn test_validate_consumer_group_id_valid() {
        assert!(validate_consumer_group_id("valid_group").is_ok());
        assert!(validate_consumer_group_id("group.with.dots").is_ok());
        assert!(validate_consumer_group_id("group-with-dashes").is_ok());
    }

    #[test]
    fn test_validate_consumer_group_id_invalid() {
        assert!(validate_consumer_group_id("").is_err());
        assert!(validate_consumer_group_id("-invalid_start").is_err());
        assert!(validate_consumer_group_id("group with spaces").is_err());
    }

    #[test]
    fn test_validate_record_valid() {
        let record = Record::new(
            Some("key".to_string()),
            "value".to_string(),
            Some(HashMap::from([("header".to_string(), "value".to_string())])),
        );
        assert!(validate_record(&record, 0).is_ok());
    }

    #[test]
    fn test_validate_record_key_too_long() {
        let long_key = "a".repeat(limits::MAX_KEY_SIZE + 1);
        let record = Record::new(Some(long_key), "value".to_string(), None);
        assert!(validate_record(&record, 0).is_err());
    }

    #[test]
    fn test_validate_record_value_too_long() {
        let long_value = "a".repeat(limits::MAX_VALUE_SIZE + 1);
        let record = Record::new(None, long_value, None);
        assert!(validate_record(&record, 0).is_err());
    }

    #[test]
    fn test_validate_produce_request_valid() {
        let request = ProduceRequest {
            records: vec![Record::new(None, "test".to_string(), None)],
        };
        assert!(validate_produce_request(&request).is_ok());
    }

    #[test]
    fn test_validate_produce_request_empty() {
        let request = ProduceRequest { records: vec![] };
        assert!(validate_produce_request(&request).is_err());
    }

    #[test]
    fn test_validate_poll_query_valid() {
        let query = PollQuery {
            max_records: Some(100),
            from_offset: None,
            include_headers: None,
        };
        assert!(validate_poll_query(&query).is_ok());
    }

    #[test]
    fn test_validate_poll_query_invalid_max_records() {
        let query = PollQuery {
            max_records: Some(limits::MAX_POLL_RECORDS + 1),
            from_offset: None,
            include_headers: None,
        };
        assert!(validate_poll_query(&query).is_err());
    }
}
