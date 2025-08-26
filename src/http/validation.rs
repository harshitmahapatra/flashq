//! HTTP request validation logic

use crate::Record;
use super::types::{ErrorResponse, ProduceRequest, PollQuery};

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