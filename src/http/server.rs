//! HTTP server implementation for FlashQ

use super::common::*;
use crate::{FlashQ, FlashQError, Record};
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use chrono::Utc;
use std::sync::Arc;
use tokio::net::TcpListener;

// =============================================================================
// CONFIGURATION & LOGGING
// =============================================================================

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum LogLevel {
    Error = 0,
    Info = 2,
    Trace = 4,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "ERROR",
            LogLevel::Info => "INFO",
            LogLevel::Trace => "TRACE",
        }
    }
}

#[derive(Clone)]
pub struct AppConfig {
    pub log_level: LogLevel,
}

pub type AppState = Arc<AppStateInner>;

#[derive(Clone)]
pub struct AppStateInner {
    pub queue: Arc<FlashQ>,
    pub config: AppConfig,
}

pub fn log(app_log_level: LogLevel, message_log_level: LogLevel, message: &str) {
    if message_log_level <= app_log_level {
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        println!("{timestamp} [{}] {message}", message_log_level.as_str());
    }
}

// =============================================================================
// REQUEST/RESPONSE TYPES
// =============================================================================

#[derive(serde::Deserialize)]
pub struct ConsumerGroupParams {
    pub group_id: String,
    pub topic: String,
}

#[derive(serde::Deserialize)]
pub struct ConsumerGroupIdParams {
    pub group_id: String,
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

pub fn error_to_status_code(error_code: &str) -> StatusCode {
    match error_code {
        "invalid_parameter" | "validation_error" => StatusCode::BAD_REQUEST,
        "topic_not_found" | "group_not_found" => StatusCode::NOT_FOUND,
        "record_validation_error" => StatusCode::UNPROCESSABLE_ENTITY,
        "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// =============================================================================
// ROUTE HANDLERS
// =============================================================================

pub async fn health_check(
    State(app_state): State<AppState>,
) -> Result<Json<HealthCheckResponse>, (StatusCode, Json<ErrorResponse>)> {
    log(app_state.config.log_level, LogLevel::Trace, "GET /health");
    Ok(Json(HealthCheckResponse {
        status: "healthy".to_string(),
        service: "flashq".to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }))
}

pub async fn produce_records(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Json(request): Json<ProduceRequest>,
) -> Result<Json<ProduceResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate topic name
    if let Err(error_response) = validate_topic_name(&topic) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "POST /topics/{}/records topic validation failed: {}",
                topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate produce request
    if let Err(error_response) = validate_produce_request(&request) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "POST /topics/{}/records validation failed: {}",
                topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    let record_count = request.records.len();

    // Convert request records to Record type
    let records: Vec<Record> = request
        .records
        .into_iter()
        .map(|msg_record| Record {
            key: msg_record.key,
            value: msg_record.value,
            headers: msg_record.headers,
        })
        .collect();

    match app_state.queue.post_records(topic.clone(), records) {
        Ok(offsets) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "POST /topics/{topic}/records - Posted {record_count} records, offsets: {offsets:?}"
                ),
            );

            let timestamp = chrono::Utc::now().to_rfc3339();
            let offset_infos: Vec<OffsetInfo> = offsets
                .into_iter()
                .map(|offset| OffsetInfo {
                    offset,
                    timestamp: timestamp.clone(),
                })
                .collect();

            Ok(Json(ProduceResponse {
                offsets: offset_infos,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("POST /topics/{topic}/records failed: {error}"),
            );
            let error_response = ErrorResponse::internal_error(&error.to_string());
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

pub async fn create_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupIdParams>,
) -> Result<Json<ConsumerGroupResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate consumer group ID
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "POST /consumer/{} validation failed: {}",
                params.group_id, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    match app_state
        .queue
        .create_consumer_group(params.group_id.clone())
    {
        Ok(_) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!("POST /consumer/{} - group created", params.group_id),
            );

            let response = ConsumerGroupResponse {
                group_id: params.group_id,
            };

            Ok(Json(response))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("POST /consumer/{} failed: {}", params.group_id, error),
            );

            let error_response = ErrorResponse::internal_error(&error.to_string());
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

pub async fn leave_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupIdParams>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    // Validate consumer group ID
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "DELETE /consumer/{} validation failed: {}",
                params.group_id, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    match app_state.queue.delete_consumer_group(&params.group_id) {
        Ok(_) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!("DELETE /consumer/{} - consumer group left", params.group_id),
            );
            Ok(StatusCode::NO_CONTENT)
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("DELETE /consumer/{} failed: {}", params.group_id, error),
            );

            let error_response = if error.is_not_found() {
                ErrorResponse::group_not_found(&params.group_id)
            } else {
                ErrorResponse::internal_error(&error.to_string())
            };

            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

pub async fn get_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
) -> Result<Json<OffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate consumer group ID
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{}/offset group validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate topic name
    if let Err(error_response) = validate_topic_name(&params.topic) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{}/offset topic validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    match app_state
        .queue
        .get_consumer_group_offset(&params.group_id, &params.topic)
    {
        Ok(committed_offset) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "GET /consumer/{}/topics/{}/offset - offset: {}",
                    params.group_id, params.topic, committed_offset
                ),
            );

            let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);

            // Use current timestamp for last_commit_time since we just retrieved the offset
            let last_commit_time = Some(chrono::Utc::now().to_rfc3339());
            let response = OffsetResponse::new(
                params.topic.clone(),
                committed_offset,
                high_water_mark,
                last_commit_time,
            );

            Ok(Json(response))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "GET /consumer/{}/topics/{}/offset failed: {}",
                    params.group_id, params.topic, error
                ),
            );

            let error_response = match &error {
                FlashQError::TopicNotFound { topic } => ErrorResponse::topic_not_found(topic),
                FlashQError::ConsumerGroupNotFound { group_id } => {
                    ErrorResponse::group_not_found(group_id)
                }
                _ => ErrorResponse::internal_error(&error.to_string()),
            };

            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

pub async fn commit_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Json(request): Json<UpdateConsumerGroupOffsetRequest>,
) -> Result<Json<GetConsumerGroupOffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate consumer group ID
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "POST /consumer/{}/topics/{}/offset group validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate topic name
    if let Err(error_response) = validate_topic_name(&params.topic) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "POST /consumer/{}/topics/{}/offset topic validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    match app_state.queue.update_consumer_group_offset(
        &params.group_id,
        params.topic.clone(),
        request.offset,
    ) {
        Ok(_) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "POST /consumer/{}/topics/{}/offset - offset: {}",
                    params.group_id, params.topic, request.offset
                ),
            );
            Ok(Json(GetConsumerGroupOffsetResponse {
                group_id: params.group_id.clone(),
                topic: params.topic.clone(),
                offset: request.offset,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "POST /consumer/{}/topics/{}/offset failed: {}",
                    params.group_id, params.topic, error
                ),
            );

            let error_response = match &error {
                FlashQError::TopicNotFound { topic } => ErrorResponse::topic_not_found(topic),
                FlashQError::ConsumerGroupNotFound { group_id } => {
                    ErrorResponse::group_not_found(group_id)
                }
                _ => ErrorResponse::internal_error(&error.to_string()),
            };

            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

pub async fn fetch_records_for_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Query(query): Query<PollQuery>,
) -> Result<Json<FetchResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate consumer group ID
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{} group validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate topic name
    if let Err(error_response) = validate_topic_name(&params.topic) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{} topic validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate query parameters
    if let Err(error_response) = validate_poll_query(&query) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{} query validation failed: {}",
                params.group_id, params.topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    let limit = query.effective_limit();
    let include_headers = query.should_include_headers();

    let records_result = match query.from_offset {
        Some(offset) => app_state.queue.poll_records_for_consumer_group_from_offset(
            &params.group_id,
            &params.topic,
            offset,
            limit,
        ),
        None => {
            app_state
                .queue
                .poll_records_for_consumer_group(&params.group_id, &params.topic, limit)
        }
    };

    let create_error_response = |error| {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /consumer/{}/topics/{}/offset: {}",
                params.group_id, params.topic, error
            ),
        );
        match &error {
            FlashQError::TopicNotFound { topic } => ErrorResponse::topic_not_found(topic),
            FlashQError::ConsumerGroupNotFound { group_id } => {
                ErrorResponse::group_not_found(group_id)
            }
            _ => ErrorResponse::internal_error(&error.to_string()),
        }
    };

    match records_result {
        Ok(records) => {
            let offset_info = match query.from_offset {
                Some(offset) => format!(" from_offset: {offset}"),
                None => String::new(),
            };

            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "GET /consumer/{}/topics/{} - max_records: {:?}{} - {} records returned",
                    params.group_id,
                    params.topic,
                    limit,
                    offset_info,
                    records.len()
                ),
            );

            let record_responses: Vec<crate::RecordWithOffset> = if include_headers {
                records
            } else {
                records
                    .into_iter()
                    .map(|msg| crate::RecordWithOffset {
                        record: Record {
                            key: msg.record.key,
                            value: msg.record.value,
                            headers: None,
                        },
                        offset: msg.offset,
                        timestamp: msg.timestamp,
                    })
                    .collect()
            };

            let offset_response = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic);

            match offset_response {
                Ok(next_offset) => {
                    let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);
                    let response =
                        FetchResponse::new(record_responses, next_offset, high_water_mark);
                    Ok(Json(response))
                }
                Err(error) => {
                    log(
                        app_state.config.log_level,
                        LogLevel::Error,
                        &format!(
                            "GET /consumer/{}/topics/{}/offset: {}",
                            params.group_id, params.topic, error
                        ),
                    );
                    let error_response = create_error_response(error);
                    Err((
                        error_to_status_code(&error_response.error),
                        Json(error_response),
                    ))
                }
            }
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "GET /consumer/{}/topics/{} failed: {}",
                    params.group_id, params.topic, error
                ),
            );

            let error_response = create_error_response(error);

            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

// =============================================================================
// APPLICATION FACTORY & SERVER UTILITIES
// =============================================================================

/// Creates the Axum router with all routes configured
pub fn create_router(app_state: AppState) -> Router {
    Router::new()
        .route("/topics/{topic}/records", post(produce_records))
        .route("/health", get(health_check))
        .route("/consumer/{group_id}", post(create_consumer_group))
        .route("/consumer/{group_id}", delete(leave_consumer_group))
        .route(
            "/consumer/{group_id}/topics/{topic}",
            get(fetch_records_for_consumer_group),
        )
        .route(
            "/consumer/{group_id}/topics/{topic}/offset",
            get(get_consumer_group_offset),
        )
        .route(
            "/consumer/{group_id}/topics/{topic}/offset",
            post(commit_consumer_group_offset),
        )
        .with_state(app_state)
}

/// Creates application state with configuration
pub fn create_app_state(log_level: LogLevel) -> AppState {
    let config = AppConfig { log_level };
    Arc::new(AppStateInner {
        queue: Arc::new(FlashQ::new()),
        config,
    })
}

/// Starts the HTTP server on the specified port
pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let log_level = if cfg!(debug_assertions) {
        LogLevel::Trace
    } else {
        LogLevel::Info
    };

    let app_state = create_app_state(log_level);
    let app = create_router(app_state.clone());

    let bind_address = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&bind_address)
        .await
        .map_err(|e| format!("Failed to bind to address {bind_address}: {e}"))?;

    log(
        app_state.config.log_level,
        LogLevel::Info,
        &format!("FlashQ Server starting on http://{bind_address}"),
    );

    axum::serve(listener, app)
        .await
        .map_err(|e| format!("Server failed to start: {e}"))?;

    Ok(())
}

// =============================================================================
// UNIT TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Trace.as_str(), "TRACE");
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Error < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Trace);
        assert!(LogLevel::Error < LogLevel::Trace);
    }

    #[test]
    fn test_error_to_status_code() {
        assert_eq!(
            error_to_status_code("invalid_parameter"),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_to_status_code("validation_error"),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_to_status_code("topic_not_found"),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            error_to_status_code("group_not_found"),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            error_to_status_code("record_validation_error"),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            error_to_status_code("internal_error"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            error_to_status_code("unknown_error"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
