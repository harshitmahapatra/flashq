use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use chrono::Utc;
use flashq::{FlashQ, FlashQError, Record, RecordWithOffset, api::*};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

// =============================================================================
// VALIDATION CONSTANTS
// =============================================================================

/// Validation limits following OpenAPI specification
///
/// Note: Header count per record is not limited - only individual header value sizes are validated.
mod limits {
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

// =============================================================================
// CONFIGURATION & LOGGING
// =============================================================================

#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum LogLevel {
    Error = 0,
    Info = 2,
    Trace = 4,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "ERROR",
            LogLevel::Info => "INFO",
            LogLevel::Trace => "TRACE",
        }
    }
}

#[derive(Clone)]
struct AppConfig {
    log_level: LogLevel,
}

type AppState = Arc<AppStateInner>;

#[derive(Clone)]
struct AppStateInner {
    queue: Arc<FlashQ>,
    config: AppConfig,
}

fn log(app_log_level: LogLevel, message_log_level: LogLevel, message: &str) {
    if message_log_level <= app_log_level {
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        println!("{timestamp} [{}] {message}", message_log_level.as_str());
    }
}

// =============================================================================
// REQUEST/RESPONSE TYPES
// =============================================================================

#[derive(serde::Deserialize)]
struct ConsumerGroupParams {
    group_id: String,
    topic: String,
}

#[derive(serde::Deserialize)]
struct ConsumerGroupIdParams {
    group_id: String,
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

fn error_to_status_code(error_code: &str) -> StatusCode {
    match error_code {
        "invalid_parameter" | "validation_error" => StatusCode::BAD_REQUEST,

        "topic_not_found" | "group_not_found" => StatusCode::NOT_FOUND,

        "record_validation_error" => StatusCode::UNPROCESSABLE_ENTITY,

        "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,

        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn validate_message_record(record: &Record, index: usize) -> Result<(), ErrorResponse> {
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

fn validate_produce_request(request: &ProduceRequest) -> Result<(), ErrorResponse> {
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
        validate_message_record(record, index)?;
    }

    Ok(())
}

fn validate_topic_name(topic: &str) -> Result<(), ErrorResponse> {
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

fn validate_consumer_group_id(group_id: &str) -> Result<(), ErrorResponse> {
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

fn validate_poll_query(query: &PollQuery) -> Result<(), ErrorResponse> {
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

// =============================================================================
// MAIN APPLICATION
// =============================================================================

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let port = if args.len() > 1 {
        match args[1].parse::<u16>() {
            Ok(p) => p,
            Err(_) => {
                eprintln!("Usage: server [port]");
                eprintln!("Invalid port number: {}", args[1]);
                std::process::exit(1);
            }
        }
    } else {
        8080
    };

    let log_level = if cfg!(debug_assertions) {
        LogLevel::Trace
    } else {
        LogLevel::Info
    };

    let config = AppConfig { log_level };
    let app_state = Arc::new(AppStateInner {
        queue: Arc::new(FlashQ::new()),
        config,
    });

    let app = Router::new()
        .route("/topics/{topic}/records", post(produce_messages))
        .route("/health", get(health_check))
        .route("/consumer/{group_id}", post(create_consumer_group))
        .route("/consumer/{group_id}", delete(leave_consumer_group))
        .route(
            "/consumer/{group_id}/topics/{topic}",
            get(fetch_messages_for_consumer_group),
        )
        .route(
            "/consumer/{group_id}/topics/{topic}/offset",
            get(get_consumer_group_offset),
        )
        .route(
            "/consumer/{group_id}/topics/{topic}/offset",
            post(commit_consumer_group_offset),
        )
        .with_state(app_state.clone());

    let bind_address = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&bind_address)
        .await
        .expect("Failed to bind to address");

    log(
        app_state.config.log_level,
        LogLevel::Info,
        &format!("FlashQ Server starting on http://{bind_address}"),
    );

    axum::serve(listener, app)
        .await
        .expect("Server failed to start");
}

// =============================================================================
// HEALTH CHECK ENDPOINT
// =============================================================================

async fn health_check(
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

// =============================================================================
// PRODUCER ENDPOINTS
// =============================================================================

async fn produce_messages(
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

// =============================================================================
// CONSUMER GROUP ENDPOINTS
// =============================================================================

async fn create_consumer_group(
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

async fn leave_consumer_group(
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

async fn get_consumer_group_offset(
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

async fn commit_consumer_group_offset(
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

async fn fetch_messages_for_consumer_group(
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

    let messages_result = match query.from_offset {
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

    match messages_result {
        Ok(messages) => {
            let offset_info = match query.from_offset {
                Some(offset) => format!(" from_offset: {offset}"),
                None => String::new(),
            };

            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "GET /consumer/{}/topics/{} - max_records: {:?}{} - {} messages returned",
                    params.group_id,
                    params.topic,
                    limit,
                    offset_info,
                    messages.len()
                ),
            );

            let message_responses: Vec<RecordWithOffset> = if include_headers {
                messages
            } else {
                messages
                    .into_iter()
                    .map(|msg| RecordWithOffset {
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
                        FetchResponse::new(message_responses, next_offset, high_water_mark);
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
