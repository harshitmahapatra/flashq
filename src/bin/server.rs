use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use chrono::Utc;
use message_queue_rs::{MessageQueue, Record, api::*};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

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
    queue: Arc<MessageQueue>,
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

#[derive(serde::Serialize)]
struct HealthCheckResponse {
    status: String,
    service: String,
    timestamp: u64,
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

fn validate_record_request(request: &PostRecordRequest) -> Result<(), ErrorResponse> {
    if let Some(key) = &request.key {
        if key.len() > 1024 {
            return Err(ErrorResponse::record_size_error(
                "Record key",
                1024,
                key.len(),
            ));
        }
    }

    if request.value.len() > 1_048_576 {
        return Err(ErrorResponse::record_size_error(
            "Record value",
            1_048_576,
            request.value.len(),
        ));
    }

    if let Some(headers) = &request.headers {
        for (header_key, header_value) in headers {
            if header_value.len() > 1024 {
                return Err(ErrorResponse::with_details(
                    "validation_error",
                    &format!(
                        "Header '{}' value exceeds maximum length of 1024 characters (got {})",
                        header_key,
                        header_value.len()
                    ),
                    serde_json::json!({
                        "field": format!("headers.{}", header_key),
                        "max_size": 1024,
                        "actual_size": header_value.len()
                    }),
                ));
            }
        }
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
        if !(1..=10000).contains(&max_records) {
            return Err(ErrorResponse::invalid_parameter(
                "max_records",
                "max_records must be between 1 and 10000",
            ));
        }
    }

    // Validate timeout_ms parameter
    if let Some(timeout_ms) = query.timeout_ms {
        if timeout_ms > 60000 {
            return Err(ErrorResponse::invalid_parameter(
                "timeout_ms",
                "timeout_ms must not exceed 60000 milliseconds",
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
        queue: Arc::new(MessageQueue::new()),
        config,
    });

    let app = Router::new()
        .route("/topics/{topic}/records", post(post_message))
        .route("/topics/{topic}/messages", get(poll_messages))
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
        &format!("Message Queue Server starting on http://{bind_address}"),
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
        service: "message-queue-rs".to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }))
}

// =============================================================================
// PRODUCER ENDPOINTS
// =============================================================================

async fn post_message(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Json(request): Json<PostRecordRequest>,
) -> Result<Json<PostRecordResponse>, (StatusCode, Json<ErrorResponse>)> {
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

    // Validate record request
    if let Err(error_response) = validate_record_request(&request) {
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

    let value_for_log = request.value.clone();
    match app_state.queue.post_record(
        topic.clone(),
        Record {
            key: request.key,
            value: request.value,
            headers: request.headers,
        },
    ) {
        Ok(offset) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "POST /topics/{topic}/records - Offset: {offset} - '{value_for_log}'"
                ),
            );
            let timestamp = chrono::Utc::now().to_rfc3339();

            Ok(Json(PostRecordResponse { offset, timestamp }))
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
// BASIC CONSUMER ENDPOINTS
// =============================================================================

async fn poll_messages(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Query(params): Query<PollQuery>,
) -> Result<Json<FetchResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate topic name
    if let Err(error_response) = validate_topic_name(&topic) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /topics/{}/records topic validation failed: {}",
                topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate query parameters
    if let Err(error_response) = validate_poll_query(&params) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!(
                "GET /topics/{}/records query validation failed: {}",
                topic, error_response.message
            ),
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    let limit = params.effective_limit();
    let timeout_ms = params.effective_timeout_ms();
    let include_headers = params.should_include_headers();

    let messages_result = match params.from_offset {
        Some(offset) => app_state
            .queue
            .poll_records_from_offset(&topic, offset, limit),
        None => app_state.queue.poll_records(&topic, limit),
    };

    match messages_result {
        Ok(messages) => {
            let offset_info = match params.from_offset {
                Some(offset) => format!(" from_offset: {offset}"),
                None => String::new(),
            };

            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "GET /topics/{}/records - max_records: {:?}, timeout_ms: {}{} - {} messages returned",
                    topic,
                    limit,
                    timeout_ms,
                    offset_info,
                    messages.len()
                ),
            );

            let message_responses: Vec<RecordResponse> = messages
                .into_iter()
                .map(|msg| RecordResponse {
                    key: msg.record.key,
                    value: msg.record.value,
                    headers: if include_headers {
                        msg.record.headers
                    } else {
                        None
                    },
                    offset: msg.offset,
                    timestamp: msg.timestamp,
                })
                .collect();

            let next_offset = if let Some(last_message) = message_responses.last() {
                last_message.offset + 1
            } else {
                params
                    .from_offset
                    .unwrap_or_else(|| app_state.queue.get_high_water_mark(&topic))
            };

            let high_water_mark = app_state.queue.get_high_water_mark(&topic);
            let response = FetchResponse::new(message_responses, next_offset, high_water_mark);

            Ok(Json(response))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("GET /topics/{topic}/records failed: {error}"),
            );

            // Check if it's a topic not found error
            let error_response = if error.is_not_found() {
                ErrorResponse::topic_not_found(&topic)
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

            let error_response = if error.is_not_found() {
                // Could be either group not found or topic not found
                if error.to_string().contains("group") {
                    ErrorResponse::group_not_found(&params.group_id)
                } else {
                    ErrorResponse::topic_not_found(&params.topic)
                }
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

            let error_response = if error.is_not_found() {
                if error.to_string().contains("group") {
                    ErrorResponse::group_not_found(&params.group_id)
                } else {
                    ErrorResponse::topic_not_found(&params.topic)
                }
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
    let timeout_ms = query.effective_timeout_ms();
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
                    "GET /consumer/{}/topics/{} - max_records: {:?}, timeout_ms: {}{} - {} messages returned",
                    params.group_id,
                    params.topic,
                    limit,
                    timeout_ms,
                    offset_info,
                    messages.len()
                ),
            );

            let message_responses: Vec<RecordResponse> = messages
                .into_iter()
                .map(|msg| RecordResponse {
                    key: msg.record.key,
                    value: msg.record.value,
                    headers: if include_headers {
                        msg.record.headers
                    } else {
                        None
                    },
                    offset: msg.offset,
                    timestamp: msg.timestamp,
                })
                .collect();

            let next_offset = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic)
                .unwrap_or_default();

            let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);
            let response = FetchResponse::new(message_responses, next_offset, high_water_mark);

            Ok(Json(response))
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

            let error_response = if error.is_not_found() {
                if error.to_string().contains("group") {
                    ErrorResponse::group_not_found(&params.group_id)
                } else {
                    ErrorResponse::topic_not_found(&params.topic)
                }
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
