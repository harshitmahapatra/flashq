use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use chrono::Utc;
use message_queue_rs::{MessageQueue, MessageQueueError, MessageRecord, api::*};
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

fn error_to_status_code(error: &MessageQueueError) -> StatusCode {
    if error.is_not_found() {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    }
}

fn validate_message_request(request: &PostMessageRequest) -> Result<(), String> {
    if let Some(key) = &request.key {
        if key.len() > 1024 {
            return Err(format!(
                "Message key exceeds maximum length of 1024 characters (got {})",
                key.len()
            ));
        }
    }

    if request.value.len() > 1_048_576 {
        return Err(format!(
            "Message value exceeds maximum length of 1MB (got {} bytes)",
            request.value.len()
        ));
    }

    if let Some(headers) = &request.headers {
        for (header_key, header_value) in headers {
            if header_value.len() > 1024 {
                return Err(format!(
                    "Header '{}' value exceeds maximum length of 1024 characters (got {})",
                    header_key,
                    header_value.len()
                ));
            }
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
    Json(request): Json<PostMessageRequest>,
) -> Result<Json<PostMessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(validation_error) = validate_message_request(&request) {
        log(
            app_state.config.log_level,
            LogLevel::Error,
            &format!("POST /api/topics/{topic}/messages validation failed: {validation_error}"),
        );
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: validation_error,
            }),
        ));
    }

    let value_for_log = request.value.clone();
    match app_state.queue.post_message(
        topic.clone(),
        MessageRecord {
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
                    "POST /api/topics/{topic}/messages - Offset: {offset} - '{value_for_log}'"
                ),
            );
            let timestamp = chrono::Utc::now().to_rfc3339();

            Ok(Json(PostMessageResponse { offset, timestamp }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("POST /api/topics/{topic}/messages failed: {error}"),
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
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
    let limit = params.effective_limit();
    let timeout_ms = params.effective_timeout_ms();
    let include_headers = params.should_include_headers();

    let messages_result = match params.from_offset {
        Some(offset) => app_state
            .queue
            .poll_messages_from_offset(&topic, offset, limit),
        None => app_state.queue.poll_messages(&topic, limit),
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
                    "GET /topics/{}/messages - max_records: {:?}, timeout_ms: {}{} - {} messages returned",
                    topic,
                    limit,
                    timeout_ms,
                    offset_info,
                    messages.len()
                ),
            );

            let message_responses: Vec<MessageResponse> = messages
                .into_iter()
                .map(|msg| MessageResponse {
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
                &format!("GET /topics/{topic}/messages failed: {error}"),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
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
                &format!("POST /consumer/{} failed: {error}", params.group_id),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}

async fn leave_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupIdParams>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
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
                &format!("DELETE /consumer/{} failed: {error}", params.group_id),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}

async fn get_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
) -> Result<Json<OffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
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

            // TODO(human): Add timestamp tracking for last_commit_time
            let response = OffsetResponse::new(
                params.topic.clone(),
                committed_offset,
                high_water_mark,
                None,
            );

            Ok(Json(response))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "GET /consumer/{}/topics/{}/offset failed: {error}",
                    params.group_id, params.topic
                ),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}

async fn commit_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Json(request): Json<UpdateConsumerGroupOffsetRequest>,
) -> Result<Json<GetConsumerGroupOffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
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
                    "POST /consumer/{}/topics/{}/offset failed: {error}",
                    params.group_id, params.topic
                ),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}

async fn fetch_messages_for_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Query(query): Query<PollQuery>,
) -> Result<Json<FetchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = query.effective_limit();
    let timeout_ms = query.effective_timeout_ms();
    let include_headers = query.should_include_headers();

    let messages_result = match query.from_offset {
        Some(offset) => app_state
            .queue
            .poll_messages_for_consumer_group_from_offset(
                &params.group_id,
                &params.topic,
                offset,
                limit,
            ),
        None => {
            app_state
                .queue
                .poll_messages_for_consumer_group(&params.group_id, &params.topic, limit)
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

            let message_responses: Vec<MessageResponse> = messages
                .into_iter()
                .map(|msg| MessageResponse {
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
                    "GET /consumer/{}/topics/{} failed: {error}",
                    params.group_id, params.topic
                ),
            );
            Err((
                error_to_status_code(&error),
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            ))
        }
    }
}
