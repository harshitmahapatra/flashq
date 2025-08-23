use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use chrono::Utc;
use message_queue_rs::{MessageQueue, MessageQueueError, MessageRecord, api::*};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

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

#[derive(serde::Deserialize)]
struct ConsumerGroupParams {
    group_id: String,
    topic: String,
}

#[derive(serde::Serialize)]
struct HealthCheckResponse {
    status: String,
    service: String,
    timestamp: u64,
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

fn error_to_status_code(error: &MessageQueueError) -> StatusCode {
    if error.is_not_found() {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    }
}

fn validate_message_request(request: &PostMessageRequest) -> Result<(), String> {
    // Validate key length (max 1024 characters)
    if let Some(key) = &request.key {
        if key.len() > 1024 {
            return Err(format!(
                "Message key exceeds maximum length of 1024 characters (got {})",
                key.len()
            ));
        }
    }

    // Validate value length (max 1MB = 1,048,576 characters)
    if request.value.len() > 1_048_576 {
        return Err(format!(
            "Message value exceeds maximum length of 1MB (got {} bytes)",
            request.value.len()
        ));
    }

    // Validate header values (each max 1024 characters)
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse port argument
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

    // Set log level based on debug build or environment
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
        .route("/api/topics/{topic}/messages", post(post_message))
        .route("/api/topics/{topic}/messages", get(poll_messages))
        .route("/health", get(health_check))
        // Consumer Group API routes
        .route("/api/consumer-groups", post(create_consumer_group))
        .route(
            "/api/consumer-groups/{group_id}/topics/{topic}/offset",
            get(get_consumer_group_offset),
        )
        .route(
            "/api/consumer-groups/{group_id}/topics/{topic}/offset",
            post(update_consumer_group_offset),
        )
        .route(
            "/api/consumer-groups/{group_id}/topics/{topic}/messages",
            get(poll_messages_for_consumer_group),
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

async fn post_message(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Json(request): Json<PostMessageRequest>,
) -> Result<Json<PostMessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate message size limits according to OpenAPI spec
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

async fn poll_messages(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Query(params): Query<PollQuery>,
) -> Result<Json<PollMessagesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let messages_result = match params.from_offset {
        Some(offset) => app_state
            .queue
            .poll_messages_from_offset(&topic, offset, params.count),
        None => app_state.queue.poll_messages(&topic, params.count),
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
                    "GET /api/topics/{}/messages - count: {:?}{} - {} messages returned",
                    topic,
                    params.count,
                    offset_info,
                    messages.len()
                ),
            );
            let count = messages.len();
            let message_responses: Vec<MessageResponse> = messages
                .into_iter()
                .map(|msg| MessageResponse {
                    key: msg.record.key,
                    value: msg.record.value,
                    headers: msg.record.headers,
                    offset: msg.offset,
                    timestamp: msg.timestamp,
                })
                .collect();

            Ok(Json(PollMessagesResponse {
                messages: message_responses,
                count,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("GET /api/topics/{topic}/messages failed: {error}"),
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

async fn create_consumer_group(
    State(app_state): State<AppState>,
    Json(request): Json<CreateConsumerGroupRequest>,
) -> Result<Json<CreateConsumerGroupResponse>, (StatusCode, Json<ErrorResponse>)> {
    match app_state
        .queue
        .create_consumer_group(request.group_id.clone())
    {
        Ok(_) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!("POST /api/consumer-groups - group_id: {}", request.group_id),
            );
            Ok(Json(CreateConsumerGroupResponse {
                group_id: request.group_id,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!("POST /api/consumer-groups failed: {error}"),
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
) -> Result<Json<GetConsumerGroupOffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
    match app_state
        .queue
        .get_consumer_group_offset(&params.group_id, &params.topic)
    {
        Ok(offset) => {
            log(
                app_state.config.log_level,
                LogLevel::Trace,
                &format!(
                    "GET /api/consumer-groups/{}/topics/{}/offset - offset: {}",
                    params.group_id, params.topic, offset
                ),
            );
            Ok(Json(GetConsumerGroupOffsetResponse {
                group_id: params.group_id.clone(),
                topic: params.topic.clone(),
                offset,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "GET /api/consumer-groups/{}/topics/{}/offset failed: {error}",
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

async fn update_consumer_group_offset(
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
                    "POST /api/consumer-groups/{}/topics/{}/offset - offset: {}",
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
                    "POST /api/consumer-groups/{}/topics/{}/offset failed: {error}",
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

async fn poll_messages_for_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Query(query): Query<PollQuery>,
) -> Result<Json<ConsumerGroupPollResponse>, (StatusCode, Json<ErrorResponse>)> {
    let messages_result = match query.from_offset {
        Some(offset) => {
            // Poll from specific offset without updating consumer group offset (replay mode)
            app_state
                .queue
                .poll_messages_for_consumer_group_from_offset(
                    &params.group_id,
                    &params.topic,
                    offset,
                    query.count,
                )
        }
        None => {
            // Normal polling from consumer group's current offset
            app_state.queue.poll_messages_for_consumer_group(
                &params.group_id,
                &params.topic,
                query.count,
            )
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
                    "GET /api/consumer-groups/{}/topics/{}/messages - count: {:?}{} - {} messages returned",
                    params.group_id,
                    params.topic,
                    query.count,
                    offset_info,
                    messages.len()
                ),
            );
            let count = messages.len();
            let message_responses: Vec<MessageResponse> = messages
                .into_iter()
                .map(|msg| MessageResponse {
                    key: msg.record.key,
                    value: msg.record.value,
                    headers: msg.record.headers,
                    offset: msg.offset,
                    timestamp: msg.timestamp,
                })
                .collect();

            // Get the current offset from the consumer group (whether it was updated or not)
            let new_offset = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic)
                .unwrap_or_default();

            Ok(Json(ConsumerGroupPollResponse {
                messages: message_responses,
                count,
                new_offset,
            }))
        }
        Err(error) => {
            log(
                app_state.config.log_level,
                LogLevel::Error,
                &format!(
                    "GET /api/consumer-groups/{}/topics/{}/messages failed: {error}",
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
