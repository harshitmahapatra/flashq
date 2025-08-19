use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use chrono::Utc;
use message_queue_rs::{MessageQueue, api::*};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

type AppState = Arc<MessageQueue>;

fn log(level: &str, message: &str) {
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
    println!("{} [{}] {}", timestamp, level, message);
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

    let queue = Arc::new(MessageQueue::new());

    let app = Router::new()
        .route("/api/topics/{topic}/messages", post(post_message))
        .route("/api/topics/{topic}/messages", get(poll_messages))
        .route("/health", get(health_check))
        .with_state(queue);

    let bind_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_address)
        .await
        .expect("Failed to bind to address");

    log("INFO", &format!("Message Queue Server starting on http://{}", bind_address));

    axum::serve(listener, app)
        .await
        .expect("Server failed to start");
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "message-queue-rs"
    }))
}

async fn post_message(
    State(queue): State<AppState>,
    Path(topic): Path<String>,
    Json(request): Json<PostMessageRequest>,
) -> Result<Json<PostMessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    match queue.post_message(topic.clone(), request.content.clone()) {
        Ok(id) => {
            log("INFO", &format!("POST /api/topics/{}/messages - ID: {} - '{}'", topic, id, request.content));
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            Ok(Json(PostMessageResponse { id, timestamp }))
        }
        Err(error) => {
            log("ERROR", &format!("POST /api/topics/{}/messages failed: {}", topic, error));
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            ))
        }
    }
}

async fn poll_messages(
    State(queue): State<AppState>,
    Path(topic): Path<String>,
    Query(params): Query<PollQuery>,
) -> Result<Json<PollMessagesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _count = params.count;
    let _topic_name = topic;
    match queue.poll_messages(&_topic_name, _count) {
        Ok(messages) => {
            log("INFO", &format!("GET /api/topics/{}/messages - {} messages", _topic_name, messages.len()));
            let message_responses: Vec<MessageResponse> = messages
                .into_iter()
                .map(|message| MessageResponse {
                    id: message.id,
                    content: message.content,
                    timestamp: message.timestamp,
                })
                .collect();
            Ok(Json(PollMessagesResponse {
                count: message_responses.len(),
                messages: message_responses,
            }))
        }
        Err(error) => {
            log("ERROR", &format!("GET /api/topics/{}/messages failed: {}", _topic_name, error));
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error }),
            ))
        }
    }
}
