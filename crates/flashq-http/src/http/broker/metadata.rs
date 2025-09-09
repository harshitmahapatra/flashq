//! Metadata route handlers for FlashQ HTTP API

use super::server::AppState;
use crate::http::{ErrorResponse, HealthCheckResponse, TopicsResponse};
use axum::{extract::State, http::StatusCode, response::Json};
use log::trace;

pub async fn health_check(
    State(_app_state): State<AppState>,
) -> Result<Json<HealthCheckResponse>, (StatusCode, Json<ErrorResponse>)> {
    trace!("GET /health");
    Ok(Json(HealthCheckResponse {
        status: "healthy".to_string(),
        service: "flashq".to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }))
}

pub async fn get_topics(
    State(app_state): State<AppState>,
) -> Result<Json<TopicsResponse>, (StatusCode, Json<ErrorResponse>)> {
    trace!("GET /topics");
    let topics = app_state.queue.get_topics();
    Ok(Json(TopicsResponse { topics }))
}
