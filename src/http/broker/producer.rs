//! Producer route handlers for FlashQ HTTP API

use crate::http::{
    ErrorResponse, ProduceRequest, ProduceResponse, validate_produce_request, validate_topic_name,
};
use crate::{Record, error, trace};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};

use super::server::{AppState, error_to_status_code};

/// Handles POST /topic/{topic}/record - produce records to a topic
pub async fn produce_records(
    State(app_state): State<AppState>,
    Path(topic): Path<String>,
    Json(request): Json<ProduceRequest>,
) -> Result<Json<ProduceResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate topic name
    if let Err(error_response) = validate_topic_name(&topic) {
        error!(
            "POST /topics/{}/records topic validation failed: {}",
            topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }

    // Validate produce request
    if let Err(error_response) = validate_produce_request(&request) {
        error!(
            "POST /topics/{}/records validation failed: {}",
            topic, error_response.message
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
        Ok(offset) => {
            trace!(
                "POST /topics/{topic}/records - Posted {record_count} records, offset: {offset}"
            );

            let timestamp = chrono::Utc::now().to_rfc3339();

            Ok(Json(ProduceResponse { offset, timestamp }))
        }
        Err(error) => {
            error!("POST /topics/{topic}/records failed: {error}");
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}
