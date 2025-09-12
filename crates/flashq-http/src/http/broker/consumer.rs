//! Consumer route handlers for FlashQ HTTP API

use super::server::{AppState, error_to_status_code};
use crate::http::{
    ConsumerGroupIdParams, ConsumerGroupParams, ConsumerGroupResponse, ErrorResponse,
    FetchResponse, GetConsumerGroupOffsetResponse, OffsetResponse, PollOffsetQuery, PollQuery,
    PollTimeQuery, UpdateConsumerGroupOffsetRequest, validate_consumer_group_id,
    validate_poll_query, validate_topic_name,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use flashq::Record;
use log::{error, trace};

#[tracing::instrument(level = "debug", skip(app_state, params), fields(group_id = %params.group_id))]
pub async fn create_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupIdParams>,
) -> Result<Json<ConsumerGroupResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "POST /consumer/{} validation failed: {}",
            params.group_id, error_response.message
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
            trace!("POST /consumer/{} - group created", params.group_id);
            Ok(Json(ConsumerGroupResponse {
                group_id: params.group_id,
            }))
        }
        Err(error) => {
            error!("POST /consumer/{} failed: {}", params.group_id, error);
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

#[tracing::instrument(level = "debug", skip(app_state, params), fields(group_id = %params.group_id))]
pub async fn leave_consumer_group(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupIdParams>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "DELETE /consumer/{} validation failed: {}",
            params.group_id, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    match app_state.queue.delete_consumer_group(&params.group_id) {
        Ok(_) => {
            trace!("DELETE /consumer/{} - consumer group left", params.group_id);
            Ok(StatusCode::NO_CONTENT)
        }
        Err(error) => {
            error!("DELETE /consumer/{} failed: {}", params.group_id, error);
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

#[tracing::instrument(level = "debug", skip(app_state, params), fields(group_id = %params.group_id, topic = %params.topic))]
pub async fn get_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
) -> Result<Json<OffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "GET /consumer/{}/topics/{}/offset group validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    if let Err(error_response) = validate_topic_name(&params.topic) {
        error!(
            "GET /consumer/{}/topics/{}/offset topic validation failed: {}",
            params.group_id, params.topic, error_response.message
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
            trace!(
                "GET /consumer/{}/topics/{}/offset - offset: {}",
                params.group_id, params.topic, committed_offset
            );
            let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);
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
            error!(
                "GET /consumer/{}/topics/{}/offset failed: {}",
                params.group_id, params.topic, error
            );
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

#[tracing::instrument(level = "debug", skip(app_state, params, request), fields(group_id = %params.group_id, topic = %params.topic))]
pub async fn commit_consumer_group_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Json(request): Json<UpdateConsumerGroupOffsetRequest>,
) -> Result<Json<GetConsumerGroupOffsetResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "POST /consumer/{}/topics/{}/offset group validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    if let Err(error_response) = validate_topic_name(&params.topic) {
        error!(
            "POST /consumer/{}/topics/{}/offset topic validation failed: {}",
            params.group_id, params.topic, error_response.message
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
            trace!(
                "POST /consumer/{}/topics/{}/offset - offset: {}",
                params.group_id, params.topic, request.offset
            );
            Ok(Json(GetConsumerGroupOffsetResponse {
                group_id: params.group_id.clone(),
                topic: params.topic.clone(),
                offset: request.offset,
            }))
        }
        Err(error) => {
            error!(
                "POST /consumer/{}/topics/{}/offset failed: {}",
                params.group_id, params.topic, error
            );
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

#[tracing::instrument(level = "debug", skip(app_state, params, query), fields(group_id = %params.group_id, topic = %params.topic))]
pub async fn fetch_records_by_offset(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Query(query): Query<PollOffsetQuery>,
) -> Result<Json<FetchResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "GET /consumer/{}/topics/{} by-offset group validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    if let Err(error_response) = validate_topic_name(&params.topic) {
        error!(
            "GET /consumer/{}/topics/{} by-offset topic validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    let pq = PollQuery {
        from_offset: query.from_offset,
        from_time: None,
        max_records: query.max_records,
        include_headers: query.include_headers,
    };
    if let Err(error_response) = validate_poll_query(&pq) {
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    let limit = pq.effective_limit();
    let include_headers = pq.should_include_headers();
    let records_result = match pq.from_offset {
        Some(offset) => app_state.queue.poll_records_for_consumer_group_from_offset(
            &params.group_id,
            &params.topic,
            offset,
            limit,
        ),
        None => {
            let current_offset = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic)
                .unwrap_or(0);
            app_state.queue.poll_records_for_consumer_group_from_offset(
                &params.group_id,
                &params.topic,
                current_offset,
                limit,
            )
        }
    };
    match records_result {
        Ok(records) => {
            trace!(
                "GET /consumer/{}/topics/{} by-offset - max_records: {:?} from_offset: {:?} - {} records returned",
                params.group_id,
                params.topic,
                limit,
                pq.from_offset,
                records.len()
            );
            let record_responses: Vec<flashq::RecordWithOffset> = if include_headers {
                records
            } else {
                records
                    .into_iter()
                    .map(|msg| flashq::RecordWithOffset {
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
            let next_offset = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic)
                .unwrap_or(0);
            let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);
            let response = FetchResponse::new(record_responses, next_offset, high_water_mark);
            Ok(Json(response))
        }
        Err(error) => {
            error!(
                "GET /consumer/{}/topics/{} by-offset failed: {}",
                params.group_id, params.topic, error
            );
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}

#[tracing::instrument(level = "debug", skip(app_state, params, query), fields(group_id = %params.group_id, topic = %params.topic))]
pub async fn fetch_records_by_time(
    State(app_state): State<AppState>,
    Path(params): Path<ConsumerGroupParams>,
    Query(query): Query<PollTimeQuery>,
) -> Result<Json<FetchResponse>, (StatusCode, Json<ErrorResponse>)> {
    if let Err(error_response) = validate_consumer_group_id(&params.group_id) {
        error!(
            "GET /consumer/{}/topics/{} by-time group validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    if let Err(error_response) = validate_topic_name(&params.topic) {
        error!(
            "GET /consumer/{}/topics/{} by-time topic validation failed: {}",
            params.group_id, params.topic, error_response.message
        );
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    let pq = PollQuery {
        from_offset: None,
        from_time: Some(query.from_time.clone()),
        max_records: query.max_records,
        include_headers: query.include_headers,
    };
    if let Err(error_response) = validate_poll_query(&pq) {
        return Err((
            error_to_status_code(&error_response.error),
            Json(error_response),
        ));
    }
    let limit = pq.effective_limit();
    let include_headers = pq.should_include_headers();
    let records_result = app_state.queue.poll_records_for_consumer_group_from_time(
        &params.group_id,
        &params.topic,
        pq.from_time.as_ref().unwrap(),
        limit,
    );
    match records_result {
        Ok(records) => {
            trace!(
                "GET /consumer/{}/topics/{} by-time - max_records: {:?} from_time: {} - {} records returned",
                params.group_id,
                params.topic,
                limit,
                pq.from_time.as_ref().unwrap(),
                records.len()
            );
            let record_responses: Vec<flashq::RecordWithOffset> = if include_headers {
                records
            } else {
                records
                    .into_iter()
                    .map(|msg| flashq::RecordWithOffset {
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
            let next_offset = app_state
                .queue
                .get_consumer_group_offset(&params.group_id, &params.topic)
                .unwrap_or(0);
            let high_water_mark = app_state.queue.get_high_water_mark(&params.topic);
            let response = FetchResponse::new(record_responses, next_offset, high_water_mark);
            Ok(Json(response))
        }
        Err(error) => {
            error!(
                "GET /consumer/{}/topics/{} by-time failed: {}",
                params.group_id, params.topic, error
            );
            let error_response = ErrorResponse::from(error);
            Err((
                error_to_status_code(&error_response.error),
                Json(error_response),
            ))
        }
    }
}
