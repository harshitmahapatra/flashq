//! Route configuration for FlashQ HTTP API

use axum::{
    Router,
    routing::{delete, get, post},
};

use super::{consumer, metadata, producer, server::AppState};

/// Creates the Axum router with all routes configured
pub fn create_router(app_state: AppState) -> Router {
    Router::new()
        // Producer routes
        .route("/topic/{topic}/record", post(producer::produce_records))
        // Metadata routes
        .route("/health", get(metadata::health_check))
        .route("/topics", get(metadata::get_topics))
        // Consumer group routes
        .route(
            "/consumer/{group_id}",
            post(consumer::create_consumer_group),
        )
        .route(
            "/consumer/{group_id}",
            delete(consumer::leave_consumer_group),
        )
        .route(
            "/consumer/{group_id}/topic/{topic}/record/offset",
            get(consumer::fetch_records_by_offset),
        )
        .route(
            "/consumer/{group_id}/topic/{topic}/record/time",
            get(consumer::fetch_records_by_time),
        )
        .route(
            "/consumer/{group_id}/topic/{topic}/offset",
            get(consumer::get_consumer_group_offset),
        )
        .route(
            "/consumer/{group_id}/topic/{topic}/offset",
            post(consumer::commit_consumer_group_offset),
        )
        .with_state(app_state)
}
