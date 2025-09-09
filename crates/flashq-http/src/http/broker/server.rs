//! HTTP server (broker) implementation for FlashQ

use axum::http::StatusCode;
use flashq::FlashQ;
use log::info;
use std::sync::Arc;
use tokio::net::TcpListener;

use super::routes::create_router;

pub type AppState = Arc<AppStateInner>;

#[derive(Clone)]
pub struct AppStateInner {
    pub queue: Arc<FlashQ>,
}

pub fn error_to_status_code(error_code: &str) -> StatusCode {
    match error_code {
        "invalid_parameter" | "validation_error" => StatusCode::BAD_REQUEST,
        "topic_not_found" | "group_not_found" => StatusCode::NOT_FOUND,
        "conflict" => StatusCode::CONFLICT,
        "invalid_offset" => StatusCode::BAD_REQUEST,
        "record_validation_error" => StatusCode::UNPROCESSABLE_ENTITY,
        "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn create_app_state(storage_backend: flashq::storage::StorageBackend) -> AppState {
    Arc::new(AppStateInner {
        queue: Arc::new(FlashQ::with_storage_backend(storage_backend)),
    })
}

pub async fn start_server(
    port: u16,
    storage_backend: flashq::storage::StorageBackend,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_state = create_app_state(storage_backend);
    let app = create_router(app_state.clone());
    let bind_address = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&bind_address)
        .await
        .map_err(|e| format!("Failed to bind to address {bind_address}: {e}"))?;
    info!("FlashQ broker starting on http://{bind_address}");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("Broker failed to start: {e}"))?;
    Ok(())
}
