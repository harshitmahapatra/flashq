//! HTTP server (broker) implementation for FlashQ

use crate::{FlashQ, info};
use axum::http::StatusCode;
use std::sync::Arc;
use tokio::net::TcpListener;

use super::routes::create_router;

// =============================================================================
// APPLICATION STATE
// =============================================================================

pub type AppState = Arc<AppStateInner>;

#[derive(Clone)]
pub struct AppStateInner {
    pub queue: Arc<FlashQ>,
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/// Maps FlashQ error codes to HTTP status codes
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

// =============================================================================
// APPLICATION FACTORY & BROKER UTILITIES
// =============================================================================

/// Creates application state with storage backend
pub fn create_app_state(storage_backend: crate::storage::StorageBackend) -> AppState {
    Arc::new(AppStateInner {
        queue: Arc::new(FlashQ::with_storage_backend(storage_backend)),
    })
}

/// Starts the HTTP broker on the specified port with the given storage backend
pub async fn start_server(
    port: u16,
    storage_backend: crate::storage::StorageBackend,
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

// =============================================================================
// UNIT TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_to_status_code() {
        assert_eq!(
            error_to_status_code("invalid_parameter"),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_to_status_code("validation_error"),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_to_status_code("topic_not_found"),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            error_to_status_code("group_not_found"),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            error_to_status_code("record_validation_error"),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            error_to_status_code("internal_error"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            error_to_status_code("unknown_error"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
