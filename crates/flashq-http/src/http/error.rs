//! HTTP error handling utilities for FlashQ

use super::common::ErrorResponse;

pub async fn handle_error_response(response: reqwest::Response, operation: &str) {
    let status = response.status();
    match response.text().await {
        Ok(body) => match serde_json::from_str::<ErrorResponse>(&body) {
            Ok(error_response) => {
                println!("Failed to {}: {}", operation, error_response.error);
            }
            Err(parse_error) => {
                println!("Broker error: {status} (failed to parse error response: {parse_error})");
                if !body.is_empty() {
                    println!("   Raw response: {}", body.trim());
                }
            }
        },
        Err(body_error) => {
            println!("Broker error: {status} (failed to read response body: {body_error})");
        }
    }
}
