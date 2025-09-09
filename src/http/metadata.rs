//! HTTP metadata client functions for FlashQ

use super::common::*;

// =============================================================================
// HEALTH CHECK COMMANDS
// =============================================================================

pub async fn handle_health_command(client: &reqwest::Client, broker_url: &str) {
    let url = format!("{broker_url}/health");
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<HealthCheckResponse>().await {
                    Ok(health_response) => {
                        println!("Broker Status: {}", health_response.status);
                        println!("Service: {}", health_response.service);
                        println!("Timestamp: {}", health_response.timestamp);
                    }
                    Err(_) => println!("Broker is healthy (response parsing failed)"),
                }
            } else {
                super::error::handle_error_response(response, "check broker health").await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}
