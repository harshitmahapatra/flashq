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

// =============================================================================
// TOPICS COMMANDS
// =============================================================================

pub async fn handle_get_topics_command(client: &reqwest::Client, broker_url: &str) {
    let url = format!("{broker_url}/topics");
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<TopicsResponse>().await {
                    Ok(topics_response) => {
                        if topics_response.topics.is_empty() {
                            println!("No topics found");
                        } else {
                            println!("Topics ({}):", topics_response.topics.len());
                            for topic in &topics_response.topics {
                                println!("  • {topic}");
                            }
                        }
                    }
                    Err(e) => println!("Failed to parse topics response: {e}"),
                }
            } else {
                super::error::handle_error_response(response, "get topics").await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}
