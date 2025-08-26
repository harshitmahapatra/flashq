//! HTTP client implementation and CLI command handlers for FlashQ

use crate::{Record, RecordWithOffset};
use super::types::*;
use std::collections::HashMap;

// =============================================================================
// CLI COMMAND HANDLERS
// =============================================================================

pub async fn handle_health_command(client: &reqwest::Client, server_url: &str) {
    let url = format!("{server_url}/health");
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<HealthCheckResponse>().await {
                    Ok(health_response) => {
                        println!("Server Status: {}", health_response.status);
                        println!("Service: {}", health_response.service);
                        println!("Timestamp: {}", health_response.timestamp);
                    }
                    Err(_) => println!("Server is healthy (response parsing failed)"),
                }
            } else {
                handle_error_response(response, "check server health").await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn handle_batch_post(
    client: &reqwest::Client,
    server_url: &str,
    topic: &str,
    batch_file: &str,
) {
    match std::fs::read_to_string(batch_file) {
        Ok(json_content) => match serde_json::from_str::<Vec<Record>>(&json_content) {
            Ok(records) => {
                let request = ProduceRequest { records };
                let url = format!("{server_url}/topics/{topic}/records");

                match client.post(&url).json(&request).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<ProduceResponse>().await {
                                Ok(produce_response) => {
                                    println!(
                                        "Posted {} records to topic '{}' starting at offset: {}",
                                        produce_response.offsets.len(),
                                        topic,
                                        produce_response
                                            .offsets
                                            .first()
                                            .map(|o| o.offset.to_string())
                                            .unwrap_or_else(|| "unknown".to_string())
                                    );
                                }
                                Err(e) => println!("Failed to parse response: {e}"),
                            }
                        } else {
                            handle_error_response(
                                response,
                                &format!("post batch to topic '{topic}'"),
                            )
                            .await;
                        }
                    }
                    Err(e) => println!("Failed to connect to server: {e}"),
                }
            }
            Err(e) => {
                println!("Failed to parse JSON file '{batch_file}': {e}");
                std::process::exit(1);
            }
        },
        Err(e) => {
            println!("Failed to read file '{batch_file}': {e}");
            std::process::exit(1);
        }
    }
}

pub async fn post_records(
    client: &reqwest::Client,
    server_url: &str,
    topic: &str,
    key: Option<String>,
    message: &str,
    headers: Option<HashMap<String, String>>,
) {
    let record = Record {
        key,
        value: message.to_string(),
        headers,
    };

    let request = ProduceRequest {
        records: vec![record],
    };

    let url = format!("{server_url}/topics/{topic}/records");

    match client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<ProduceResponse>().await {
                    Ok(produce_response) => {
                        if let Some(first_offset) = produce_response.offsets.first() {
                            println!(
                                "Posted record to topic '{}' with offset: {}",
                                topic, first_offset.offset
                            );
                        } else {
                            println!("Posted record to topic '{topic}' (no offset returned)");
                        }
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                handle_error_response(response, &format!("post to topic '{topic}'")).await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn create_consumer_group_command(client: &reqwest::Client, server_url: &str, group_id: &str) {
    let url = format!("{server_url}/consumer/{group_id}");
    match client.post(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!("Created consumer group '{group_id}'");
            } else {
                handle_error_response(response, &format!("create consumer group '{group_id}'"))
                    .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn leave_consumer_group_command(client: &reqwest::Client, server_url: &str, group_id: &str) {
    let url = format!("{server_url}/consumer/{group_id}");
    match client.delete(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!("Left consumer group '{group_id}'");
            } else {
                handle_error_response(response, &format!("leave consumer group '{group_id}'"))
                    .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn fetch_consumer_records_command(
    client: &reqwest::Client,
    server_url: &str,
    group_id: &str,
    topic: &str,
    max_records: Option<usize>,
    from_offset: Option<u64>,
    include_headers: Option<bool>,
) {
    let mut fetch_url = format!("{server_url}/consumer/{group_id}/topics/{topic}");
    let mut query_params = Vec::new();

    if let Some(c) = max_records {
        query_params.push(format!("max_records={c}"));
    }
    if let Some(offset) = from_offset {
        query_params.push(format!("from_offset={offset}"));
    }
    if let Some(headers) = include_headers {
        query_params.push(format!("include_headers={headers}"));
    }

    if !query_params.is_empty() {
        fetch_url.push_str(&format!("?{}", query_params.join("&")));
    }

    match client.get(&fetch_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<FetchResponse>().await {
                    Ok(fetch_response) => {
                        let record_count = fetch_response.records.len();
                        println!(
                            "Got {record_count} records for consumer group '{group_id}' from topic '{topic}'"
                        );

                        for record in fetch_response.records {
                            print_record(&record);
                        }

                        println!("Next offset: {}", fetch_response.next_offset);
                        if let Some(lag) = fetch_response.lag {
                            println!("Consumer lag: {lag}");
                        }
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                handle_error_response(
                    response,
                    &format!("fetch records for consumer group '{group_id}' from topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn commit_offset_command(
    client: &reqwest::Client,
    server_url: &str,
    group_id: &str,
    topic: &str,
    offset: u64,
) {
    let url = format!("{server_url}/consumer/{group_id}/topics/{topic}/offset");
    let request = UpdateConsumerGroupOffsetRequest { offset };
    match client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!(
                    "Committed offset {offset} for consumer group '{group_id}' and topic '{topic}'"
                );
            } else {
                handle_error_response(
                    response,
                    &format!("commit offset for consumer group '{group_id}' and topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

pub async fn get_offset_command(
    client: &reqwest::Client,
    server_url: &str,
    group_id: &str,
    topic: &str,
) {
    let url = format!("{server_url}/consumer/{group_id}/topics/{topic}/offset");
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<GetConsumerGroupOffsetResponse>().await {
                    Ok(offset_response) => {
                        println!(
                            "Consumer group '{}' offset for topic '{}': {}",
                            group_id, topic, offset_response.offset
                        );
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                handle_error_response(
                    response,
                    &format!("get offset for consumer group '{group_id}' and topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

pub async fn handle_error_response(response: reqwest::Response, operation: &str) {
    let status = response.status();

    match response.text().await {
        Ok(body) => match serde_json::from_str::<ErrorResponse>(&body) {
            Ok(error_response) => {
                println!("Failed to {}: {}", operation, error_response.error);
            }
            Err(parse_error) => {
                println!("Server error: {status} (failed to parse error response: {parse_error})");
                if !body.is_empty() {
                    println!("   Raw response: {}", body.trim());
                }
            }
        },
        Err(body_error) => {
            println!("Server error: {status} (failed to read response body: {body_error})");
        }
    }
}

pub fn print_record(record: &RecordWithOffset) {
    print!(
        "{} [{}] {}",
        record.timestamp, record.offset, record.record.value
    );

    if let Some(ref key) = record.record.key {
        print!(" (key: {key})");
    }

    if let Some(ref headers) = record.record.headers {
        if !headers.is_empty() {
            print!(" (headers: {headers:?})");
        }
    }

    println!();
}