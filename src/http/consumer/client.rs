//! HTTP consumer client functions for FlashQ

use crate::http::common::*;

// =============================================================================
// CONSUMER GROUP COMMANDS
// =============================================================================

pub async fn create_consumer_group_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
) {
    let url = format!("{broker_url}/consumer/{group_id}");
    match client.post(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!("Created consumer group '{group_id}'");
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!("create consumer group '{group_id}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}

pub async fn leave_consumer_group_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
) {
    let url = format!("{broker_url}/consumer/{group_id}");
    match client.delete(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!("Left consumer group '{group_id}'");
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!("leave consumer group '{group_id}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}

/// Fetch records using the new by-offset endpoint.
pub async fn fetch_consumer_records_by_offset_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
    topic: &str,
    from_offset: Option<u64>,
    max_records: Option<usize>,
    include_headers: Option<bool>,
) {
    let mut fetch_url = format!("{broker_url}/consumer/{group_id}/topic/{topic}/record/offset");
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
                            crate::http::common::print_record(&record);
                        }
                        println!("Next offset: {}", fetch_response.next_offset);
                        if let Some(lag) = fetch_response.lag {
                            println!("Consumer lag: {lag}");
                        }
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!(
                        "fetch (by-offset) for consumer group '{group_id}' from topic '{topic}'"
                    ),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}

/// Fetch records using the new by-time endpoint.
pub async fn fetch_consumer_records_by_time_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
    topic: &str,
    from_time: &str,
    max_records: Option<usize>,
    include_headers: Option<bool>,
) {
    let mut fetch_url = format!("{broker_url}/consumer/{group_id}/topic/{topic}/record/time");
    let mut query_params = Vec::new();
    query_params.push(format!("from_time={}", urlencoding::encode(from_time)));
    if let Some(c) = max_records {
        query_params.push(format!("max_records={c}"));
    }
    if let Some(headers) = include_headers {
        query_params.push(format!("include_headers={headers}"));
    }
    fetch_url.push_str(&format!("?{}", query_params.join("&")));

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
                            crate::http::common::print_record(&record);
                        }
                        println!("Next offset: {}", fetch_response.next_offset);
                        if let Some(lag) = fetch_response.lag {
                            println!("Consumer lag: {lag}");
                        }
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!(
                        "fetch (by-time) for consumer group '{group_id}' from topic '{topic}'"
                    ),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}

pub async fn commit_offset_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
    topic: &str,
    offset: u64,
) {
    let url = format!("{broker_url}/consumer/{group_id}/topic/{topic}/offset");
    let request = UpdateConsumerGroupOffsetRequest { offset };
    match client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!(
                    "Committed offset {offset} for consumer group '{group_id}' and topic '{topic}'"
                );
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!("commit offset for consumer group '{group_id}' and topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}

pub async fn get_offset_command(
    client: &reqwest::Client,
    broker_url: &str,
    group_id: &str,
    topic: &str,
) {
    let url = format!("{broker_url}/consumer/{group_id}/topic/{topic}/offset");
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
                crate::http::error::handle_error_response(
                    response,
                    &format!("get offset for consumer group '{group_id}' and topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}
