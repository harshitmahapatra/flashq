//! HTTP producer client functions for FlashQ

use crate::Record;
use crate::http::common::*;
use std::collections::HashMap;

// =============================================================================
// PRODUCER COMMANDS
// =============================================================================

pub async fn handle_batch_post(
    client: &reqwest::Client,
    broker_url: &str,
    topic: &str,
    batch_file: &str,
) {
    match std::fs::read_to_string(batch_file) {
        Ok(json_content) => match serde_json::from_str::<Vec<Record>>(&json_content) {
            Ok(records) => {
                let request = ProduceRequest { records };
                let url = format!("{broker_url}/topic/{topic}/record");

                match client.post(&url).json(&request).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<ProduceResponse>().await {
                                Ok(produce_response) => {
                                    println!(
                                        "Posted {} records to topic '{}'. Last offset: {}",
                                        request.records.len(),
                                        topic,
                                        produce_response.offset
                                    );
                                }
                                Err(e) => println!("Failed to parse response: {e}"),
                            }
                        } else {
                            crate::http::error::handle_error_response(
                                response,
                                &format!("post batch to topic '{topic}'"),
                            )
                            .await;
                        }
                    }
                    Err(e) => println!("Failed to connect to broker: {e}"),
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
    broker_url: &str,
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

    let url = format!("{broker_url}/topic/{topic}/record");

    match client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<ProduceResponse>().await {
                    Ok(produce_response) => {
                        println!(
                            "Posted record to topic '{}' with offset: {}",
                            topic, produce_response.offset
                        );
                    }
                    Err(e) => println!("Failed to parse response: {e}"),
                }
            } else {
                crate::http::error::handle_error_response(
                    response,
                    &format!("post to topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to broker: {e}"),
    }
}
