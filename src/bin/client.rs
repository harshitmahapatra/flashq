use clap::{Parser, Subcommand};
use flashq::api::*;
use flashq::{Record, RecordWithOffset};

// =============================================================================
// CLI CONFIGURATION
// =============================================================================

#[derive(Parser)]
#[command(name = "client")]
#[command(about = "Message Queue Client")]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "8080")]
    port: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Post {
        topic: String,
        #[arg(required_unless_present = "batch")]
        message: Option<String>,
        #[arg(short, long)]
        key: Option<String>,
        #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
        header: Option<Vec<String>>,
        #[arg(short, long)]
        batch: Option<String>, // JSON file path for batch posting
    },
    Poll {
        topic: String,
        #[arg(short, long)]
        count: Option<usize>,
        #[arg(long)]
        from_offset: Option<u64>,
    },
    ConsumerGroup {
        #[command(subcommand)]
        action: ConsumerGroupAction,
    },
    Health,
}

#[derive(Subcommand)]
enum ConsumerGroupAction {
    Create {
        group_id: String,
    },
    Leave {
        group_id: String,
    },
    Poll {
        group_id: String,
        topic: String,
        #[arg(short, long)]
        count: Option<usize>,
        #[arg(long)]
        from_offset: Option<u64>,
    },
    Offset {
        group_id: String,
        topic: String,
        #[arg(long)]
        get: bool,
        #[arg(long)]
        set: Option<u64>,
    },
}

// =============================================================================
// MAIN APPLICATION
// =============================================================================

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let server_url = format!("http://127.0.0.1:{}", cli.port);
    let client = reqwest::Client::new();

    match cli.command {
        Commands::Post {
            topic,
            message,
            key,
            header,
            batch,
        } => {
            if let Some(batch_file) = batch {
                // Implement batch posting from JSON file
                match std::fs::read_to_string(&batch_file) {
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
                                                    "✓ Posted {} records to topic '{}' starting at offset: {}",
                                                    produce_response.offsets.len(),
                                                    topic,
                                                    produce_response
                                                        .offsets
                                                        .first()
                                                        .map(|o| o.offset.to_string())
                                                        .unwrap_or_else(|| "unknown".to_string())
                                                );
                                            }
                                            Err(e) => println!("✗ Failed to parse response: {e}"),
                                        }
                                    } else {
                                        handle_error_response(
                                            response,
                                            &format!("post batch to topic '{topic}'"),
                                        )
                                        .await;
                                    }
                                }
                                Err(e) => println!("✗ Failed to connect to server: {e}"),
                            }
                        }
                        Err(e) => {
                            println!("✗ Failed to parse JSON file '{}': {}", batch_file, e);
                            std::process::exit(1);
                        }
                    },
                    Err(e) => {
                        println!("✗ Failed to read file '{}': {}", batch_file, e);
                        std::process::exit(1);
                    }
                }
            } else if let Some(message) = message {
                let headers = parse_headers(header);
                post_message_with_record(&client, &server_url, &topic, key, &message, headers)
                    .await;
            } else {
                println!("✗ Either provide a message or use --batch with a JSON file");
                std::process::exit(1);
            }
        }
        Commands::Poll {
            topic,
            count,
            from_offset,
        } => {
            if let Some(offset) = from_offset {
                // Implement polling from specific offset using temporary consumer group
                let temp_group_id = format!("client-poll-{}", std::process::id());

                // Create consumer group
                let create_url = format!("{server_url}/consumer/{temp_group_id}");
                match client.post(&create_url).send().await {
                    Ok(response) => {
                        if !response.status().is_success() {
                            handle_error_response(
                                response,
                                &format!("create temporary consumer group '{temp_group_id}'"),
                            )
                            .await;
                            return;
                        }
                    }
                    Err(e) => {
                        println!("✗ Failed to create temporary consumer group: {e}");
                        return;
                    }
                }

                // Set offset for consumer group
                let offset_url =
                    format!("{server_url}/consumer/{temp_group_id}/topics/{topic}/offset");
                let offset_request = UpdateConsumerGroupOffsetRequest { offset };
                match client.post(&offset_url).json(&offset_request).send().await {
                    Ok(response) => {
                        if !response.status().is_success() {
                            handle_error_response(
                                response,
                                &format!(
                                    "set offset for temporary consumer group '{temp_group_id}'"
                                ),
                            )
                            .await;
                            let _ = client
                                .delete(format!("{server_url}/consumer/{temp_group_id}"))
                                .send()
                                .await;
                            return;
                        }
                    }
                    Err(e) => {
                        println!("✗ Failed to set offset: {e}");
                        let _ = client
                            .delete(format!("{server_url}/consumer/{temp_group_id}"))
                            .send()
                            .await;
                        return;
                    }
                }

                // Fetch messages
                let mut fetch_url = format!("{server_url}/consumer/{temp_group_id}/topics/{topic}");
                if let Some(c) = count {
                    fetch_url.push_str(&format!("?count={c}"));
                }

                match client.get(&fetch_url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<FetchResponse>().await {
                                Ok(fetch_response) => {
                                    let message_count = fetch_response.records.len();
                                    println!(
                                        "✓ Got {} messages for topic '{}' from offset {}",
                                        message_count, topic, offset
                                    );

                                    for message in fetch_response.records {
                                        print_message(&message);
                                    }
                                }
                                Err(e) => println!("✗ Failed to parse response: {e}"),
                            }
                        } else {
                            handle_error_response(
                                response,
                                &format!("fetch messages from topic '{topic}' at offset {offset}"),
                            )
                            .await;
                        }
                    }
                    Err(e) => println!("✗ Failed to connect to server: {e}"),
                }

                // Clean up temporary consumer group
                let _ = client
                    .delete(format!("{server_url}/consumer/{temp_group_id}"))
                    .send()
                    .await;
            } else {
                poll_messages(&client, &server_url, &topic, count).await;
            }
        }
        Commands::ConsumerGroup { action } => {
            match action {
                ConsumerGroupAction::Create { group_id } => {
                    // Implement consumer group creation
                    let url = format!("{server_url}/consumer/{group_id}");
                    match client.post(&url).send().await {
                        Ok(response) => {
                            if response.status().is_success() {
                                println!("✓ Created consumer group '{}'", group_id);
                            } else {
                                handle_error_response(
                                    response,
                                    &format!("create consumer group '{group_id}'"),
                                )
                                .await;
                            }
                        }
                        Err(e) => println!("✗ Failed to connect to server: {e}"),
                    }
                }
                ConsumerGroupAction::Leave { group_id } => {
                    // Implement leaving consumer group
                    let url = format!("{server_url}/consumer/{group_id}");
                    match client.delete(&url).send().await {
                        Ok(response) => {
                            if response.status().is_success() {
                                println!("✓ Left consumer group '{}'", group_id);
                            } else {
                                handle_error_response(
                                    response,
                                    &format!("leave consumer group '{group_id}'"),
                                )
                                .await;
                            }
                        }
                        Err(e) => println!("✗ Failed to connect to server: {e}"),
                    }
                }
                ConsumerGroupAction::Poll {
                    group_id,
                    topic,
                    count,
                    from_offset,
                } => {
                    // Implement consumer group polling
                    let mut fetch_url = format!("{server_url}/consumer/{group_id}/topics/{topic}");
                    let mut query_params = Vec::new();

                    if let Some(c) = count {
                        query_params.push(format!("count={}", c));
                    }
                    if let Some(offset) = from_offset {
                        query_params.push(format!("from_offset={}", offset));
                    }

                    if !query_params.is_empty() {
                        fetch_url.push_str(&format!("?{}", query_params.join("&")));
                    }

                    match client.get(&fetch_url).send().await {
                        Ok(response) => {
                            if response.status().is_success() {
                                match response.json::<FetchResponse>().await {
                                    Ok(fetch_response) => {
                                        let message_count = fetch_response.records.len();
                                        println!(
                                            "✓ Got {} messages for consumer group '{}' from topic '{}'",
                                            message_count, group_id, topic
                                        );

                                        for message in fetch_response.records {
                                            print_message(&message);
                                        }

                                        println!("Next offset: {}", fetch_response.next_offset);
                                        if let Some(lag) = fetch_response.lag {
                                            println!("Consumer lag: {}", lag);
                                        }
                                    }
                                    Err(e) => println!("✗ Failed to parse response: {e}"),
                                }
                            } else {
                                handle_error_response(
                                    response,
                                    &format!(
                                        "poll consumer group '{group_id}' for topic '{topic}'"
                                    ),
                                )
                                .await;
                            }
                        }
                        Err(e) => println!("✗ Failed to connect to server: {e}"),
                    }
                }
                ConsumerGroupAction::Offset {
                    group_id,
                    topic,
                    get,
                    set,
                } => {
                    if get {
                        // Implement getting consumer group offset
                        let url = format!("{server_url}/consumer/{group_id}/topics/{topic}/offset");
                        match client.get(&url).send().await {
                            Ok(response) => {
                                if response.status().is_success() {
                                    match response.json::<GetConsumerGroupOffsetResponse>().await {
                                        Ok(offset_response) => {
                                            println!(
                                                "✓ Consumer group '{}' offset for topic '{}': {}",
                                                group_id, topic, offset_response.offset
                                            );
                                        }
                                        Err(e) => println!("✗ Failed to parse response: {e}"),
                                    }
                                } else {
                                    handle_error_response(response, &format!("get offset for consumer group '{group_id}' and topic '{topic}'")).await;
                                }
                            }
                            Err(e) => println!("✗ Failed to connect to server: {e}"),
                        }
                    } else if let Some(offset) = set {
                        // Implement setting consumer group offset
                        let url = format!("{server_url}/consumer/{group_id}/topics/{topic}/offset");
                        let request = UpdateConsumerGroupOffsetRequest { offset };
                        match client.post(&url).json(&request).send().await {
                            Ok(response) => {
                                if response.status().is_success() {
                                    println!(
                                        "✓ Set offset {} for consumer group '{}' and topic '{}'",
                                        offset, group_id, topic
                                    );
                                } else {
                                    handle_error_response(response, &format!("set offset for consumer group '{group_id}' and topic '{topic}'")).await;
                                }
                            }
                            Err(e) => println!("✗ Failed to connect to server: {e}"),
                        }
                    } else {
                        println!("✗ Must specify either --get or --set with offset command");
                        std::process::exit(1);
                    }
                }
            }
        }
        Commands::Health => {
            // Implement health check
            let url = format!("{server_url}/health");
            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        // Simple health check - just check if response is OK
                        println!("✓ Server is healthy");
                    } else {
                        handle_error_response(response, "check server health").await;
                    }
                }
                Err(e) => println!("✗ Failed to connect to server: {e}"),
            }
        }
    }
}

fn parse_headers(
    header_args: Option<Vec<String>>,
) -> Option<std::collections::HashMap<String, String>> {
    header_args.map(|headers| {
        headers
            .iter()
            .filter_map(|h| {
                let mut split = h.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => {
                        eprintln!("Warning: Invalid header format '{}', expected KEY=VALUE", h);
                        None
                    }
                }
            })
            .collect()
    })
}

async fn post_message_with_record(
    client: &reqwest::Client,
    server_url: &str,
    topic: &str,
    key: Option<String>,
    message: &str,
    headers: Option<std::collections::HashMap<String, String>>,
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
                                "✓ Posted message to topic '{}' with offset: {}",
                                topic, first_offset.offset
                            );
                        } else {
                            println!("✓ Posted message to topic '{topic}' (no offset returned)");
                        }
                    }
                    Err(e) => println!("✗ Failed to parse response: {e}"),
                }
            } else {
                handle_error_response(response, &format!("post to topic '{topic}'")).await;
            }
        }
        Err(e) => println!("✗ Failed to connect to server: {e}"),
    }
}

// =============================================================================
// COMMAND IMPLEMENTATIONS
// =============================================================================

async fn poll_messages(
    client: &reqwest::Client,
    server_url: &str,
    topic: &str,
    count: Option<usize>,
) {
    // Generate a unique consumer group ID for this client session
    let group_id = format!("client-{}", std::process::id());

    // Create consumer group
    let create_url = format!("{server_url}/consumer/{group_id}");
    match client.post(&create_url).send().await {
        Ok(response) => {
            if !response.status().is_success() {
                handle_error_response(response, &format!("create consumer group '{group_id}'"))
                    .await;
                return;
            }
        }
        Err(e) => {
            println!("✗ Failed to create consumer group: {e}");
            return;
        }
    }

    // Fetch messages from consumer group
    let mut fetch_url = format!("{server_url}/consumer/{group_id}/topics/{topic}");
    if let Some(c) = count {
        fetch_url.push_str(&format!("?max_records={c}"));
    }

    match client.get(&fetch_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<FetchResponse>().await {
                    Ok(fetch_response) => {
                        let message_count = fetch_response.records.len();
                        println!("✓ Got {} messages for topic '{}'", message_count, topic);

                        // Get last offset before consuming records
                        let commit_offset = if !fetch_response.records.is_empty() {
                            Some(fetch_response.records.last().unwrap().offset + 1)
                        } else {
                            None
                        };

                        for message in fetch_response.records {
                            print_message(&message);
                        }

                        // Commit offset after successfully processing messages (Kafka-style)
                        if let Some(offset) = commit_offset {
                            commit_consumer_group_offset(
                                client, server_url, &group_id, topic, offset,
                            )
                            .await;
                        }
                    }
                    Err(e) => println!("✗ Failed to parse response: {e}"),
                }
            } else {
                handle_error_response(response, &format!("retrieve messages from topic '{topic}'"))
                    .await;
            }
        }
        Err(e) => println!("✗ Failed to connect to server: {e}"),
    }

    // Clean up consumer group
    let delete_url = format!("{server_url}/consumer/{group_id}");
    let _ = client.delete(&delete_url).send().await;
}

async fn commit_consumer_group_offset(
    client: &reqwest::Client,
    server_url: &str,
    group_id: &str,
    topic: &str,
    offset: u64,
) {
    use flashq::api::UpdateConsumerGroupOffsetRequest;

    let commit_url = format!("{server_url}/consumer/{group_id}/topics/{topic}/offset");
    let request = UpdateConsumerGroupOffsetRequest { offset };

    match client.post(&commit_url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!(
                    "✓ Committed offset {} for consumer group '{}'",
                    offset, group_id
                );
            } else {
                handle_error_response(response, &format!("commit offset for group '{group_id}'"))
                    .await;
            }
        }
        Err(e) => println!("✗ Failed to commit offset: {e}"),
    }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

async fn handle_error_response(response: reqwest::Response, operation: &str) {
    let status = response.status();

    // Try to get the response body as text first
    match response.text().await {
        Ok(body) => {
            // Try to parse as ErrorResponse JSON
            match serde_json::from_str::<ErrorResponse>(&body) {
                Ok(error_response) => {
                    println!("✗ Failed to {}: {}", operation, error_response.error);
                }
                Err(parse_error) => {
                    println!(
                        "✗ Server error: {status} (failed to parse error response: {parse_error})"
                    );
                    if !body.is_empty() {
                        println!("   Raw response: {}", body.trim());
                    }
                }
            }
        }
        Err(body_error) => {
            println!("✗ Server error: {status} (failed to read response body: {body_error})");
        }
    }
}

fn print_message(message: &RecordWithOffset) {
    print!(
        "{} [{}] {}",
        message.timestamp, message.offset, message.record.value
    );

    if let Some(ref key) = message.record.key {
        print!(" (key: {key})");
    }

    if let Some(ref headers) = message.record.headers {
        if !headers.is_empty() {
            print!(" (headers: {headers:?})");
        }
    }

    println!();
}
