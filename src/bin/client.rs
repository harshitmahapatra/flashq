use clap::{Parser, Subcommand};
use flashq::api::*;
use flashq::{Record, RecordWithOffset};

// =============================================================================
// CLI CONFIGURATION
// =============================================================================

#[derive(Parser)]
#[command(name = "client")]
#[command(about = "FlashQ Client")]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "8080")]
    port: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Health,

    #[command(subcommand)]
    Producer(ProducerCommands),

    #[command(subcommand)]
    Consumer(ConsumerCommands),
}

#[derive(Subcommand)]
enum ProducerCommands {
    Records {
        topic: String,
        #[arg(required_unless_present = "batch")]
        message: Option<String>,
        #[arg(short, long)]
        key: Option<String>,
        #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
        header: Option<Vec<String>>,
        #[arg(short, long)]
        batch: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConsumerCommands {
    Create {
        group_id: String,
    },
    Leave {
        group_id: String,
    },
    Fetch {
        group_id: String,
        topic: String,
        #[arg(long)]
        max_records: Option<usize>,
        #[arg(long)]
        from_offset: Option<u64>,
        #[arg(long)]
        include_headers: Option<bool>,
    },
    #[command(subcommand)]
    Offset(OffsetCommands),
}

#[derive(Subcommand)]
enum OffsetCommands {
    Commit {
        group_id: String,
        topic: String,
        offset: u64,
        #[arg(long)]
        metadata: Option<String>,
    },
    Get {
        group_id: String,
        topic: String,
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
        Commands::Health => {
            handle_health_command(&client, &server_url).await;
        }
        Commands::Producer(producer_cmd) => {
            handle_producer_command(&client, &server_url, producer_cmd).await;
        }
        Commands::Consumer(consumer_cmd) => {
            handle_consumer_command(&client, &server_url, consumer_cmd).await;
        }
    }
}

// =============================================================================
// COMMAND HANDLERS
// =============================================================================

async fn handle_producer_command(
    client: &reqwest::Client,
    server_url: &str,
    producer_cmd: ProducerCommands,
) {
    match producer_cmd {
        ProducerCommands::Records {
            topic,
            message,
            key,
            header,
            batch,
        } => {
            if let Some(batch_file) = batch {
                handle_batch_post(client, server_url, &topic, &batch_file).await;
            } else if let Some(message) = message {
                let headers = parse_headers(header);
                post_messages(client, server_url, &topic, key, &message, headers).await;
            } else {
                println!("Error: Either provide a message or use --batch with a JSON file");
                std::process::exit(1);
            }
        }
    }
}

async fn handle_consumer_command(
    client: &reqwest::Client,
    server_url: &str,
    consumer_cmd: ConsumerCommands,
) {
    match consumer_cmd {
        ConsumerCommands::Create { group_id } => {
            create_consumer_group_command(client, server_url, &group_id).await;
        }
        ConsumerCommands::Leave { group_id } => {
            leave_consumer_group_command(client, server_url, &group_id).await;
        }
        ConsumerCommands::Fetch {
            group_id,
            topic,
            max_records,
            from_offset,
            include_headers: _,
        } => {
            fetch_consumer_messages_command(
                client,
                server_url,
                &group_id,
                &topic,
                max_records,
                from_offset,
            )
            .await;
        }
        ConsumerCommands::Offset(offset_cmd) => {
            handle_offset_command(client, server_url, offset_cmd).await;
        }
    }
}

async fn handle_offset_command(
    client: &reqwest::Client,
    server_url: &str,
    offset_cmd: OffsetCommands,
) {
    match offset_cmd {
        OffsetCommands::Commit {
            group_id,
            topic,
            offset,
            metadata: _,
        } => {
            commit_offset_command(client, server_url, &group_id, &topic, offset).await;
        }
        OffsetCommands::Get { group_id, topic } => {
            get_offset_command(client, server_url, &group_id, &topic).await;
        }
    }
}

async fn handle_health_command(client: &reqwest::Client, server_url: &str) {
    let url = format!("{server_url}/health");
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!("Server is healthy");
            } else {
                handle_error_response(response, "check server health").await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

// =============================================================================
// PRODUCER OPERATIONS
// =============================================================================

async fn handle_batch_post(
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

async fn post_messages(
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
                                "Posted message to topic '{}' with offset: {}",
                                topic, first_offset.offset
                            );
                        } else {
                            println!("Posted message to topic '{topic}' (no offset returned)");
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

// =============================================================================
// CONSUMER OPERATIONS
// =============================================================================

async fn create_consumer_group_command(client: &reqwest::Client, server_url: &str, group_id: &str) {
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

async fn leave_consumer_group_command(client: &reqwest::Client, server_url: &str, group_id: &str) {
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

async fn fetch_consumer_messages_command(
    client: &reqwest::Client,
    server_url: &str,
    group_id: &str,
    topic: &str,
    max_records: Option<usize>,
    from_offset: Option<u64>,
) {
    let mut fetch_url = format!("{server_url}/consumer/{group_id}/topics/{topic}");
    let mut query_params = Vec::new();

    if let Some(c) = max_records {
        query_params.push(format!("max_records={c}"));
    }
    if let Some(offset) = from_offset {
        query_params.push(format!("from_offset={offset}"));
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
                            "Got {message_count} messages for consumer group '{group_id}' from topic '{topic}'"
                        );

                        for message in fetch_response.records {
                            print_message(&message);
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
                    &format!("fetch messages for consumer group '{group_id}' from topic '{topic}'"),
                )
                .await;
            }
        }
        Err(e) => println!("Failed to connect to server: {e}"),
    }
}

// =============================================================================
// OFFSET OPERATIONS
// =============================================================================

async fn commit_offset_command(
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

async fn get_offset_command(
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

async fn handle_error_response(response: reqwest::Response, operation: &str) {
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
                        eprintln!("Warning: Invalid header format '{h}', expected KEY=VALUE");
                        None
                    }
                }
            })
            .collect()
    })
}
