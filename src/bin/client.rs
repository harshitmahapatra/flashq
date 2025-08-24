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
        message: String,
    },
    Poll {
        topic: String,
        #[arg(short, long)]
        count: Option<usize>,
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
        Commands::Post { topic, message } => {
            post_message(&client, &server_url, &topic, &message).await;
        }
        Commands::Poll { topic, count } => {
            poll_messages(&client, &server_url, &topic, count).await;
        }
    }
}

// =============================================================================
// COMMAND IMPLEMENTATIONS
// =============================================================================

async fn post_message(client: &reqwest::Client, server_url: &str, topic: &str, message: &str) {
    let record = Record {
        key: None,
        value: message.to_string(),
        headers: None,
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
                        println!(
                            "✓ Got {} messages for topic '{}'",
                            fetch_response.records.len(),
                            topic
                        );
                        for message in fetch_response.records {
                            print_message(&message);
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
