use clap::{Parser, Subcommand};
use message_queue_rs::api::*;

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
    let request = PostRecordRequest {
        key: None,
        value: message.to_string(),
        headers: None,
    };

    let url = format!("{server_url}/topics/{topic}/records");

    match client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<PostRecordResponse>().await {
                    Ok(post_response) => {
                        println!(
                            "✓ Posted message to topic '{}' with offset: {}",
                            topic, post_response.offset
                        );
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
    let mut url = format!("{server_url}/topics/{topic}/messages");
    if let Some(c) = count {
        url.push_str(&format!("?count={c}"));
    }

    match client.get(&url).send().await {
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
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

async fn handle_error_response(response: reqwest::Response, operation: &str) {
    let status = response.status();
    match response.json::<ErrorResponse>().await {
        Ok(error_response) => {
            println!("✗ Failed to {}: {}", operation, error_response.error);
        }
        Err(_) => {
            println!("✗ Server error: {status}");
        }
    }
}

fn print_message(message: &RecordResponse) {
    print!(
        "{} [{}] {}",
        message.timestamp, message.offset, message.value
    );

    if let Some(ref key) = message.key {
        print!(" (key: {key})");
    }

    if let Some(ref headers) = message.headers {
        if !headers.is_empty() {
            print!(" (headers: {headers:?})");
        }
    }

    println!();
}
