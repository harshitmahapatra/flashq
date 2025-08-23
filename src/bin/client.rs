// chrono imports removed since we now use ISO 8601 strings directly
use clap::{Parser, Subcommand};
use message_queue_rs::api::*;

#[derive(Parser)]
#[command(name = "client")]
#[command(about = "Message Queue Client")]
#[command(version)]
struct Cli {
    /// Server port
    #[arg(short, long, default_value = "8080")]
    port: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Post a message to a topic
    Post {
        /// Topic name
        topic: String,
        /// Message content
        message: String,
    },
    /// Poll messages from a topic
    Poll {
        /// Topic name
        topic: String,
        /// Number of messages to retrieve
        #[arg(short, long)]
        count: Option<usize>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let server_url = format!("http://127.0.0.1:{}", cli.port);
    let client = reqwest::Client::new();

    match cli.command {
        Commands::Post { topic, message } => {
            let request = PostMessageRequest {
                key: None,
                value: message.clone(),
                headers: None,
            };

            let url = format!("{server_url}/api/topics/{topic}/messages");

            match client.post(&url).json(&request).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<PostMessageResponse>().await {
                            Ok(post_response) => {
                                println!(
                                    "✓ Posted message to topic '{}' with offset: {}",
                                    topic, post_response.offset
                                );
                            }
                            Err(e) => println!("✗ Failed to parse response: {e}"),
                        }
                    } else {
                        let status = response.status();
                        match response.json::<ErrorResponse>().await {
                            Ok(error_response) => {
                                println!(
                                    "✗ Failed to post to topic '{}': {}",
                                    topic, error_response.error
                                );
                            }
                            Err(_) => {
                                println!("✗ Server error: {status}");
                            }
                        }
                    }
                }
                Err(e) => println!("✗ Failed to connect to server: {e}"),
            }
        }

        Commands::Poll { topic, count } => {
            let mut url = format!("{server_url}/api/topics/{topic}/messages");
            if let Some(c) = count {
                url.push_str(&format!("?count={c}"));
            }

            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<PollMessagesResponse>().await {
                            Ok(poll_response) => {
                                println!(
                                    "✓ Got {} messages for topic '{}'",
                                    poll_response.count, topic
                                );
                                for message in poll_response.messages {
                                    print!(
                                        "{} [{}] {}",
                                        message.timestamp, message.offset, message.value
                                    );

                                    // Display key if present
                                    if let Some(ref key) = message.key {
                                        print!(" (key: {key})");
                                    }

                                    // Display headers if present
                                    if let Some(ref headers) = message.headers {
                                        if !headers.is_empty() {
                                            print!(" (headers: {headers:?})");
                                        }
                                    }

                                    println!();
                                }
                            }
                            Err(e) => println!("✗ Failed to parse response: {e}"),
                        }
                    } else {
                        let status = response.status();
                        match response.json::<ErrorResponse>().await {
                            Ok(error_response) => {
                                println!(
                                    "✗ Error retrieving messages from topic '{}': {}",
                                    topic, error_response.error
                                );
                            }
                            Err(_) => {
                                println!("✗ Server error: {status}");
                            }
                        }
                    }
                }
                Err(e) => println!("✗ Failed to connect to server: {e}"),
            }
        }
    }
}
