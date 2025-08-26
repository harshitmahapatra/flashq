//! CLI interface implementation for FlashQ HTTP client

use clap::{Parser, Subcommand};
use super::{client::*, types::parse_headers};

// =============================================================================
// CLI CONFIGURATION STRUCTS
// =============================================================================

#[derive(Parser)]
#[command(name = "client")]
#[command(about = "FlashQ Client")]
#[command(version)]
pub struct Cli {
    #[arg(short, long, default_value = "8080")]
    pub port: u16,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Health,

    #[command(subcommand)]
    Producer(ProducerCommands),

    #[command(subcommand)]
    Consumer(ConsumerCommands),
}

#[derive(Subcommand)]
pub enum ProducerCommands {
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
pub enum ConsumerCommands {
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
pub enum OffsetCommands {
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
// COMMAND DISPATCHERS
// =============================================================================

pub async fn handle_cli_command(
    client: &reqwest::Client,
    server_url: &str,
    command: Commands,
) {
    match command {
        Commands::Health => {
            handle_health_command(client, server_url).await;
        }
        Commands::Producer(producer_cmd) => {
            handle_producer_command(client, server_url, producer_cmd).await;
        }
        Commands::Consumer(consumer_cmd) => {
            handle_consumer_command(client, server_url, consumer_cmd).await;
        }
    }
}

pub async fn handle_producer_command(
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
                post_records(client, server_url, &topic, key, &message, headers).await;
            } else {
                println!("Error: Either provide a message or use --batch with a JSON file");
                std::process::exit(1);
            }
        }
    }
}

pub async fn handle_consumer_command(
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
            include_headers,
        } => {
            fetch_consumer_records_command(
                client,
                server_url,
                &group_id,
                &topic,
                max_records,
                from_offset,
                include_headers,
            )
            .await;
        }
        ConsumerCommands::Offset(offset_cmd) => {
            handle_offset_command(client, server_url, offset_cmd).await;
        }
    }
}

pub async fn handle_offset_command(
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