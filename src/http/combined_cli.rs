//! Combined CLI interface implementation for FlashQ HTTP client

use crate::http::{
    consumer::cli::{ConsumerCommands, handle_consumer_command},
    metadata::handle_health_command,
    producer::cli::{ProducerCommands, handle_producer_command},
};
use clap::{Parser, Subcommand};

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

// =============================================================================
// COMBINED COMMAND DISPATCHER
// =============================================================================

pub async fn handle_cli_command(client: &reqwest::Client, broker_url: &str, command: Commands) {
    match command {
        Commands::Health => {
            handle_health_command(client, broker_url).await;
        }
        Commands::Producer(producer_cmd) => {
            handle_producer_command(client, broker_url, producer_cmd).await;
        }
        Commands::Consumer(consumer_cmd) => {
            handle_consumer_command(client, broker_url, consumer_cmd).await;
        }
    }
}

// =============================================================================
// RE-EXPORTS FOR CONVENIENCE
// =============================================================================

// Re-export command types for external use
pub use crate::http::consumer::cli::OffsetCommands;

// Re-export command handlers for external use
pub use crate::http::consumer::cli::handle_offset_command;

// =============================================================================
// UNIT TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_struct_creation() {
        let cli = Cli {
            port: 9090,
            command: Commands::Health,
        };
        assert_eq!(cli.port, 9090);
    }

    #[test]
    fn test_commands_enum_variants() {
        let _health = Commands::Health;
        let _producer = Commands::Producer(ProducerCommands::Records {
            topic: "test".to_string(),
            message: Some("value".to_string()),
            key: None,
            header: None,
            batch: None,
        });
        let _consumer = Commands::Consumer(ConsumerCommands::Create {
            group_id: "group".to_string(),
        });
    }
}
