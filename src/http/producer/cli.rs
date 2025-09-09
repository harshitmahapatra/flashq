//! Producer CLI interface implementation

use super::client::*;
use crate::http::common::parse_headers;
use clap::Subcommand;

// =============================================================================
// PRODUCER CLI COMMANDS
// =============================================================================

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

// =============================================================================
// PRODUCER COMMAND HANDLERS
// =============================================================================

pub async fn handle_producer_command(
    client: &reqwest::Client,
    broker_url: &str,
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
                handle_batch_post(client, broker_url, &topic, &batch_file).await;
            } else if let Some(message) = message {
                let headers = parse_headers(header);
                post_records(client, broker_url, &topic, key, &message, headers).await;
            } else {
                println!("Error: Either provide a message or use --batch with a JSON file");
                std::process::exit(1);
            }
        }
    }
}

// =============================================================================
// UNIT TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_commands_enum_variants() {
        let _records = ProducerCommands::Records {
            topic: "test".to_string(),
            message: Some("value".to_string()),
            key: None,
            header: None,
            batch: None,
        };
    }
}
