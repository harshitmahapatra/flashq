//! Consumer CLI interface implementation

use super::client::*;
use clap::Subcommand;

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
        from_time: Option<String>,
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

pub async fn handle_consumer_command(
    client: &reqwest::Client,
    broker_url: &str,
    consumer_cmd: ConsumerCommands,
) {
    match consumer_cmd {
        ConsumerCommands::Create { group_id } => {
            create_consumer_group_command(client, broker_url, &group_id).await;
        }
        ConsumerCommands::Leave { group_id } => {
            leave_consumer_group_command(client, broker_url, &group_id).await;
        }
        ConsumerCommands::Fetch {
            group_id,
            topic,
            max_records,
            from_offset,
            from_time,
            include_headers,
        } => {
            if from_offset.is_some() && from_time.is_some() {
                eprintln!(
                    "Error: --from-offset and --from-time are mutually exclusive. Choose one."
                );
                std::process::exit(1);
            }
            if let Some(ts) = from_time.as_ref() {
                fetch_consumer_records_by_time_command(
                    client,
                    broker_url,
                    &group_id,
                    &topic,
                    ts,
                    max_records,
                    include_headers,
                )
                .await;
            } else {
                fetch_consumer_records_by_offset_command(
                    client,
                    broker_url,
                    &group_id,
                    &topic,
                    from_offset,
                    max_records,
                    include_headers,
                )
                .await;
            }
        }
        ConsumerCommands::Offset(offset_cmd) => {
            handle_offset_command(client, broker_url, offset_cmd).await;
        }
    }
}

pub async fn handle_offset_command(
    client: &reqwest::Client,
    broker_url: &str,
    offset_cmd: OffsetCommands,
) {
    match offset_cmd {
        OffsetCommands::Commit {
            group_id,
            topic,
            offset,
            metadata: _,
        } => {
            commit_offset_command(client, broker_url, &group_id, &topic, offset).await;
        }
        OffsetCommands::Get { group_id, topic } => {
            get_offset_command(client, broker_url, &group_id, &topic).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_consumer_commands_enum_variants() {
        let _create = ConsumerCommands::Create {
            group_id: "group".to_string(),
        };
        let _fetch = ConsumerCommands::Fetch {
            group_id: "group".to_string(),
            topic: "topic".to_string(),
            max_records: None,
            from_offset: None,
            from_time: None,
            include_headers: None,
        };
    }
    #[test]
    fn test_offset_commands_enum_variants() {
        let _commit = OffsetCommands::Commit {
            group_id: "group".to_string(),
            topic: "topic".to_string(),
            offset: 42,
            metadata: None,
        };
        let _get = OffsetCommands::Get {
            group_id: "group".to_string(),
            topic: "topic".to_string(),
        };
    }
}
