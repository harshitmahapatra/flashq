//! FlashQ HTTP Client Binary
//!
//! Lightweight binary that provides CLI interface to the FlashQ HTTP broker.
//! All core client and CLI logic is implemented in src/http/ modules.

use clap::Parser;
use flashq::http::combined_cli::{Cli, handle_cli_command};

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    let broker_url = format!("http://127.0.0.1:{}", cli.port);
    let client = reqwest::Client::new();

    handle_cli_command(&client, &broker_url, cli.command).await;
}
