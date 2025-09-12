//! FlashQ HTTP Client Binary

use clap::Parser;
use flashq_http::combined_cli::{Cli, handle_cli_command};

#[tokio::main]
async fn main() {
    // Initialize tracing + log bridge; respects RUST_LOG
    flashq::telemetry::init();
    let cli = Cli::parse();
    let broker_url = format!("http://127.0.0.1:{}", cli.port);
    let client = reqwest::Client::new();
    handle_cli_command(&client, &broker_url, cli.command).await;
}
