//! FlashQ HTTP Client Binary
//! 
//! Lightweight binary that provides CLI interface to the FlashQ HTTP server.
//! All core client and CLI logic is implemented in src/http/ modules.

use clap::Parser;
use flashq::http::cli::{Cli, handle_cli_command};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let server_url = format!("http://127.0.0.1:{}", cli.port);
    let client = reqwest::Client::new();

    handle_cli_command(&client, &server_url, cli.command).await;
}