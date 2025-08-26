//! FlashQ HTTP Server Binary
//! 
//! Lightweight binary that instantiates and starts the FlashQ HTTP server.
//! All core server logic is implemented in src/http/server.rs.

use flashq::http::server::start_server;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let port = if args.len() > 1 {
        match args[1].parse::<u16>() {
            Ok(p) => p,
            Err(_) => {
                eprintln!("Usage: server [port]");
                eprintln!("Invalid port number: {}", args[1]);
                std::process::exit(1);
            }
        }
    } else {
        8080
    };

    if let Err(e) = start_server(port).await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}