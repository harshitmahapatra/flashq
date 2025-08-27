//! FlashQ HTTP Server Binary
//!
//! Lightweight binary that instantiates and starts the FlashQ HTTP server.
//! All core server logic is implemented in src/http/server.rs.

use flashq::http::server::start_server;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut port = 8080;
    let mut storage_backend = flashq::storage::StorageBackend::new_memory();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--storage" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --storage requires a value (memory|file)");
                    print_usage();
                    std::process::exit(1);
                }
                match args[i].as_str() {
                    "memory" => storage_backend = flashq::storage::StorageBackend::new_memory(),
                    "file" => {
                        storage_backend = match flashq::storage::StorageBackend::new_file(
                            flashq::storage::file::SyncMode::Immediate,
                        ) {
                            Ok(backend) => backend,
                            Err(e) => {
                                eprintln!("Error: Failed to initialize file storage: {e}");
                                std::process::exit(1);
                            }
                        }
                    }
                    _ => {
                        eprintln!(
                            "Error: Invalid storage backend '{}'. Valid options: memory, file",
                            args[i]
                        );
                        print_usage();
                        std::process::exit(1);
                    }
                }
            }
            "--data-dir" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --data-dir requires a path");
                    print_usage();
                    std::process::exit(1);
                }
                let data_dir = std::path::PathBuf::from(&args[i]);
                storage_backend = match flashq::storage::StorageBackend::new_file_with_path(
                    flashq::storage::file::SyncMode::Immediate,
                    data_dir,
                ) {
                    Ok(backend) => backend,
                    Err(e) => {
                        eprintln!("Error: Failed to initialize file storage with custom path: {e}");
                        std::process::exit(1);
                    }
                };
            }
            arg => {
                if let Ok(p) = arg.parse::<u16>() {
                    port = p;
                } else {
                    eprintln!("Error: Invalid argument '{arg}'");
                    print_usage();
                    std::process::exit(1);
                }
            }
        }
        i += 1;
    }

    if let Err(e) = start_server(port, storage_backend).await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!("Usage: server [port] [--storage <backend>] [--data-dir <path>]");
    eprintln!("  port: Port number to bind to (default: 8080)");
    eprintln!("  --storage: Storage backend to use");
    eprintln!("    memory: In-memory storage (default, data lost on restart)");
    eprintln!("    file: File-based storage (data persisted to ./data directory)");
    eprintln!("  --data-dir: Custom data directory for file storage (overrides --storage file)");
}
