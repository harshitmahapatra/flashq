//! FlashQ HTTP Server Binary
//!
//! Lightweight binary that instantiates and starts the FlashQ HTTP server.
//! All core server logic is implemented in src/http/server.rs.

use flashq::http::server::start_server;
use std::env;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let mut port = 8080;
    let mut storage_backend = flashq::storage::StorageBackend::new_memory();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--storage" => {
                i += 1;
                if i >= args.len() {
                    log::error!("--storage requires a value (memory|file)");
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
                                log::error!("Failed to initialize file storage: {e}");
                                std::process::exit(1);
                            }
                        }
                    }
                    _ => {
                        log::error!(
                            "Invalid storage backend '{}'. Valid options: memory, file",
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
                    log::error!("--data-dir requires a path");
                    print_usage();
                    std::process::exit(1);
                }
                let data_dir = std::path::PathBuf::from(&args[i]);
                storage_backend = match flashq::storage::StorageBackend::new_file_with_path(
                    flashq::storage::file::SyncMode::Immediate,
                    flashq::storage::file::common::FileIoMode::default(),
                    data_dir,
                ) {
                    Ok(backend) => backend,
                    Err(e) => {
                        log::error!("Failed to initialize file storage with custom path: {e}");
                        std::process::exit(1);
                    }
                };
            }
            arg => {
                if let Ok(p) = arg.parse::<u16>() {
                    port = p;
                } else {
                    log::error!("Invalid argument '{arg}'");
                    print_usage();
                    std::process::exit(1);
                }
            }
        }
        i += 1;
    }

    if let Err(e) = start_server(port, storage_backend).await {
        log::error!("Server error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    log::info!("Usage: server [port] [--storage <backend>] [--data-dir <path>]");
    log::info!("  port: Port number to bind to (default: 8080)");
    log::info!("  --storage: Storage backend to use");
    log::info!("    memory: In-memory storage (default, data lost on restart)");
    log::info!("    file: File-based storage (data persisted to ./data directory)");
    log::info!("  --data-dir: Custom data directory for file storage (overrides --storage file)");
}
