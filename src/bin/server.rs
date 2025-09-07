//! FlashQ HTTP Server Binary
//!
//! Lightweight binary that instantiates and starts the FlashQ HTTP server.
//! All core server logic is implemented in src/http/server.rs.

use flashq::http::server::start_server;
use flashq::storage::file::SyncMode;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let mut port: u16 = 8080;
    let mut storage_selection: Option<&str> = None; // "memory" | "file"
    let mut data_dir: Option<PathBuf> = None;
    let mut batch_bytes: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--" => {
                i += 1;
                continue;
            }
            "--storage" => {
                i += 1;
                if i >= args.len() {
                    log::error!("--storage requires a value (memory|file)");
                    print_usage();
                    std::process::exit(1);
                }
                match args[i].as_str() {
                    "memory" | "file" => storage_selection = Some(args[i].as_str()),
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
                data_dir = Some(PathBuf::from(&args[i]));
            }
            "--batch-bytes" => {
                i += 1;
                if i >= args.len() {
                    log::error!("--batch-bytes requires a positive integer (bytes)");
                    print_usage();
                    std::process::exit(1);
                }
                match args[i].parse::<usize>() {
                    Ok(v) if v > 0 => batch_bytes = Some(v),
                    _ => {
                        log::error!("--batch-bytes must be a positive integer (bytes)");
                        print_usage();
                        std::process::exit(1);
                    }
                }
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

    // Build storage backend after parsing all flags
    let use_file = storage_selection == Some("file") || data_dir.is_some();
    let storage_backend = if use_file {
        let dir = data_dir.unwrap_or_else(|| PathBuf::from("./data"));
        match batch_bytes {
            Some(bb) => match flashq::storage::StorageBackend::new_file_with_path_and_batch_bytes(
                SyncMode::Immediate,
                dir,
                bb,
            ) {
                Ok(backend) => backend,
                Err(e) => {
                    log::error!("Failed to initialize file storage: {e}");
                    std::process::exit(1);
                }
            },
            None => {
                match flashq::storage::StorageBackend::new_file_with_path(SyncMode::Immediate, dir)
                {
                    Ok(backend) => backend,
                    Err(e) => {
                        log::error!("Failed to initialize file storage: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
    } else {
        match batch_bytes {
            Some(bb) => flashq::storage::StorageBackend::new_memory_with_batch_bytes(bb),
            None => flashq::storage::StorageBackend::new_memory(),
        }
    };

    if let Err(e) = start_server(port, storage_backend).await {
        log::error!("Server error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    log::info!(
        "Usage: server [port] [--storage <backend>] [--data-dir <path>] [--batch-bytes <n>]"
    );
    log::info!("  port: Port number to bind to (default: 8080)");
    log::info!("  --storage: Storage backend to use");
    log::info!("    memory: In-memory storage (default, data lost on restart)");
    log::info!("    file: File-based storage (data persisted to ./data directory)");
    log::info!("  --data-dir: Custom data directory for file storage (overrides --storage file)");
    log::info!(
        "  --batch-bytes: Target maximum bytes per storage I/O batch (default OS-aware ~128 KiB)"
    );
}
