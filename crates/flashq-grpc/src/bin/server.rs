use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::{Parser, ValueEnum};
use flashq_cluster::storage::{StorageBackend, file::SyncMode};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StorageKind {
    Memory,
    File,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum FileSyncMode {
    None,
    Immediate,
    Periodic,
}

impl From<FileSyncMode> for SyncMode {
    fn from(v: FileSyncMode) -> Self {
        match v {
            FileSyncMode::None => SyncMode::None,
            FileSyncMode::Immediate => SyncMode::Immediate,
            FileSyncMode::Periodic => SyncMode::Periodic,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "flashq-grpc-server",
    version,
    author,
    about = "FlashQ gRPC server"
)]
struct Args {
    /// Bind address (IP or hostname)
    #[arg(long, default_value = "0.0.0.0")]
    addr: String,

    /// Port to listen on
    #[arg(long, default_value_t = 50051)]
    port: u16,

    /// Storage backend: memory or file
    #[arg(long, value_enum, default_value_t = StorageKind::Memory)]
    storage: StorageKind,

    /// Data directory for file storage
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// File sync mode (file backend only)
    #[arg(long, value_enum, default_value_t = FileSyncMode::None)]
    sync: FileSyncMode,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging via tracing-subscriber with env filter support
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.addr, args.port).parse()?;

    let backend = match args.storage {
        StorageKind::Memory => StorageBackend::new_memory(),
        StorageKind::File => StorageBackend::new_file_with_path(args.sync.into(), &args.data_dir)?,
    };

    let core = Arc::new(flashq_cluster::FlashQ::with_storage_backend(backend));
    tracing::info!(%addr, "Starting FlashQ gRPC server");
    flashq_grpc::server::serve(addr, core).await?;
    Ok(())
}
