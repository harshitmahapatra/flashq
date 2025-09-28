use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::{Parser, ValueEnum};
use flashq_cluster::{
    manifest::loader::ManifestLoader,
    metadata_store::MetadataBackend,
    service::ClusterServiceImpl,
    storage::{StorageBackend, file::SyncMode},
    types::BrokerId,
};

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

    /// Cluster manifest file path
    #[arg(long)]
    manifest: Option<PathBuf>,

    /// Broker ID for cluster operations
    #[arg(long, default_value_t = 1)]
    broker_id: u32,
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

    // Create metadata store backend for cluster operations
    let metadata_backend = match args.storage {
        StorageKind::Memory => MetadataBackend::new_memory(),
        StorageKind::File => MetadataBackend::new_file(&args.data_dir),
    };

    // Create metadata store with optional manifest
    let metadata_store = if let Some(manifest_path) = args.manifest {
        let manifest = ManifestLoader::from_path(&manifest_path)?;
        metadata_backend.create_with_manifest(manifest)?
    } else {
        metadata_backend.create()?
    };

    // Create cluster service
    let broker_id = BrokerId(args.broker_id);
    let cluster_service = Arc::new(ClusterServiceImpl::new(metadata_store, broker_id));

    tracing::info!(%addr, broker_id = %args.broker_id, "Starting FlashQ gRPC server with cluster support");
    flashq_grpc::server::serve(addr, core, cluster_service).await?;
    Ok(())
}
