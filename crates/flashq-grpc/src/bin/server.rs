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

    /// Cluster controller endpoint (for follower brokers)
    #[arg(long)]
    cluster_controller: Option<String>,

    /// Connection timeout for cluster client in seconds
    #[arg(long, default_value_t = 10)]
    cluster_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;

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

    let broker_id = BrokerId(args.broker_id);

    // Create FlashQBroker implementation from the gRPC service
    let flashq_service = Arc::new(flashq_grpc::server::FlashqGrpcService::new(core.clone()));

    // Create cluster service with optional cluster client
    let cluster_service = if let Some(controller_endpoint) = args.cluster_controller {
        tracing::info!(%controller_endpoint, "Connecting to cluster controller");

        // Create cluster client with timeout
        let timeout = Duration::from_secs(args.cluster_timeout);
        let cluster_client = flashq_cluster::client::ClusterClient::connect_with_timeout(
            controller_endpoint,
            timeout,
        )
        .await?;

        tracing::info!("Successfully connected to cluster controller");
        let service = Arc::new(ClusterServiceImpl::with_client_and_broker(
            metadata_store,
            cluster_client,
            broker_id,
            flashq_service.clone(),
        ));

        // Start follower heartbeat task
        if let Err(e) = service.start_follower_heartbeat_task().await {
            tracing::error!("Failed to start follower heartbeat task: {}", e);
            return Err(e.into());
        }

        service
    } else {
        tracing::info!("Running in standalone mode (no cluster controller)");
        Arc::new(ClusterServiceImpl::with_broker(
            metadata_store,
            broker_id,
            flashq_service.clone(),
        ))
    };

    tracing::info!(%addr, broker_id = %args.broker_id, "Starting FlashQ gRPC server with cluster support");
    flashq_grpc::server::serve(addr, core, cluster_service).await?;
    Ok(())
}
