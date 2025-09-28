use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::sleep;

// Re-export cluster types for test convenience
pub use flashq_cluster::{
    Record, metadata_store::FileMetadataStore, service::ClusterServiceImpl, types::*,
    storage::{StorageBackend, file::SyncMode},
};
pub use flashq_grpc::server::FlashqGrpcService;

static SERVER_INIT: Once = Once::new();
static SERVER_BINARY_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

pub fn find_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

pub fn ensure_server_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(p) = option_env!("CARGO_BIN_EXE_grpc-server") {
        return Ok(PathBuf::from(p));
    }
    SERVER_INIT.call_once(|| {
        let output = Command::new("cargo")
            .args(["build", "-p", "flashq-grpc", "--bin", "grpc-server"])
            .output()
            .expect("Failed to build grpc-server binary");
        if !output.status.success() {
            panic!(
                "Failed to build grpc-server binary: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        *SERVER_BINARY_PATH.lock().unwrap() = Some(PathBuf::from("target/debug/grpc-server"));
    });
    Ok(SERVER_BINARY_PATH
        .lock()
        .unwrap()
        .as_ref()
        .expect("server path set")
        .clone())
}

pub struct TestServer {
    pub process: Child,
    pub port: u16,
    data_dir: Option<PathBuf>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

impl TestServer {
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let port = find_available_port()?;
        let bin = ensure_server_binary()?;
        let mut process = Command::new(bin)
            .args(["--port", &port.to_string(), "--storage", "memory"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for readiness by retrying Admin.Health via gRPC
        let addr = format!("http://127.0.0.1:{port}");
        for _ in 0..30 {
            if let Ok(Some(status)) = process.try_wait() {
                if let Some(mut stderr) = process.stderr.take() {
                    let mut buf = String::new();
                    let _ = stderr.read_to_string(&mut buf);
                    eprintln!("grpc-server exited early: {status}, stderr: {buf}");
                }
                return Err("grpc-server exited".into());
            }
            if let Ok(mut c) =
                flashq_grpc::flashq::v1::admin_client::AdminClient::connect(addr.clone()).await
            {
                if c.health(flashq_grpc::flashq::v1::Empty {}).await.is_ok() {
                    return Ok(Self {
                        process,
                        port,
                        data_dir: None,
                    });
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
        let _ = process.kill();
        Err("grpc-server failed to start".into())
    }

    pub async fn start_with_storage(storage: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if storage == "file" {
            let temp_dir = tempfile::Builder::new()
                .prefix("flashq_grpc_test_")
                .tempdir()?;
            Self::start_with_data_dir(temp_dir.path()).await
        } else {
            Self::start().await
        }
    }

    pub async fn start_with_data_dir(dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let port = find_available_port()?;
        let bin = ensure_server_binary()?;
        let mut process = Command::new(bin)
            .args([
                "--port",
                &port.to_string(),
                "--storage",
                "file",
                "--data-dir",
                dir.to_str().unwrap(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let addr = format!("http://127.0.0.1:{port}");
        for _ in 0..30 {
            if let Ok(Some(status)) = process.try_wait() {
                if let Some(mut stderr) = process.stderr.take() {
                    let mut buf = String::new();
                    let _ = stderr.read_to_string(&mut buf);
                    eprintln!("grpc-server exited early: {status}, stderr: {buf}");
                }
                return Err("grpc-server exited".into());
            }
            if let Ok(mut c) =
                flashq_grpc::flashq::v1::admin_client::AdminClient::connect(addr.clone()).await
            {
                if c.health(flashq_grpc::flashq::v1::Empty {}).await.is_ok() {
                    return Ok(Self {
                        process,
                        port,
                        data_dir: Some(dir.to_path_buf()),
                    });
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
        let _ = process.kill();
        Err("grpc-server failed to start".into())
    }

    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }
}

/// Create a FlashQ broker with file storage and integrated cluster service for testing
#[allow(dead_code)]
pub fn create_test_broker_with_cluster_service(
    temp_dir: &TempDir,
    broker_id: BrokerId,
) -> (Arc<FlashqGrpcService>, Arc<ClusterServiceImpl>) {
    // Create FlashQ core with file storage backend (following server.rs pattern)
    let storage_backend = StorageBackend::new_file_with_path(
        SyncMode::None, // Use no sync for tests to avoid performance overhead
        temp_dir.path().join("flashq_data"),
    ).expect("Failed to create file storage backend");

    let core = Arc::new(flashq_cluster::FlashQ::with_storage_backend(storage_backend));

    // Create file-based metadata store for cluster
    let metadata_store = Arc::new(
        FileMetadataStore::new(temp_dir.path().join("cluster")).unwrap()
    );

    // Create FlashQ gRPC service
    let grpc_service = Arc::new(FlashqGrpcService::new(core));

    // Create cluster service with FlashQ broker integration
    let cluster_service = Arc::new(ClusterServiceImpl::with_broker(
        metadata_store,
        broker_id,
        grpc_service.clone(),
    ));

    (grpc_service, cluster_service)
}

/// Create a test topic with sample records for integration testing
#[allow(dead_code)]
pub fn create_test_topic_with_records(
    grpc_service: &FlashqGrpcService,
    topic: &str,
    record_count: usize,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let records: Vec<Record> = (0..record_count)
        .map(|i| Record::new(None, format!("test-record-{i}"), None))
        .collect();

    grpc_service
        .core
        .post_records(topic.to_string(), records.clone())?;
    Ok(records)
}
