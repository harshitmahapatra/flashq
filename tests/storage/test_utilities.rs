use flashq::{Record, storage::file::SyncMode};
use std::os::unix::fs::PermissionsExt;
use uuid::Uuid;

/// Generate a unique test ID for isolating test data
pub fn generate_test_id() -> String {
    Uuid::new_v4().to_string().replace('-', "")
}

/// Create a temporary directory for testing using tempdir()
pub fn create_test_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("flashq_{prefix}_"))
        .tempdir()
        .expect("Failed to create temporary directory")
}

/// Create a unique topic name for testing
pub fn create_test_topic(prefix: &str) -> String {
    let test_id = generate_test_id();
    format!("{prefix}_topic_{test_id}")
}

/// Create a unique consumer group ID for testing
pub fn create_test_consumer_group(prefix: &str) -> String {
    let test_id = generate_test_id();
    format!("{prefix}_group_{test_id}")
}

pub fn test_record(key: &str, value: &str) -> Record {
    Record::new(Some(key.to_string()), value.to_string(), None)
}

pub fn make_file_readonly(file_path: &std::path::Path) -> std::io::Result<()> {
    let metadata = std::fs::metadata(file_path)?;
    let mut perms = metadata.permissions();
    perms.set_mode(0o444); // Read-only for owner, group, and others
    std::fs::set_permissions(file_path, perms)
}

pub fn make_file_writable(file_path: &std::path::Path) -> std::io::Result<()> {
    let metadata = std::fs::metadata(file_path)?;
    let mut perms = metadata.permissions();
    perms.set_mode(0o644); // Read-write for owner, read-only for group and others
    std::fs::set_permissions(file_path, perms)
}

/// Common test configuration
pub struct TestConfig {
    pub temp_dir: tempfile::TempDir,
    pub topic_name: String,
    pub sync_mode: SyncMode,
}

impl TestConfig {
    pub fn new(prefix: &str) -> Self {
        Self {
            temp_dir: create_test_dir(prefix),
            topic_name: create_test_topic(prefix),
            sync_mode: SyncMode::Immediate,
        }
    }

    /// Get the path to the temporary directory
    pub fn temp_dir_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }
}

// TempDir automatically cleans up on drop, no manual cleanup needed
