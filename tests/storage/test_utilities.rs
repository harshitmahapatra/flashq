use flashq::storage::file::SyncMode;
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
