use flashq::storage::file::SyncMode;
use std::path::PathBuf;
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

/// Create a unique test directory path under target/test_data
/// Caller is responsible for creating the directory.
pub fn unique_test_dir(name: &str) -> PathBuf {
    let mut dir = PathBuf::from("target/test_data");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("{name}_{nanos}"));
    dir
}

/// Build a large string payload of given length
pub fn big_val(len: usize) -> String {
    std::iter::repeat_n('x', len).collect()
}

/// Common test configuration
pub struct TestConfig {
    pub temp_dir: tempfile::TempDir,
    pub topic_name: String,
    pub sync_mode: SyncMode,
    pub segment_size: u64,
}

impl TestConfig {
    pub fn new(prefix: &str) -> Self {
        Self {
            temp_dir: create_test_dir(prefix),
            topic_name: create_test_topic(prefix),
            sync_mode: SyncMode::Immediate,
            segment_size: 1024 * 1024, // 1MB for tests
        }
    }

    /// Get the path to the temporary directory
    pub fn temp_dir_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }
}

// TempDir automatically cleans up on drop, no manual cleanup needed
