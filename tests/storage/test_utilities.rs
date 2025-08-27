use flashq::storage::file::SyncMode;
use std::path::PathBuf;
use uuid::Uuid;

/// Generate a unique test ID for isolating test data
pub fn generate_test_id() -> String {
    Uuid::new_v4().to_string().replace('-', "")
}

/// Create a temporary directory for testing
pub fn create_test_dir(prefix: &str) -> PathBuf {
    let test_id = generate_test_id();
    std::env::temp_dir().join(format!("flashq_{prefix}_{test_id}"))
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

/// Clean up test directory
pub fn cleanup_test_dir(dir: &PathBuf) {
    std::fs::remove_dir_all(dir).ok();
}

/// Common test configuration
pub struct TestConfig {
    pub temp_dir: PathBuf,
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
}

impl Drop for TestConfig {
    fn drop(&mut self) {
        cleanup_test_dir(&self.temp_dir);
    }
}
