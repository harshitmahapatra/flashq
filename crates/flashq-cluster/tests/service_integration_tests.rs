//! Service Integration Tests
//!
//! Integration tests for ClusterServiceImpl with different metadata store backends,
//! focusing on file-based persistence and service layer functionality.

mod test_utilities;

mod service {
    pub mod file_service_tests;
}
