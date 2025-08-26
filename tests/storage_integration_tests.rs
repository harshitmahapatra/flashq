// Storage Integration Tests
//
// This module organizes all storage-related integration tests into a single test target.
// Individual test modules are located in the tests/storage/ directory.

mod storage {
    pub mod file_topic_log_tests;
    pub mod persistence_tests;
    pub mod storage_backend_tests;
}
