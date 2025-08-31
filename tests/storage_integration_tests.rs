// Storage Integration Tests
//
// This module organizes all storage-related integration tests into logical groups.
// Each module focuses on a specific aspect of storage functionality with clear
// setup-action-expectation test structure.

mod storage {
    pub mod consumer_group_tests;
    pub mod directory_locking_tests;
    pub mod error_simulation_tests;
    pub mod file_topic_log_tests;
    pub mod persistence_tests;
    pub mod segment_manager_tests;
    pub mod segment_tests;
    pub mod storage_backend_tests;
    pub mod test_utilities;
}
