// Storage Integration Tests
// Aggregates all storage-related integration tests under a single target.

mod storage {
    pub mod batching_tests;
    pub mod consumer_group_tests;
    pub mod directory_locking_tests;
    pub mod error_simulation_tests;
    pub mod file_io_integration_tests;
    pub mod file_topic_log_tests;
    pub mod persistence_tests;
    pub mod segment_manager_tests;
    pub mod segment_tests;
    pub mod storage_backend_tests;
    pub mod test_utilities;
    pub mod time_polling_tests;
}
