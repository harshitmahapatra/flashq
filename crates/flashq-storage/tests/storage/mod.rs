// Storage Integration Tests
// Aggregates all storage-related integration tests under a single target.

mod batching_tests;
mod consumer_group_tests;
mod consumer_offset_store_tests;
mod directory_locking_tests;
mod error_simulation_tests;
mod file_io_integration_tests;
mod file_topic_log_tests;
mod index_tests;
mod partition_backward_compatibility_tests;
mod partition_tests;
mod persistence_tests;
mod segment_manager_tests;
mod segment_tests;
mod storage_backend_tests;
mod test_utilities;
mod time_index_tests;
mod time_polling_tests;
