//! Metadata Store Module Integration Tests
//!
//! Organizes all metadata_store-related integration tests for the flashq-cluster crate into a single test target.
//! Individual test modules are located in the tests/metadata_store/ directory.

mod test_utilities;

mod metadata_store {
    pub mod file_metadata_store_tests;
}
