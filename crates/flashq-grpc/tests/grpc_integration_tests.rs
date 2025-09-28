// gRPC Integration Tests
//
// Organizes all gRPC-related integration tests into a single test target.
// Individual test modules are located in the tests/grpc/ directory.

pub mod test_utilities;

mod grpc {
    pub mod admin_tests;
    pub mod consumer_tests;
    pub mod producer_tests;
    pub mod storage_integration_tests;
    pub mod subscribe_tests;
    pub mod validation_tests;
}
