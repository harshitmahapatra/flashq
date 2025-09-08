// HTTP Integration Tests
//
// This module organizes all HTTP-related integration tests into a single test target.
// Individual test modules are located in the tests/http/ directory.

mod http {
    // Shared test infrastructure
    pub mod test_utilities;

    // Core focused test modules (single responsibility)
    pub mod client_tests;
    pub mod consumer_group_tests;
    pub mod consumer_polling_tests;
    pub mod health_tests;
    pub mod offset_tests;
    pub mod producer_tests;
    pub mod server_openapi_compliance_tests;
    pub mod server_tests;
    pub mod storage_integration_tests;
    pub mod time_polling_tests;
}
