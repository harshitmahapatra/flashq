// HTTP Integration Tests
// 
// This module organizes all HTTP-related integration tests into a single test target.
// Individual test modules are located in the tests/http/ directory.

mod http {
    pub mod http_test_helpers;
    pub mod server_tests;
    pub mod client_tests;
    pub mod consumer_tests;
    pub mod producer_tests;
    pub mod server_openapi_compliance_tests;
}