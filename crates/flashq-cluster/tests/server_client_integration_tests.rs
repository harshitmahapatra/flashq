//! Server and Client Integration Tests
//!
//! Integration tests for ClusterServer and ClusterClient components,
//! testing end-to-end gRPC communication and error handling.

mod test_utilities;

mod server {
    pub mod server_integration_tests;
}

mod client {
    pub mod client_integration_tests;
}
