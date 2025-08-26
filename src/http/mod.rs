//! HTTP API components for FlashQ
//! 
//! This module contains all HTTP-specific functionality including:
//! - Request/response types for the REST API
//! - Validation logic for HTTP endpoints
//! - Server implementation utilities

pub mod types;
pub mod validation;
pub mod client;
pub mod server;
pub mod common;
pub mod cli;

pub use types::*;
pub use validation::*;