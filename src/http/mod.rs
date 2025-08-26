//! HTTP API components for FlashQ
//!
//! This module contains all HTTP-specific functionality including:
//! - Request/response types for the REST API
//! - Validation logic for HTTP endpoints
//! - Server implementation utilities

pub mod cli;
pub mod client;
pub mod common;
pub mod server;

pub use common::*;
