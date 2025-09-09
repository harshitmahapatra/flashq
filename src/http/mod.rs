//! HTTP API components for FlashQ
//!
//! This module contains all HTTP-specific functionality including:
//! - Request/response types for the REST API
//! - Validation logic for HTTP endpoints
//! - Broker implementation utilities

pub mod broker;
pub mod combined_cli;
pub mod common;
pub mod consumer;
pub mod error;
pub mod metadata;
pub mod producer;

pub use common::*;
