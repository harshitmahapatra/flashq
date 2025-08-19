# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust-based message queue implementation with HTTP API endpoints, comprehensive testing, and production-ready features. The project includes both library and binary crates with full integration test coverage.

## Development Commands

### Building and Running
- `cargo build` - Build the project
- `cargo run` - Build and run the application
- `cargo build --release` - Build optimized release version

### Testing and Quality
- `cargo test` - Run all tests (unit + integration)
- `cargo test --test integration_tests` - Run only integration tests
- `cargo test <test_name>` - Run a specific test
- `cargo clippy` - Run Rust linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo check` - Quick compile check without generating binaries

### Dependencies
- `cargo add <crate_name>` - Add a new dependency
- `cargo update` - Update dependencies to latest compatible versions

## Project Structure

Following Rust best practices with library and binary crates:
- `src/lib.rs` - Library crate containing core message queue functionality
- `src/main.rs` - Binary crate with application entry point  
- `src/bin/server.rs` - HTTP server implementation with REST API
- `src/bin/client.rs` - CLI client for interacting with the server
- `tests/integration_tests.rs` - Comprehensive integration test suite
- `Cargo.toml` - Project configuration using Rust 2024 edition

### Library Crate (`src/lib.rs`)
- `Message` struct - Individual message with content, timestamp, and unique ID
- `MessageQueue` struct - Main queue implementation with topic-based organization
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### Binary Crates
- **`src/main.rs`** - Simple CLI demonstration of library functionality
- **`src/bin/server.rs`** - HTTP REST API server with endpoints for posting/polling messages
- **`src/bin/client.rs`** - Command-line client for interacting with the HTTP server

### Integration Tests (`tests/integration_tests.rs`)
- **End-to-end workflow testing** - Multi-topic message posting and polling
- **HTTP API validation** - All REST endpoints tested with real server instances
- **FIFO ordering verification** - Ensures message ordering guarantees
- **Count parameter testing** - Validates polling limits work correctly
- **Error handling** - Tests invalid requests and edge cases
- **Health check testing** - Server status endpoint validation

## Architecture Notes

Current implementation features:
- **Topic-based messaging**: Messages are organized by topic strings
- **Non-destructive polling**: Messages remain in queue after being read
- **FIFO ordering**: Messages are returned in the order they were posted  
- **Thread safety**: Safe concurrent access using Arc<Mutex<>>
- **Unique message IDs**: Each message gets an incrementing ID
- **Count limiting**: Poll operations can limit number of messages returned
- **HTTP REST API**: Full REST endpoints for posting and polling messages
- **JSON serialization**: All data structures support serde for API communication
- **Comprehensive testing**: Unit tests for core logic + integration tests for HTTP API
- **Production ready**: Error handling, health checks, and proper HTTP status codes