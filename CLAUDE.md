# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust-based message queue implementation with HTTP API endpoints, comprehensive testing, and production-ready features. The project includes both library and binary crates with full integration test coverage.

## Development Commands

### Building and Running
- `cargo build` - Build the project (debug mode)
- `cargo run` - Build and run the application
- `cargo build --release` - Build optimized release version

### Production Binary Building
- `cargo build --release --bin server` - Build optimized server binary
- `cargo build --release --bin client` - Build optimized client binary
- `cargo build --release` - Build all optimized binaries
- Binaries located in `target/release/` directory

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

## Production Deployment

### Running Production Binaries

After building with `cargo build --release`:

**Server:**
```bash
# Default port (8080)
./target/release/server

# Custom port
./target/release/server 9090
```

**Client:**
```bash
# Post message
./target/release/client post news "Production message"

# Poll messages
./target/release/client poll news

# Custom server port
./target/release/client --port=9090 post news "Custom port message"
```

### Binary Installation

**System-wide installation:**
```bash
# Install to ~/.cargo/bin (ensure it's in PATH)
cargo install --path . --bin server
cargo install --path . --bin client

# Run from anywhere
server 8080
client post news "Installed binary message"
```

**Manual deployment:**
```bash
# Copy binaries to deployment location
cp target/release/server /usr/local/bin/
cp target/release/client /usr/local/bin/
chmod +x /usr/local/bin/server
chmod +x /usr/local/bin/client
```