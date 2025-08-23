# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust-based message queue implementation with HTTP API endpoints, comprehensive testing, and production-ready features. The project includes both library and binary crates with full integration test coverage.

## Development Commands

### Building and Running
- `cargo build` - Build the project (debug mode)
- `cargo run --bin message-queue-rs` - Build and run the interactive demo
- `cargo run --bin server` - Build and run the HTTP server
- `cargo run --bin client` - Build and run the CLI client
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
- `src/main.rs` - Lightweight binary entry point (delegates to demo module)
- `src/demo.rs` - Interactive demo module with CLI functionality  
- `src/bin/server.rs` - HTTP server implementation with REST API
- `src/bin/client.rs` - CLI client for interacting with the server
- `tests/integration_tests.rs` - Comprehensive integration test suite
- `Cargo.toml` - Project configuration using Rust 2024 edition

### Library Crate (`src/lib.rs`)
- `Message` struct - Individual message with content, timestamp, and unique ID
- `MessageQueue` struct - Main queue implementation with topic-based organization
- `demo` module - Interactive CLI functionality (exposed publicly)
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### Binary Crates
- **`src/main.rs`** - Lightweight entry point (2 lines) following Rust best practices
- **`src/demo.rs`** - Interactive CLI demo module with user-friendly menu system
- **`src/bin/server.rs`** - HTTP REST API server with endpoints for posting/polling messages
- **`src/bin/client.rs`** - Command-line client for interacting with the HTTP server

### Interactive Demo (`src/demo.rs`)
The demo module provides an educational interactive demonstration of the message queue library:

**Features:**
- **Menu-driven interface** - 5 clear options with emoji-based visual feedback
- **Post messages interactively** - Prompts for topic and content with input validation
- **Poll messages with options** - Choose topic and optionally limit message count
- **Topic management** - View all created topics with message counts from current session
- **Quick demo mode** - Automated demonstration posting and polling multiple messages
- **Error handling** - Graceful input validation and user-friendly error messages

**Usage:**
```bash
# Run the interactive demo
cargo run --bin message-queue-rs

# Or after building
./target/debug/message-queue-rs
```

**Menu Options:**
1. Post a message - Interactive topic and content input
2. Poll messages from a topic - Choose topic and message count limit
3. View all topics - Show session statistics and topic overview
4. Run quick demo - Automated demonstration of core functionality
5. Exit - Clean program termination

This provides an excellent way to understand the library API and test functionality without requiring HTTP server setup or external clients.

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
- **Consumer groups**: Kafka-style consumer group offset management for coordinated consumption

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[API Reference](docs/api.md)** - Complete HTTP REST API documentation with examples
- **[Architecture](docs/architecture.md)** - System design and data structure details  
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines

## Production Deployment

### Running Production Binaries

After building with `cargo build --release`:

**Server:**
```bash
# Default port (8080) with INFO-level logging
./target/release/server

# Custom port with INFO-level logging
./target/release/server 9090
```

**Logging Behavior:**
- **Debug builds** (`cargo run --bin server`): TRACE-level logging (verbose request/response details)
- **Release builds** (`./target/release/server`): INFO-level logging (production-appropriate output)
- Automatic detection based on compilation flags - no manual configuration needed

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

## MCP Servers

The following MCP (Model Context Protocol) servers are currently configured and active:

### serena
- **Purpose**: Professional coding agent with semantic coding tools
- **Capabilities**: 
  - Symbol-based code analysis and editing
  - Intelligent code exploration with minimal token usage
  - Memory management for codebase information
  - Project onboarding and context management
- **Key Tools**: 
  - `find_symbol`, `get_symbols_overview` - Code structure analysis
  - `search_for_pattern` - Pattern matching across codebase
  - `replace_symbol_body`, `insert_after_symbol` - Precise code editing
  - Memory management tools for project context

### ide
- **Purpose**: VS Code integration for diagnostics and code execution
- **Capabilities**:
  - Language server diagnostics integration
  - Jupyter kernel code execution support
- **Key Tools**:
  - `getDiagnostics` - Retrieve VS Code language diagnostics
  - `executeCode` - Execute Python code in Jupyter kernel

## Important Instructions for Claude Code

- **Test Driven Development**: Prefer TDD when implementing changes and features. Write tests first, then implement the code to make them pass.
- **Human Implementation Preference**: After writing tests and providing function skeletons, prefer to let the human attempt the actual implementation rather than completing it automatically.
- **Commit Messages**: Do not include Claude, AI, or automated generation references in commit messages. Write natural, human-style commit messages focused on the technical changes. Keep messages concise and use bullet points for multiple changes.
- **GitHub Communication**: Keep all GitHub PR comments, issue comments, and descriptions concise and focused. Avoid verbose explanations - aim for clarity and brevity.
- **TODO(human) sections**: NEVER implement or fill in TODO(human) sections - these are specifically for human collaboration. Wait for the human to implement these sections before proceeding.
- **File creation**: NEVER create files unless they're absolutely necessary for achieving your goal.
- **File editing**: ALWAYS prefer editing an existing file to creating a new one.
- **Documentation**: NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.