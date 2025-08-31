# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlashQ is a Kafka-inspired record queue implementation with HTTP API endpoints, segment-based file storage backend, comprehensive error handling, and production-ready features. The project includes enhanced record structure with keys, headers, and offsets, consumer groups, Kafka-aligned segment architecture, and full integration test coverage.

## Development Commands

### Building and Running
- `cargo build` - Build the project (debug mode)
- `cargo run --bin flashq` - Build and run the interactive demo
- `cargo run --bin server` - Build and run the HTTP server (in-memory storage)
- `cargo run --bin server -- --storage=file --data-dir=./data` - Run server with file storage
- `cargo run --bin client` - Build and run the CLI client
- `cargo build --release` - Build optimized release version

### Production Binary Building
- `cargo build --release --bin server` - Build optimized server binary
- `cargo build --release --bin client` - Build optimized client binary
- `cargo build --release` - Build all optimized binaries
- Binaries located in `target/release/` directory

### Testing and Quality
- `cargo test` - Run all tests (unit + integration)
- `cargo test --test '*'` - Run only integration tests
- `cargo test <test_name>` - Run a specific test
- `cargo clippy` - Run Rust linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo check` - Quick compile check without generating binaries

### Benchmarking
```bash
cargo bench                              # Run all benchmarks
cargo bench --bench memory_storage       # Memory storage benchmarks only
cargo bench --bench file_storage         # File storage benchmarks only
```


### OpenAPI Specification Validation
- `openapi-generator validate -i docs/openapi.yaml` - Validate OpenAPI specification syntax and structure

### Dependencies
- `cargo add <crate_name>` - Add a new dependency
- `cargo update` - Update dependencies to latest compatible versions

## Project Structure

Following Rust best practices with library and binary crates:
- `src/lib.rs` - Library crate containing core record queue functionality
- `src/main.rs` - Lightweight binary entry point (delegates to demo module)
- `src/demo.rs` - Interactive demo module with CLI functionality  
- `src/bin/server.rs` - HTTP server implementation with REST API
- `src/bin/client.rs` - CLI client for interacting with the server
- `src/error.rs` - Comprehensive error handling with structured error types
- `src/storage/` - Storage backend implementations (memory and file-based)
- `tests/` - Comprehensive test suite organized by component (HTTP and storage)
- `Cargo.toml` - Project configuration using Rust 2024 edition

### Library Crate (`src/lib.rs`)
- `Record` struct - Record payload with optional key and headers
- `RecordWithOffset` struct - Record with offset and ISO 8601 timestamp
- `FlashQ` struct - Main queue implementation with pluggable storage backends
- Storage backend abstraction with memory and file implementations
- Consumer group management with persistent offset tracking
- `demo` module - Interactive CLI functionality (exposed publicly)
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- Comprehensive unit tests for all data structures

### Storage Module (`src/storage/`)
- `StorageBackend` enum - Pluggable storage backend selection (memory/file)
- `TopicLog` and `ConsumerGroup` traits - Storage abstraction layer
- `FileTopicLog` - Kafka-aligned segment-based file storage with crash recovery
- `SegmentManager` - Manages log segment lifecycle and rolling
- `LogSegment` - Individual segment files with sparse indexing
- `SparseIndex` - Efficient offset-to-position mapping within segments
- `FileConsumerGroup` - Persistent consumer group offset management
- Directory locking mechanism to prevent concurrent access
- Comprehensive error handling and recovery capabilities

### Binary Crates
- **`src/main.rs`** - Lightweight entry point (2 lines) following Rust best practices
- **`src/demo.rs`** - Interactive CLI demo module with user-friendly menu system
- **`src/bin/server.rs`** - HTTP REST API server with endpoints for posting/polling records
- **`src/bin/client.rs`** - Command-line client for interacting with the HTTP server

### Interactive Demo (`src/demo.rs`)
The demo module provides an educational interactive demonstration of the record queue library:

**Features:**
- **Menu-driven interface** - 5 clear options with emoji-based visual feedback
- **Post records interactively** - Prompts for topic and record value with input validation
- **Enhanced record display** - Shows keys, headers, offsets, and ISO 8601 timestamps
- **Poll records with options** - Choose topic and optionally limit record count
- **Topic management** - View all created topics with record counts from current session
- **Quick demo mode** - Automated demonstration posting and polling multiple records
- **Error handling** - Graceful input validation and user-friendly error messages

**Usage:**
```bash
# Run the interactive demo
cargo run --bin flashq

# Or after building
./target/debug/flashq
```

**Menu Options:**
1. Post a record - Interactive topic and record value input (creates Record)
2. Poll records from a topic - Display enhanced records with keys, headers, offsets
3. View all topics - Show session statistics and topic overview
4. Run quick demo - Automated demonstration of core functionality
5. Exit - Clean program termination

This provides an excellent way to understand the library API and test functionality without requiring HTTP server setup or external clients.

### Test Suite (`tests/`)
**HTTP Integration Tests (`tests/http/`):**
- End-to-end workflow testing with multi-topic scenarios
- Consumer group operations and offset management
- FIFO ordering verification and replay functionality
- Record size validation and error handling
- Health check and OpenAPI compliance testing

**Storage Integration Tests (`tests/storage/`):**
- Segment-based file storage testing with crash recovery
- Directory locking and concurrent access prevention  
- Error simulation (disk full, permission errors)
- Consumer group persistence across restarts
- Segment architecture validation and sparse indexing

## Architecture Notes

Current implementation features:
- **Kafka-style messaging**: Records with optional keys and headers for routing/metadata
- **Topic-based organization**: Records organized by topic strings with separate offset counters
- **Pluggable storage**: In-memory and file-based storage backends
- **File storage**: Kafka-aligned segment-based architecture with rolling segments and sparse indexing
- **Directory locking**: Prevents concurrent access to file storage directories
- **Error handling**: Comprehensive error types with structured logging
- **Crash recovery**: File storage recovers state from segment files with automatic discovery
- **Offset-based positioning**: Sequential offsets within topics starting from 0
- **Non-destructive polling**: Records remain in queue after being read
- **FIFO ordering**: Records returned in the order they were posted with offset guarantees
- **Thread safety**: Safe concurrent access using Arc<Mutex<>>
- **ISO 8601 timestamps**: Human-readable timestamp format for record creation time
- **Consumer groups**: Persistent consumer group offset management
- **Replay functionality**: Seek to specific offsets with `from_offset` parameter
- **Record size validation**: OpenAPI-compliant validation (key: 1024 chars, value: 1MB, headers: 1024 chars each)
- **HTTP REST API**: Full REST endpoints for posting, polling, and consumer group operations
- **JSON serialization**: All data structures support serde for API communication
- **Comprehensive testing**: Unit tests for core logic + integration tests for HTTP API and storage

## Performance Characteristics

**Memory Storage (Fast, Volatile):**
- Throughput: 10K-276K records/sec
- Latency: 1.8-10ms
- Best for: Real-time processing, temporary queues

**File Storage (Persistent, Segment-Based):**
- Throughput: 856-17.5K records/sec  
- Latency: 28.6-116.8ms
- Best for: Durable messaging, audit logs
- Architecture: Kafka-aligned segments with sparse indexing

Memory storage provides 12-310x performance advantage over file storage, while file storage offers full persistence and crash recovery capabilities.

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[API Reference](docs/api.md)** - Complete HTTP REST API documentation with examples
- **[Architecture](docs/architecture.md)** - System design and data structure details  
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines
- **[Performance](docs/performance.md)** - Benchmarking results and storage backend comparison

## Production Deployment

### Running Production Binaries

After building with `cargo build --release`:

**Server:**
```bash
# In-memory storage (default)
./target/release/server                    # Port 8080
./target/release/server 9090              # Custom port

# Segment-based file storage backend
./target/release/server -- --storage=file --data-dir=./data
./target/release/server 9090 -- --storage=file --data-dir=./data
```

**Logging Behavior:**
- **Debug builds** (`cargo run --bin server`): TRACE-level logging (verbose request/response details)
- **Release builds** (`./target/release/server`): INFO-level logging (production-appropriate output)
- Automatic detection based on compilation flags - no manual configuration needed

**Client:**
```bash
# Post record
./target/release/client post news "Production record"

# Poll records
./target/release/client poll news

# Custom server port
./target/release/client --port=9090 post news "Custom port record"
```

### Binary Installation

**System-wide installation:**
```bash
# Install to ~/.cargo/bin (ensure it's in PATH)
cargo install --path . --bin server
cargo install --path . --bin client

# Run from anywhere
server 8080
client post news "Installed binary record"
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
- **Commit Messages**: Do not include Claude, AI, or automated generation references in commit messages. Write natural, human-style commit messages focused on the technical changes. Keep messages concise (1-2 lines preferred) and use bullet points only for multiple distinct changes. Avoid verbose explanations.
- **GitHub Communication**: Keep all GitHub PR comments, issue comments, and descriptions concise and focused. Avoid verbose explanations - aim for clarity and brevity.
- **TODO(human) sections**: NEVER implement or fill in TODO(human) sections - these are specifically for human collaboration. Wait for the human to implement these sections before proceeding.
- **File creation**: NEVER create files unless they're absolutely necessary for achieving your goal.
- **File editing**: ALWAYS prefer editing an existing file to creating a new one.
- **Documentation**: NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.