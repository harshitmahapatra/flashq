# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlashQ is a Kafka-inspired record queue implementation with gRPC API endpoints, segment-based file storage backend, configurable batching for high-throughput processing, comprehensive error handling, and production-ready features. The project is organized as a Cargo workspace with four crates:

- **`flashq-storage`** - Storage backend implementations (memory and file-based) with Record types
- **`flashq`** - Core queue library with topic and consumer group management
- **`flashq-grpc`** - gRPC broker, producer, consumer, and client implementations
- **`flashq-cluster`** - Cluster coordination and metadata management

The project includes enhanced record structure with keys, headers, and offsets, snapshot-based consumer offset storage, Kafka-aligned segment architecture, cluster coordination protocols, and full integration test coverage.

## Development Commands

### Building and Running
- `cargo build` - Build the project workspace (debug mode)
- `cargo run -p flashq --bin flashq` - Build and run the interactive demo
- `cargo run -p flashq-grpc --bin grpc-server` - Build and run the gRPC server (in-memory storage)
- `cargo run -p flashq-grpc --bin grpc-server -- --storage=file --data-dir=./data` - Run gRPC server with file storage
- `cargo run -p flashq-grpc --bin grpc-client` - Build and run the gRPC CLI client
- `cargo build --release` - Build optimized release version

### Production Binary Building
- `cargo build --release -p flashq-grpc --bin grpc-server` - Build optimized gRPC server binary
- `cargo build --release -p flashq-grpc --bin grpc-client` - Build optimized gRPC client binary
- `cargo build --release` - Build all optimized binaries
- Binaries located in `target/release/` directory

### Testing and Quality
- `cargo test` - Run all tests (workspace: unit + integration)
- `cargo test --test '*'` - Run only integration tests
- `cargo test -p flashq-storage --test storage_integration_tests` - Run storage tests
- `cargo test -p flashq-grpc --test grpc_integration_tests` - Run gRPC tests
- `cargo test -p flashq-cluster --test '*'` - Run cluster coordination tests
- `cargo test <test_name>` - Run a specific test
- `cargo clippy` - Run Rust linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo check` - Quick compile check without generating binaries

### Benchmarking
```bash
cargo bench                              # Run all benchmarks
cargo bench -p flashq-storage --bench memory_storage       # Memory storage benchmarks only
cargo bench -p flashq-storage --bench file_storage_std     # Standard file I/O benchmarks
cargo bench -p flashq-storage --bench batching_baseline    # Batching performance benchmarks
```


### Dependencies
- `cargo add <crate_name>` - Add a new dependency
- `cargo update` - Update dependencies to latest compatible versions

## Project Structure

Following Rust best practices with a workspace containing library and binary crates:

### Workspace Structure
- `Cargo.toml` - Workspace configuration using Rust 2024 edition
- `crates/flashq-storage/` - Storage backend implementations library crate
- `crates/flashq/` - Core queue library crate
- `crates/flashq-grpc/` - gRPC broker, producer, consumer, and client crate
- `crates/flashq-cluster/` - Cluster coordination and metadata management crate

### Storage Crate (`crates/flashq-storage/`)
- `src/lib.rs` - Storage library with Record types and backend exports
- `src/error.rs` - Storage-specific error types
- `src/storage/` - Storage backend implementations (memory and file-based)
- `tests/storage/` - Storage integration test suite
- `benches/` - Performance benchmarks for storage backends

### Core Library Crate (`crates/flashq/`)
- `src/lib.rs` - Core FlashQ queue implementation
- `src/main.rs` - Lightweight binary entry point (delegates to demo module)
- `src/demo.rs` - Interactive demo module with CLI functionality
- `src/error.rs` - FlashQ-specific error types

### gRPC Crate (`crates/flashq-grpc/`)
- `Cargo.toml` - gRPC dependencies (tonic, prost, protoc-bin-vendored)
- `build.rs` - Protocol Buffer code generation with vendored protoc
- `proto/flashq.proto` - Protocol Buffer schema definition
- `src/lib.rs` - gRPC library exports and generated code
- `src/bin/server.rs` - gRPC server implementation with cluster integration
- `src/bin/client.rs` - CLI client for gRPC operations
- `src/server.rs` - Producer/Consumer/Admin service implementations
- `src/client.rs` - gRPC client connection utilities
- `tests/grpc/` - gRPC integration test suite
- `tests/cluster/` - Cluster coordination integration tests

### Cluster Crate (`crates/flashq-cluster/`)
- `Cargo.toml` - Cluster dependencies (tonic, serde, serde_yaml)
- `build.rs` - Protocol Buffer code generation for cluster protocol
- `proto/cluster.proto` - Cluster coordination protocol definition
- `src/lib.rs` - Cluster library exports and generated code
- `src/service.rs` - ClusterServiceImpl implementation
- `src/server.rs` - ClusterServer gRPC adapter
- `src/client.rs` - ClusterClient gRPC adapter
- `src/traits.rs` - ClusterService and ClusterBroker trait definitions
- `src/metadata_store/` - Metadata storage implementations (memory/file)
- `src/manifest/` - Manifest loading from YAML/JSON
- `tests/` - Comprehensive cluster coordination test suite
- `README.md` - Detailed cluster architecture documentation

### Storage Components (`flashq-storage`)
- `Record` struct - Record payload with optional key and headers
- `RecordWithOffset` struct - Record with offset and ISO 8601 timestamp
- `StorageBackend` enum - Pluggable storage backend selection (memory/file)
- `TopicLog` and `ConsumerOffsetStore` traits - Storage abstraction layer with batched operations
- `batching_heuristics` - Shared utilities for record size estimation and batch optimization
- `FileTopicLog` - Kafka-aligned segment-based file storage with crash recovery
- `SegmentManager` - Manages log segment lifecycle and rolling
- `LogSegment` - Individual segment files with sparse indexing and pluggable I/O
- `SparseIndex` - Efficient offset-to-position mapping within segments
- `FileIO` trait - Abstraction for file I/O operations (standard vs io_uring)
- `StdFileIO` - Standard file I/O implementation using std::fs
- `IoUringFileIO` - Linux io_uring implementation (experimental, currently slower)
- `FileConsumerOffsetStore` - Snapshot-based consumer offset storage with monotonic updates
- `InMemoryConsumerOffsetStore` - In-memory offset snapshot storage
- Directory locking mechanism to prevent concurrent access
- Comprehensive error handling and recovery capabilities

### Core Queue Components (`flashq`)
- `FlashQ` struct - Main queue implementation with pluggable storage backends
- Consumer group management with persistent offset tracking
- `demo` module - Interactive CLI functionality (exposed publicly)
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- Re-exports storage types for backward compatibility

### Binary Crates
- **`crates/flashq/src/main.rs`** - Lightweight entry point (2 lines) following Rust best practices
- **`crates/flashq/src/demo.rs`** - Interactive CLI demo module with user-friendly menu system
- **`crates/flashq-grpc/src/bin/server.rs`** - gRPC server with Producer/Consumer/Admin services
- **`crates/flashq-grpc/src/bin/client.rs`** - Command-line client for interacting with the gRPC server

### Interactive Demo (`crates/flashq/src/demo.rs`)
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
cargo run -p flashq --bin flashq

# Or after building
./target/debug/flashq
```

**Menu Options:**
1. Post a record - Interactive topic and record value input (creates Record)
2. Poll records from a topic - Display enhanced records with keys, headers, offsets
3. View all topics - Show session statistics and topic overview
4. Run quick demo - Automated demonstration of core functionality
5. Exit - Clean program termination

This provides an excellent way to understand the library API and test functionality without requiring gRPC server setup or external clients.

### Test Suite
**gRPC Integration Tests (`crates/flashq-grpc/tests/grpc/`):**
- End-to-end workflow testing with multi-topic scenarios
- Producer/Consumer/Admin service operations
- Streaming subscription functionality
- Consumer group operations and offset management
- FIFO ordering verification and replay functionality
- Record size validation and error handling

**Storage Integration Tests (`crates/flashq-storage/tests/storage/`):**
- Segment-based file storage testing with crash recovery
- Directory locking and concurrent access prevention
- Error simulation (disk full, permission errors)
- Consumer offset store testing with monotonic enforcement
- Segment architecture validation and sparse indexing
- Batching operations testing with performance validation

**Cluster Integration Tests (`crates/flashq-cluster/tests/`):**
- ClusterService implementation testing
- Metadata store operations (memory and file backends)
- Manifest loading and cluster initialization
- Heartbeat protocol and epoch tracking
- ClusterServer/ClusterClient communication
- Broker coordination scenarios

## Architecture Notes

Current implementation features:
- **Configurable batching**: High-throughput batch operations with configurable batch_bytes (4-44x performance improvement)
- **Kafka-style messaging**: Records with optional keys and headers for routing/metadata
- **Topic-based organization**: Records organized by topic strings with separate offset counters
- **Pluggable storage**: In-memory and file-based storage backends with batched operations
- **File storage**: Kafka-aligned segment-based architecture with rolling segments and sparse indexing
- **Snapshot-based offsets**: ConsumerOffsetStore trait with monotonic updates (Phase 3)
- **Cluster coordination**: Metadata management, heartbeat protocol, and leader election
- **Directory locking**: Prevents concurrent access to file storage directories
- **Error handling**: Comprehensive error types with structured logging
- **Crash recovery**: File storage recovers state from segment files with automatic discovery
- **Offset-based positioning**: Sequential offsets within topics starting from 0
- **Non-destructive polling**: Records remain in queue after being read
- **FIFO ordering**: Records returned in the order they were posted with offset guarantees
- **Thread safety**: Safe concurrent access using DashMap and parking_lot RwLock
- **ISO 8601 timestamps**: Human-readable timestamp format for record creation time
- **Consumer groups**: Persistent consumer group offset management with monotonic enforcement
- **Replay functionality**: Seek to specific offsets with `from_offset` parameter
- **Record size validation**: (key: 1024 chars, value: 1MB, headers: 1024 chars each)
- **gRPC API**: Protocol Buffer-based API with streaming subscriptions
- **Cluster gRPC protocol**: Bidirectional heartbeat streaming and metadata queries
- **Manifest-based bootstrap**: Initialize cluster state from YAML/JSON configuration
- **JSON serialization**: All data structures support serde for API communication
- **Comprehensive testing**: Unit tests for core logic + integration tests for gRPC/cluster coordination

## Performance Characteristics

**Memory Storage - Batched (Recommended for High Volume):**
- Throughput: 144K-2.1M records/sec (batch operations)
- Single-record: 89.2K-471K records/sec
- Latency: 475µs-11.2ms
- Best for: High-throughput processing, bulk operations

**File Storage - Batched (Recommended for Persistent High Volume):**
- Throughput: 48.1K-374K records/sec (batch operations)
- Single-record: 11.0K-44.6K records/sec
- Latency: 2.67ms-90.7ms
- Best for: Persistent high-volume messaging, audit logs
- Architecture: Kafka-aligned segments with batched writes

**Performance Improvements:**
- Batching provides 4-44x performance improvement over single record operations
- Memory storage provides 8-43x performance advantage over file storage
- Configurable batch_bytes allows tuning for optimal throughput vs memory usage

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[API Reference](docs/api.md)** - Complete gRPC API documentation with examples
- **[Architecture](docs/architecture.md)** - System design and data structure details
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines
- **[Performance](docs/performance.md)** - Benchmarking results and storage backend comparison

## Production Deployment

### Running Production Binaries

After building with `cargo build --release`:

**gRPC Server:**
```bash
# In-memory storage (default)
./target/release/grpc-server                    # Port 50051
./target/release/grpc-server -- --port=50052    # Custom port

# Segment-based file storage backend
./target/release/grpc-server -- --storage=file --data-dir=./data

# Performance tuning with batch size configuration
./target/release/grpc-server -- --batch-bytes=65536                 # 64KB batches
./target/release/grpc-server -- --storage=file --batch-bytes=131072 # 128KB batches
```

**Logging Behavior:**
- **Debug builds** (`cargo run -p flashq-grpc --bin grpc-server`): TRACE-level logging (verbose request/response details)
- **Release builds** (`./target/release/grpc-server`): INFO-level logging (production-appropriate output)
- Automatic detection based on compilation flags - no manual configuration needed

**gRPC Client:**
```bash
# Produce records
./target/release/grpc-client -- produce --topic=news --value="Production record"

# Fetch records
./target/release/grpc-client -- fetch-offset --group-id=my-group --topic=news

# Subscribe to stream
./target/release/grpc-client -- subscribe --group-id=my-group --topic=news
```

### Binary Installation

**System-wide installation:**
```bash
# Install to ~/.cargo/bin (ensure it's in PATH)
cargo install --path crates/flashq --bin flashq          # Interactive demo
cargo install --path crates/flashq-broker --bin grpc-server # gRPC server
cargo install --path crates/flashq-broker --bin grpc-client # gRPC client

# Run from anywhere
flashq                                    # Interactive demo
grpc-server                              # gRPC server
grpc-client -- produce --topic=news --value="Hello"   # gRPC client
```

**Manual deployment:**
```bash
# Copy binaries to deployment location
cp target/release/flashq /usr/local/bin/
cp target/release/grpc-server /usr/local/bin/
cp target/release/grpc-client /usr/local/bin/
chmod +x /usr/local/bin/flashq
chmod +x /usr/local/bin/grpc-server
chmod +x /usr/local/bin/grpc-client
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
- **Code Simplicity**: ALWAYS prefer smaller functions with descriptive names and less comments over long functions. Break down longs functions using private functions with descriptive names.
- **Test Simplicity**: ALWAYS prefer tests with the following structure 1. Setup 2. Action 3. Expectation.
- **Test Atomicity**: ALWAYS prefer a test testing only one logic. If more than one logic is tested in a tested in a test, split it into multiple tests testing one logic.
- **Test Uniqueness**: NEVER allow two tests to test the same logic.