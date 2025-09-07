# Development Guide

Development setup, testing, and contribution guidelines for FlashQ.

## Setup

```bash
git clone <repository-url> && cd flashq
cargo build && cargo test  # Verify setup
```

## Building

```bash
cargo build                    # Debug build
cargo build --release          # Release build (target/release/)
cargo build --bin server       # Specific binary
```

## Testing

### Test Types

- **Unit** (`src/lib.rs`): Core functionality and data structures
- **HTTP Integration** (`tests/http/`): REST API endpoints and client functionality
- **Storage Integration** (`tests/storage/`): File backend, persistence, and crash recovery
- **Benchmarks** (`benches/`): Performance testing with memory profiling
- **Utilities** (`tests/*/test_utilities.rs`): Shared test infrastructure

### Running Tests

```bash
cargo test                              # All tests (unit + integration)
cargo test --lib                        # Unit tests only
cargo test --test '*'                   # All integration tests
cargo test --test http_integration_tests # HTTP integration tests
cargo test --test storage_integration_tests # Storage integration tests
cargo test test_name                     # Specific test
cargo test -- --nocapture              # With output
```

### Benchmarking

```bash
cargo bench                              # Run all benchmarks
cargo bench --bench memory_storage       # Memory storage benchmarks only
cargo bench --bench file_storage_std     # File storage benchmarks only
cargo bench --bench batching_baseline    # Batching performance benchmarks
```

### Test Coverage

**HTTP Integration:** Record CRUD, FIFO ordering, consumer groups, validation, error handling  
**Storage Integration:** File persistence, crash recovery, directory locking, error simulation  
**Batching Integration:** Batch operations, performance validation, memory efficiency  
**Client Integration:** CLI commands, batch operations, consumer group lifecycle  
**Validation:** Size limits, pattern validation, HTTP status codes, OpenAPI compliance

## Code Quality

```bash
cargo clippy                    # Linting
cargo fmt                       # Format code  
cargo check                     # Fast compile check
```

## Running During Development

```bash
cargo run --bin flashq                           # Interactive demo
cargo run --bin server                           # HTTP server (in-memory, TRACE logging)
cargo run --bin server -- --storage=file         # File storage backend
cargo run --bin server -- --batch-bytes=65536    # Custom batch size (64KB)
cargo run --bin server 9090 -- --storage=file --data-dir=./test-data  # Custom config
./target/release/server 8080                     # Production (INFO logging)
cargo run --bin client -- health                 # CLI client
```

## Project Structure

```
src/lib.rs              # Core FlashQ + Record types
src/main.rs             # Entry point 
src/demo.rs             # Interactive demo
src/error.rs            # Comprehensive error handling
src/storage/            # Storage abstraction layer
├── mod.rs              # Public exports and documentation
├── trait.rs            # TopicLog and ConsumerGroup traits
├── backend.rs          # StorageBackend factory with directory locking
├── batching_heuristics.rs # Shared batching utilities and size estimation
├── memory.rs           # InMemoryTopicLog implementation
└── file/               # Segment-based file storage
    ├── mod.rs          # File storage module exports
    ├── common.rs       # Shared serialization utilities
    ├── topic_log.rs    # FileTopicLog implementation
    ├── consumer_group.rs # File-based consumer groups
    ├── segment.rs      # LogSegment implementation
    ├── segment_manager.rs # Segment lifecycle management
    ├── index.rs        # Sparse indexing for segments
    └── file_io.rs      # File I/O operations
src/http/               # HTTP components
├── mod.rs              # HTTP types and validation
├── server.rs           # HTTP server implementation
├── client.rs           # HTTP client utilities
├── cli.rs              # CLI command structures
└── common.rs           # Shared validation logic
src/bin/server.rs       # HTTP server binary with storage backend selection
src/bin/client.rs       # CLI client binary
tests/                  # Integration test suite
├── http_integration_tests.rs # Main HTTP integration test runner
├── storage_integration_tests.rs # Main storage integration test runner
├── http/               # HTTP integration tests (API endpoints, client functionality)
└── storage/            # Storage integration tests (persistence, crash recovery)
benches/                # Performance benchmarks
├── memory_storage.rs   # Memory backend benchmarks
├── file_storage_std.rs # File backend benchmarks
└── batching_baseline.rs # Batching performance benchmarks
```

## Contribution Guidelines

1. **Code Style:** `cargo fmt && cargo clippy` 
2. **Testing:** Include tests for new functionality
3. **Process:** Feature branch → tests → `cargo test` → PR

## Storage Development

### File Storage Testing

```bash
# Test file storage with temporary directories
cargo test --test storage_integration_tests

# Test specific storage components  
cargo test --test storage_integration_tests::file_topic_log_tests
cargo test --test storage_integration_tests::error_simulation_tests
```

### Storage Backend Selection

```bash
# In-memory (default)
cargo run --bin server

# File storage with custom configuration
cargo run --bin server -- --storage=file --data-dir=./dev-data --batch-bytes=262144  # 256KB batches
```

## Debugging

**Logging:** Debug builds use TRACE, release builds use INFO. Structured logging via `log` crate.  
**Port conflicts:** `lsof -ti:8080 | xargs kill` or use different port  
**Test issues:** `cargo test test_name -- --nocapture`  
**File storage:** Check `./data/` directory for persistent state and logs