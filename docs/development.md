# Development Guide

Development setup, testing, and contribution guidelines for FlashQ.

## Setup

```bash
git clone <repository-url> && cd flashq
cargo build && cargo test  # Verify setup (workspace)
```

## Building

```bash
cargo build                    # Debug build (workspace)
cargo build --release          # Release build (target/release/)
cargo build -p flashq-grpc --bin grpc-server # gRPC server binary
cargo build -p flashq --bin flashq        # Demo binary
```

## Testing

### Test Types

- **Unit** (`crates/*/src/lib.rs`): Core functionality and data structures
- **gRPC Integration** (`crates/flashq-grpc/tests/grpc/`): gRPC services and streaming functionality
- **Storage Integration** (`crates/flashq-storage/tests/storage/`): File backend, persistence, and crash recovery
- **Benchmarks** (`crates/flashq-storage/benches/`): Performance testing with memory profiling
- **Utilities** (`crates/*/tests/*/test_utilities.rs`): Shared test infrastructure

### Running Tests

```bash
cargo test                              # All tests (workspace)
cargo test -p flashq --lib              # Core library unit tests
cargo test -p flashq-storage --lib      # Storage library unit tests
cargo test -p flashq-grpc --lib         # gRPC library unit tests
cargo test --test '*'                   # All integration tests
cargo test -p flashq-grpc --test grpc_integration_tests # gRPC integration tests
cargo test -p flashq-storage --test storage_integration_tests # Storage integration tests
cargo test test_name                     # Specific test
cargo test -- --nocapture              # With output
RUST_LOG=debug cargo test              # Tests with debug logging output
```

### Benchmarking

```bash
cargo bench                              # Run all benchmarks
cargo bench -p flashq-storage --bench memory_storage       # Memory storage benchmarks only
cargo bench -p flashq-storage --bench file_storage_std     # File storage benchmarks only
cargo bench -p flashq-storage --bench batching_baseline    # Batching performance benchmarks
```

### Test Coverage

**gRPC Integration:** Producer/Consumer/Admin services, streaming subscriptions, Protocol Buffer serialization
**Storage Integration:** File persistence, crash recovery, directory locking, error simulation, partition tests
**Partition Integration:** Multi-partition consumer groups, backward compatibility with partition 0
**Batching Integration:** Batch operations, performance validation, memory efficiency
**Client Integration:** CLI commands, batch operations, consumer group lifecycle
**Validation:** Size limits, pattern validation

## Code Quality

```bash
cargo clippy                    # Linting
cargo fmt                       # Format code  
cargo check                     # Fast compile check
```

## Running During Development

```bash
cargo run -p flashq --bin flashq                           # Interactive demo
cargo run -p flashq-grpc --bin grpc-server                 # gRPC server (in-memory, TRACE logging)
cargo run -p flashq-grpc --bin grpc-server -- --storage=file # gRPC file storage
./target/release/grpc-server                               # Production gRPC (INFO logging)
cargo run -p flashq-grpc --bin grpc-client -- connect      # gRPC CLI client
```

## Project Structure

```
Cargo.toml              # Workspace configuration
crates/
├── flashq-storage/     # Storage backend library crate
│   ├── src/lib.rs      # Record types and storage exports
│   ├── src/error.rs    # Storage error types
│   ├── src/storage/    # Storage abstraction layer
│   │   ├── mod.rs      # Public exports and documentation
│   │   ├── trait.rs    # TopicLog and ConsumerOffsetStore traits
│   │   ├── backend.rs  # StorageBackend factory with directory locking
│   │   ├── batching_heuristics.rs # Shared batching utilities
│   │   ├── memory.rs   # InMemoryTopicLog implementation
│   │   └── file/       # Segment-based file storage
│   │       ├── mod.rs  # File storage module exports
│   │       ├── common.rs # Shared serialization utilities
│   │       ├── topic_log.rs # FileTopicLog implementation
│   │       ├── consumer_group.rs # File-based consumer groups
│   │       ├── segment.rs # LogSegment implementation
│   │       ├── segment_manager.rs # Segment lifecycle management
│   │       ├── index.rs # Sparse indexing for segments
│   │       └── file_io.rs # File I/O operations
│   ├── tests/storage/  # Storage integration tests
│   └── benches/        # Performance benchmarks
├── flashq/             # Core queue library crate
│   ├── src/lib.rs      # Core FlashQ implementation
│   ├── src/main.rs     # Entry point for demo binary
│   ├── src/demo.rs     # Interactive demo
│   └── src/error.rs    # FlashQ error types
├── flashq-grpc/        # gRPC broker crate
│   ├── Cargo.toml      # gRPC dependencies (tonic, prost, protoc)
│   ├── build.rs        # Protocol Buffer code generation
│   ├── proto/flashq.proto # Protocol Buffer schema
│   ├── src/lib.rs      # gRPC library exports
│   ├── src/bin/server.rs # gRPC server binary
│   ├── src/bin/client.rs # gRPC CLI client binary
│   ├── src/server.rs   # Producer/Consumer/Admin service implementations
│   ├── src/client.rs   # gRPC client connection utilities
│   └── tests/grpc/     # gRPC integration tests
└── flashq-cluster/     # Cluster coordination crate
    ├── src/service.rs  # ClusterService implementation
    ├── src/metadata_store/ # Metadata storage
    └── tests/          # Cluster integration tests
```

## Contribution Guidelines

1. **Code Style:** `cargo fmt && cargo clippy` 
2. **Testing:** Include tests for new functionality
3. **Process:** Feature branch → tests → `cargo test` → PR

## Storage Development

### File Storage Testing

```bash
# Test file storage with temporary directories
cargo test -p flashq-storage --test storage_integration_tests

# Test specific storage components
cargo test -p flashq-storage --test storage_integration_tests::file_topic_log_tests
cargo test -p flashq-storage --test storage_integration_tests::error_simulation_tests

# Test partition functionality
cargo test -p flashq-storage --test storage_integration_tests::partition_tests
cargo test -p flashq-storage --test storage_integration_tests::partition_backward_compatibility_tests
RUST_LOG=flashq_storage::storage::file::consumer_group=debug cargo test partition_tests -- --nocapture
```

### Storage Backend Selection

```bash
# In-memory (default)
cargo run -p flashq-grpc --bin grpc-server

# File storage with custom configuration
cargo run -p flashq-grpc --bin grpc-server -- --storage=file --data-dir=./dev-data --batch-bytes=262144  # 256KB batches
```

## Debugging

### Tracing and Logging

FlashQ uses `tracing` and `log` crates for instrumentation with automatic level filtering:
- **Debug builds:** TRACE level (verbose)
- **Release builds:** INFO level (production)
- **Environment control:** Set `RUST_LOG=debug` or `RUST_LOG=flashq=trace,warn`
- **Test logging:** Tests use `test-log` with tracing features for visible output
- **Test execution:** Use `RUST_LOG=debug cargo test` to see logging during tests
- **Benchmarks:** Automatically disabled via `init_for_benchmarks()` to prevent overhead

**Test Log Configuration:**
- Add `#[test_log::test]` attribute to tests for automatic log capture
- Configure `test-log = { version = "0.2", features = ["trace"] }` in Cargo.toml
- Test output includes structured tracing spans and debug information

### Common Issues

**Port conflicts:** `lsof -ti:8080 | xargs kill` or use different port  
**Test issues:** `cargo test test_name -- --nocapture`  
**File storage:** Check `./data/` directory for persistent state and logs  
**Tracing output:** Control with `RUST_LOG` environment variable