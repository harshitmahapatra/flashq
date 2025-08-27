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

### Test Coverage

**HTTP Integration:** Record CRUD, FIFO ordering, consumer groups, validation, error handling  
**Storage Integration:** File persistence, crash recovery, directory locking, error simulation  
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
├── memory.rs           # InMemoryTopicLog implementation
└── file.rs             # FileTopicLog with WAL and crash recovery
src/http/               # HTTP components
├── mod.rs              # HTTP types and validation
├── server.rs           # HTTP server implementation
├── client.rs           # HTTP client utilities
├── cli.rs              # CLI command structures
└── common.rs           # Shared validation logic
src/bin/server.rs       # HTTP server binary with storage backend selection
src/bin/client.rs       # CLI client binary
tests/http/             # HTTP integration tests
tests/storage/          # Storage integration tests
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
cargo run --bin server -- --storage=file --data-dir=./dev-data --sync-mode=periodic --wal-commit-threshold=100
```

## Debugging

**Logging:** Debug builds use TRACE, release builds use INFO. Structured logging via `log` crate.  
**Port conflicts:** `lsof -ti:8080 | xargs kill` or use different port  
**Test issues:** `cargo test test_name -- --nocapture`  
**File storage:** Check `./data/` directory for persistent state and logs