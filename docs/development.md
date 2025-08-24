# Development Guide

This guide covers development setup, testing, and contribution guidelines for FlashQ.

## Prerequisites

- **Rust**: Latest stable version (1.70+)
- **Cargo**: Comes with Rust installation
- **Git**: For version control

## Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd flashq

# Build the project
cargo build

# Run tests to verify setup
cargo test
```

## Building

### Debug Build (Development)
```bash
# Build all binaries
cargo build

# Build specific binary
cargo build --bin server
cargo build --bin client
```

### Release Build (Production)
```bash
# Build optimized binaries
cargo build --release

# Build specific optimized binary
cargo build --release --bin server
cargo build --release --bin client

# Binaries located in target/release/
```

## Testing

### Test Categories

1. **Unit Tests** - Located in `src/lib.rs`, test core functionality
2. **Server Integration Tests** - Located in `tests/server_tests.rs`, `tests/consumer_tests.rs`, `tests/producer_tests.rs` - test HTTP API end-to-end
3. **Client Integration Tests** - Located in `tests/client_tests.rs` - test CLI client functionality against live server
4. **Test Utilities** - Located in `tests/test_helpers.rs` - shared test infrastructure

### Running Tests

```bash
# Run all tests (unit + integration)
cargo test

# Run only unit tests
cargo test --lib

# Run only server integration tests  
cargo test --test server_tests
cargo test --test consumer_tests
cargo test --test producer_tests

# Run only client integration tests
cargo test --test client_tests

# Run specific test
cargo test test_poll_messages_with_count_limit

# Run tests with output
cargo test -- --nocapture

# Run tests and show ignored
cargo test -- --ignored
```

### Test Coverage

Current test coverage includes:

**Server Integration Tests:**
- Record creation and offset assignment
- FIFO ordering within topics
- Topic isolation (records don't leak between topics)
- Polling with count limits
- Non-destructive polling (records persist)
- HTTP API endpoints (POST/GET)
- Consumer group operations and offset management
- Error handling for invalid requests with OpenAPI-compliant structured responses
- Request validation including schema validation, size limits, and pattern matching
- HTTP status code verification (400, 404, 422, 500)
- Health check endpoint

**Client Integration Tests:**
- CLI client producer commands (`producer records`)
- CLI client consumer commands (`consumer create/fetch/leave`)  
- CLI client offset operations (`consumer offset get/commit`)
- Batch posting with keys, headers, and file input
- Advanced consumer options (max-records, from-offset, include-headers)
- Health check command functionality
- Error handling for invalid CLI commands

### Validation Testing

The project includes comprehensive validation tests for OpenAPI compliance:

```bash
# Test error response structure
cargo test test_server_error_handling

# Test request validation limits  
cargo test test_message_size_and_validation_limits

# Test malformed requests
cargo test test_malformed_requests
```

**Validation Test Coverage:**
- Record size limits (keys: 1024 chars, values: 1MB, headers: 1024 chars each)
- Batch size limits (1-1000 records per batch request)
- Topic name pattern validation (`^[a-zA-Z0-9._][a-zA-Z0-9._-]*$`)
- Consumer group ID pattern validation
- Query parameter limits (`max_records`: 1-10000, `timeout_ms`: 0-60000)
- HTTP status code correctness (400, 404, 422, 500)
- Structured error response format with `error`, `message`, and `details` fields
- Batch validation with per-record error reporting including field paths

## Code Quality

### Linting
```bash
# Run Clippy for code quality suggestions
cargo clippy

# Run Clippy with all features
cargo clippy --all-features

# Fail on warnings
cargo clippy -- -D warnings
```

### Formatting
```bash
# Format code according to Rust standards
cargo fmt

# Check formatting without modifying files
cargo fmt -- --check
```

### Quick Compile Check
```bash
# Fast compilation check without building binaries
cargo check

# Check all targets
cargo check --all-targets
```

## Running During Development

### Interactive Demo
```bash
# Run the educational demo interface
cargo run --bin flashq
```

### HTTP Server
```bash
# Start server on default port (8080) with TRACE logging
cargo run --bin server

# Start server on custom port with TRACE logging
cargo run --bin server 9090

# Production mode with INFO logging (quieter output)
cargo build --release
./target/release/server 8080

# Run in background (Linux/macOS)
cargo run --bin server &
```

**Logging Levels:**
- **Debug builds**: TRACE level - shows detailed request/response information
- **Release builds**: INFO level - production-appropriate minimal logging
- Automatically detected based on compilation flags

### CLI Client
```bash
# Health check
cargo run --bin client -- health

# Producer operations
cargo run --bin client -- producer records news "Development record"
cargo run --bin client -- producer records news "Message with metadata" --key "user123" --header "priority=high"
cargo run --bin client -- producer records news --batch batch_records.json

# Consumer group operations  
cargo run --bin client -- consumer create my-group
cargo run --bin client -- consumer fetch my-group news
cargo run --bin client -- consumer fetch my-group news --max-records 5 --from-offset 10
cargo run --bin client -- consumer offset get my-group news
cargo run --bin client -- consumer offset commit my-group news 15
cargo run --bin client -- consumer leave my-group

# Use custom port
cargo run --bin client -- --port=9090 producer records test "Custom port message"
```

## Project Structure for Contributors

```
flashq/
├── src/
│   ├── lib.rs              # Core library with MessageQueue and Record types
│   ├── main.rs             # Main binary entry point  
│   ├── demo.rs             # Interactive demo module
│   ├── api.rs              # HTTP API data structures
│   └── bin/
│       ├── server.rs       # HTTP server with organized validation constants
│       └── client.rs       # CLI client implementation
├── tests/
│   ├── server_tests.rs      # HTTP server integration tests  
│   ├── consumer_tests.rs    # Consumer group API tests
│   ├── producer_tests.rs    # Producer API tests
│   ├── client_tests.rs      # CLI client functionality tests
│   └── test_helpers.rs      # Shared test infrastructure
├── docs/                   # Detailed documentation
├── Cargo.toml              # Project configuration
└── README.md               # Quick start guide
```

## Code Organization

### Validation Constants

Validation limits are organized in a `limits` module within `src/bin/server.rs`:

```rust
mod limits {
    pub const MAX_KEY_SIZE: usize = 1024;
    pub const MAX_VALUE_SIZE: usize = 1_048_576;
    pub const MAX_HEADER_VALUE_SIZE: usize = 1024;
    pub const MAX_BATCH_SIZE: usize = 1000;
    pub const MAX_POLL_RECORDS: usize = 10000;
}
```

**Benefits:**
- Clear namespace separation for validation constraints
- Easy to locate and modify limits
- Consistent usage with `limits::` prefix throughout code
- Single source of truth for OpenAPI specification compliance

## Contribution Guidelines

### Code Style
- Follow Rust standard formatting (`cargo fmt`)
- Address all Clippy warnings (`cargo clippy`)
- Add appropriate documentation for public APIs
- Include unit tests for new functionality
- Update integration tests for API changes

### Testing Requirements
- All new functionality must include tests
- Maintain or improve test coverage
- Integration tests for HTTP API changes
- Performance tests for significant algorithmic changes

### Documentation
- Update relevant documentation in `docs/` for architectural changes
- Keep README.md concise and focused on quick start
- Add inline documentation for complex logic
- Include usage examples for new features

### Pull Request Process
1. Create feature branch from main
2. Implement changes with tests
3. Run full test suite: `cargo test`
4. Check code quality: `cargo clippy && cargo fmt --check`
5. Update documentation as needed
6. Submit PR with clear description of changes

## Debugging

### Logging

The server implements automatic log level detection:

- **Development** (`cargo run --bin server`): TRACE level logging
  - Shows detailed HTTP request/response information
  - Helpful for debugging API interactions
- **Production** (`./target/release/server`): INFO level logging  
  - Minimal output suitable for production deployment
  - Shows server startup and critical events only

**For debugging during development:**
```rust
// The server uses a built-in logging system
// Add debug prints in your development if needed
println!("Debug: record count = {}", records.len());
```

### Common Issues

**Port Already in Use:**
```bash
# Kill process using port 8080
lsof -ti:8080 | xargs kill

# Or use different port
cargo run --bin server 8081
```

**Test Failures:**
```bash
# Run specific failing test with output
cargo test test_name -- --nocapture

# Run tests in single thread to avoid conflicts
cargo test -- --test-threads=1
```

## Performance Testing

### Basic Load Testing
```bash
# Start server
cargo run --bin server &

# Use client in loop for basic testing
for i in {1..100}; do
  cargo run --bin client -- producer records test "Record $i"
done

# Create consumer group and fetch to verify all records received
cargo run --bin client -- consumer create load-test-group
cargo run --bin client -- consumer fetch load-test-group test
```

### Memory Usage
```bash
# Monitor memory usage during operation
cargo run --bin server &
htop  # or similar system monitor
```