# Development Guide

This guide covers development setup, testing, and contribution guidelines for message-queue-rs.

## Prerequisites

- **Rust**: Latest stable version (1.70+)
- **Cargo**: Comes with Rust installation
- **Git**: For version control

## Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd message-queue-rs

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
2. **Integration Tests** - Located in `tests/integration_tests.rs`, test HTTP API end-to-end

### Running Tests

```bash
# Run all tests (unit + integration)
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests  
cargo test --test integration_tests

# Run specific test
cargo test test_poll_messages_with_count_limit

# Run tests with output
cargo test -- --nocapture

# Run tests and show ignored
cargo test -- --ignored
```

### Test Coverage

Current test coverage includes:
- Message creation and ID assignment
- FIFO ordering within topics
- Topic isolation (messages don't leak between topics)
- Polling with count limits
- Non-destructive polling (messages persist)
- HTTP API endpoints (POST/GET)
- Error handling for invalid requests
- Health check endpoint

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
cargo run --bin message-queue-rs
```

### HTTP Server
```bash
# Start server on default port (8080)
cargo run --bin server

# Start server on custom port
cargo run --bin server 9090

# Run in background (Linux/macOS)
cargo run --bin server &
```

### CLI Client
```bash
# Post a message
cargo run --bin client -- post news "Development message"

# Poll messages
cargo run --bin client -- poll news

# Poll with count limit
cargo run --bin client -- poll news 5

# Use custom port
cargo run --bin client -- --port=9090 post test "Custom port"
```

## Project Structure for Contributors

```
message-queue-rs/
├── src/
│   ├── lib.rs              # Core library with MessageQueue and Message
│   ├── main.rs             # Main binary entry point  
│   ├── demo.rs             # Interactive demo module
│   ├── api.rs              # HTTP API data structures
│   └── bin/
│       ├── server.rs       # HTTP server implementation
│       └── client.rs       # CLI client implementation
├── tests/
│   └── integration_tests.rs # End-to-end API testing
├── docs/                   # Detailed documentation
├── Cargo.toml              # Project configuration
└── README.md               # Quick start guide
```

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
The project currently uses simple println! for output. For debugging:

```rust
// Add debug prints in your development
println!("Debug: message count = {}", messages.len());
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

# Use curl in loop for basic testing
for i in {1..100}; do
  cargo run --bin client -- post test "Message $i"
done

# Poll to verify all messages received
cargo run --bin client -- poll test
```

### Memory Usage
```bash
# Monitor memory usage during operation
cargo run --bin server &
htop  # or similar system monitor
```