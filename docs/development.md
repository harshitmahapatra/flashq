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

- **Unit** (`src/lib.rs`): Core functionality
- **Integration** (`tests/*.rs`): HTTP API and CLI client end-to-end
- **Utilities** (`tests/test_helpers.rs`): Shared infrastructure

### Running Tests

```bash
cargo test                              # All tests
cargo test --lib                        # Unit tests only
cargo test --test server_tests          # Specific integration test
cargo test test_name                     # Specific test
cargo test -- --nocapture              # With output
```

### Test Coverage

**Server Integration:** Record CRUD, FIFO ordering, consumer groups, validation, error handling  
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
cargo run --bin flashq          # Interactive demo
cargo run --bin server          # HTTP server (debug/TRACE logging)
cargo run --bin server 9090     # Custom port
./target/release/server 8080    # Production (INFO logging)
cargo run --bin client -- health # CLI client
```

## Project Structure

```
src/lib.rs          # Core FlashQ + Record types
src/main.rs         # Entry point 
src/demo.rs         # Interactive demo
src/api.rs          # HTTP API structures
src/bin/server.rs   # HTTP server + validation constants
src/bin/client.rs   # CLI client
tests/*.rs          # Integration tests
```

## Contribution Guidelines

1. **Code Style:** `cargo fmt && cargo clippy` 
2. **Testing:** Include tests for new functionality
3. **Process:** Feature branch → tests → `cargo test` → PR

## Debugging

**Logging:** Debug builds use TRACE, release builds use INFO  
**Port conflicts:** `lsof -ti:8080 | xargs kill` or use different port  
**Test issues:** `cargo test test_name -- --nocapture`