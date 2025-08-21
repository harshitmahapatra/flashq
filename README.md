# Message Queue RS

A lightweight, networked message queue system built in Rust with HTTP REST API. Features topic-based messaging, FIFO ordering, and concurrent client support.

## Features

- **Topic-based messaging** - Organize messages by topics
- **HTTP REST API** - Easy integration with any HTTP client
- **Non-destructive polling** - Messages persist after being read
- **FIFO ordering** - Messages returned in order they were posted
- **Thread-safe** - Concurrent posting and polling support
- **JSON communication** - Structured request/response format

## Quick Start

### Interactive Demo
Explore the message queue with a user-friendly menu interface:

```bash
cargo run --bin message-queue-rs
```

### HTTP Server & Client

**Start the server:**
```bash
# Default port (8080)
cargo run --bin server

# Custom port
cargo run --bin server 9090
```

**Use the CLI client:**
```bash
# Post a message
cargo run --bin client -- post news "Breaking news: Rust is awesome!"

# Poll messages from a topic
cargo run --bin client -- poll news

# Poll with count limit
cargo run --bin client -- poll news 5
```

### HTTP API

Post a message:
```bash
curl -X POST http://127.0.0.1:8080/api/topics/news/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello, World!"}'
```

Poll messages:
```bash
curl http://127.0.0.1:8080/api/topics/news/messages?count=5
```

## Development

```bash
# Build and test
cargo build
cargo test

# Code quality
cargo clippy
cargo fmt

# Release build
cargo build --release
```

## Documentation

- **[API Reference](docs/api.md)** - Complete HTTP API documentation
- **[Architecture](docs/architecture.md)** - Internal design and project structure
- **[Development Guide](docs/development.md)** - Setup, testing, and contribution guidelines

## Dependencies

- **axum** - HTTP server framework
- **tokio** - Async runtime  
- **serde** - JSON serialization
- **reqwest** - HTTP client

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.