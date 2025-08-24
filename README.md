# ⚡ FlashQ

FlashQ is a Kafka-inspired message queue system built in Rust with HTTP REST API. Features topic-based messaging with optional keys and headers, offset-based positioning, consumer groups, and concurrent client support.

**Note: This is a hobby project made by me for learning rust, it should not be used in production.**

## Features

- **Batch producer API** - Post 1-1000 records per request for efficient throughput
- **Kafka-style messaging** - Optional record keys and headers for routing/metadata
- **Topic-based organization** - Records organized by topic strings
- **Offset-based positioning** - Sequential record positioning within topics
- **Replay functionality** - Seek to specific offsets for record replay
- **Consumer groups** - Coordinated consumption with offset management
- **HTTP REST API** - Easy integration with any HTTP client
- **Non-destructive polling** - Records persist after being read
- **FIFO ordering** - Records returned in order they were posted
- **Thread-safe** - Concurrent posting and polling support
- **ISO 8601 timestamps** - Human-readable record timestamps
- **OpenAPI-compliant error handling** - Structured error responses with proper HTTP status codes
- **Request validation** - Comprehensive validation with detailed error context and field-level reporting

## Quick Start

### Interactive Demo
Explore the message queue with a user-friendly menu interface:

```bash
cargo run --bin flashq
```

### HTTP Server & Client

**Start the server:**
```bash
# Default port (8080) - TRACE logging in debug mode
cargo run --bin server

# Custom port - TRACE logging in debug mode
cargo run --bin server 9090

# Production mode - INFO logging only
cargo build --release
./target/release/server 8080
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

Post a single record (batch format):
```bash
curl -X POST http://127.0.0.1:8080/topics/news/records \
  -H "Content-Type: application/json" \
  -d '{"records": [{"key": null, "value": "Hello, World!", "headers": null}]}'
```

Post multiple records in a batch:
```bash
curl -X POST http://127.0.0.1:8080/topics/news/records \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {"key": "user123", "value": "Important update", "headers": {"priority": "high", "source": "mobile"}},
      {"key": "user456", "value": "Another message", "headers": {"priority": "low", "source": "web"}}
    ]
  }'
```

Poll records:
```bash
# Get all records from topic
curl http://127.0.0.1:8080/topics/news/messages

# Limit to 5 records
curl http://127.0.0.1:8080/topics/news/messages?count=5

# Replay from specific offset (seek functionality)
curl http://127.0.0.1:8080/topics/news/messages?from_offset=10&count=5
```

Consumer group operations:
```bash
# Create consumer group
curl -X POST http://127.0.0.1:8080/consumer/my-group \
  -H "Content-Type: application/json" \
  -d '{"group_id": "my-group"}'

# Poll records for consumer group (automatically advances offset)
curl http://127.0.0.1:8080/consumer/my-group/topics/news

# Replay from specific offset for consumer group (doesn't advance group offset)  
curl http://127.0.0.1:8080/consumer/my-group/topics/news?from_offset=5&count=3

# Leave consumer group
curl -X DELETE http://127.0.0.1:8080/consumer/my-group
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
- **chrono** - ISO 8601 timestamp handling
- **clap** - Command-line interface

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
