# Message Queue RS

A Kafka-inspired message queue system built in Rust with HTTP REST API. Features topic-based messaging with optional keys and headers, offset-based positioning, consumer groups, and concurrent client support.

## Features

- **Kafka-style messaging** - Optional message keys and headers for routing/metadata
- **Topic-based organization** - Messages organized by topic strings
- **Offset-based positioning** - Sequential message positioning within topics
- **Replay functionality** - Seek to specific offsets for message replay
- **Consumer groups** - Coordinated consumption with offset management
- **HTTP REST API** - Easy integration with any HTTP client
- **Non-destructive polling** - Messages persist after being read
- **FIFO ordering** - Messages returned in order they were posted
- **Thread-safe** - Concurrent posting and polling support
- **ISO 8601 timestamps** - Human-readable message timestamps

## Quick Start

### Interactive Demo
Explore the message queue with a user-friendly menu interface:

```bash
cargo run --bin message-queue-rs
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

Post a simple message:
```bash
curl -X POST http://127.0.0.1:8080/api/topics/news/messages \
  -H "Content-Type: application/json" \
  -d '{"key": null, "value": "Hello, World!", "headers": null}'
```

Post a message with key and headers:
```bash
curl -X POST http://127.0.0.1:8080/api/topics/news/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "user123", "value": "Important update", "headers": {"priority": "high", "source": "mobile"}}'
```

Poll messages:
```bash
# Get all messages from topic
curl http://127.0.0.1:8080/api/topics/news/messages

# Limit to 5 messages
curl http://127.0.0.1:8080/api/topics/news/messages?count=5

# Replay from specific offset (seek functionality)
curl http://127.0.0.1:8080/api/topics/news/messages?from_offset=10&count=5
```

Consumer group operations:
```bash
# Create consumer group
curl -X POST http://127.0.0.1:8080/api/consumer-groups \
  -H "Content-Type: application/json" \
  -d '{"group_id": "my-group"}'

# Poll messages for consumer group (automatically advances offset)
curl http://127.0.0.1:8080/api/consumer-groups/my-group/topics/news/messages

# Replay from specific offset for consumer group (doesn't advance group offset)  
curl http://127.0.0.1:8080/api/consumer-groups/my-group/topics/news/messages?from_offset=5&count=3
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