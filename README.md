# ⚡ FlashQ

FlashQ is a Kafka-inspired message queue system built in Rust with HTTP REST API. Features topic-based messaging with optional keys and headers, offset-based positioning, consumer groups, and concurrent client support.

**Note: This is a hobby project made by me for learning rust, it should not be used in production.**

## Features

- **Topic-based messaging** - Records organized by topic with optional keys and headers
- **Offset-based positioning** - Sequential offsets within topics with replay functionality
- **Consumer groups** - Offset management for coordinated consumption
- **Batch operations** - Post up to 1000 records per request
- **HTTP REST API** - JSON-based endpoints for posting and polling
- **Thread-safe** - Concurrent access support
- **Request validation** - Input validation with structured error responses

## Quick Start

### Interactive Demo
Explore the flashq with a user-friendly menu interface:

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
cargo run --bin client -- producer records news "Breaking news: Rust is awesome!"

# Post with key and headers
cargo run --bin client -- producer records news "Update message" --key "user123" --header "priority=high" --header "source=mobile"

# Post batch from file
cargo run --bin client -- producer records news --batch batch_records.json

# Create consumer group and fetch messages
cargo run --bin client -- consumer create my-group
cargo run --bin client -- consumer fetch my-group news

# Fetch with options
cargo run --bin client -- consumer fetch my-group news --max-records 5 --from-offset 10

# Manage consumer group offsets
cargo run --bin client -- consumer offset get my-group news
cargo run --bin client -- consumer offset commit my-group news 15

# Leave consumer group
cargo run --bin client -- consumer leave my-group

# Health check
cargo run --bin client -- health
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

# Poll records for consumer group (does not advance offset)
curl http://127.0.0.1:8080/consumer/my-group/topics/news

# Commit offset after processing messages
curl -X POST http://127.0.0.1:8080/consumer/my-group/topics/news/offset \
  -H "Content-Type: application/json" \
  -d '{"offset": 5}'

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
