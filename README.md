# ⚡ FlashQ

A Kafka-inspired record queue system built in Rust with HTTP REST API.

**Note: This is a hobby project for learning Rust - not for production use.**

## Features

- Topic-based messaging with keys and headers
- Consumer groups with offset management  
- Batch operations (up to 1000 records)
- HTTP REST API with JSON
- Thread-safe concurrent access
- Pluggable storage backends

## Quick Start

### Interactive Demo
Explore the flashq with a user-friendly menu interface:

```bash
cargo run --bin flashq
```

### HTTP Server & Client

**Start server:**
```bash
cargo run --bin server           # Debug mode (port 8080)
cargo run --bin server 9090      # Custom port
```

**Basic client usage:**
```bash
# Post record
cargo run --bin client -- producer records news "Hello, World!"

# Create and use consumer group
cargo run --bin client -- consumer create my-group
cargo run --bin client -- consumer fetch my-group news
```

**HTTP API examples:**
```bash
# Post record
curl -X POST http://127.0.0.1:8080/topics/news/records \
  -H "Content-Type: application/json" \
  -d '{"records": [{"value": "Hello, World!"}]}'

# Poll records  
curl http://127.0.0.1:8080/topics/news/records
```

## Development

```bash
cargo build && cargo test     # Build and test
cargo clippy && cargo fmt     # Code quality
```

## Documentation

- **[API Reference](docs/api.md)** - Complete HTTP API documentation
- **[Architecture](docs/architecture.md)** - Internal design and project structure
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines

## License

MIT License - see [LICENSE](LICENSE) file for details.
