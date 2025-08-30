# ⚡ FlashQ

A Kafka-inspired record queue system built in Rust with HTTP REST API.

**Note: This is a hobby project for learning Rust - not for production use.**

## Features

- Topic-based messaging with keys and headers
- Consumer groups with offset management  
- Batch operations (up to 1000 records)
- HTTP REST API with JSON
- Thread-safe concurrent access
- Kafka-aligned segment-based file storage with crash recovery
- Error handling with structured logging
- Pluggable storage backends (in-memory and file-based)
- Performance benchmarking with memory profiling

## Quick Start

### Interactive Demo
Explore the flashq with a user-friendly menu interface:

```bash
cargo run --bin flashq
```

### HTTP Server & Client

**Start server:**
```bash
# In-memory storage (default)
cargo run --bin server           # Debug mode (port 8080)
cargo run --bin server 9090      # Custom port

# File storage backend
cargo run --bin server -- --storage=file --data-dir=./data
cargo run --bin server 9090 -- --storage=file --sync-mode=always
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
cargo build && cargo test     # Build and run all tests
cargo test --test '*'         # Integration tests only
cargo bench                   # Run performance benchmarks
cargo clippy && cargo fmt     # Code quality and formatting
cargo check                   # Quick compile check
```

## Documentation

- **[API Reference](docs/api.md)** - Complete HTTP API documentation
- **[Performance](docs/performance.md)** - Benchmarking results and storage backend comparison
- **[Architecture](docs/architecture.md)** - Internal design and project structure
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines

## License

MIT License - see [LICENSE](LICENSE) file for details.
