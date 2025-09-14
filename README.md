# ⚡ FlashQ

A Kafka-inspired record queue system built in Rust with HTTP REST API. The project consists of two crates in a Cargo workspace:

- **`flashq`** - Core library with storage backends and queue management  
- **`flashq-http`** - HTTP broker, producer, consumer, and client implementations

**Note: This is a hobby project for learning Rust - not for production use.**

## Features

- Topic-based messaging with keys and headers
- Partition-aware consumer groups with per-partition offset tracking
- Time-based polling for historical data queries
- Configurable batch operations for high-throughput processing
- HTTP REST API with JSON
- Thread-safe concurrent access
- Kafka-aligned segment-based file storage with crash recovery
- Error handling with structured logging
- Pluggable storage backends (in-memory, file)
- Performance benchmarking with memory profiling

## Quick Start

### Interactive Demo
Explore the flashq with a user-friendly menu interface:

```bash
cargo run -p flashq --bin flashq
```

### HTTP Broker & Client

**Start broker:**
```bash
# In-memory storage (default)
cargo run -p flashq-http --bin broker           # Debug mode (port 8080)
cargo run -p flashq-http --bin broker 9090      # Custom port

# File storage backend
cargo run -p flashq-http --bin broker -- --storage=file --data-dir=./data
cargo run -p flashq-http --bin broker 9090 -- --storage=file --data-dir=./custom

# Configure batch size for performance tuning
cargo run -p flashq-http --bin broker -- --batch-bytes=65536      # 64KB batches
cargo run -p flashq-http --bin broker -- --storage=file --batch-bytes=131072  # 128KB batches
```

**Basic client usage:**
```bash
# Post record
cargo run -p flashq-http --bin client -- producer records news "Hello, World!"

# Create and use consumer group
cargo run -p flashq-http --bin client -- consumer create my-group
cargo run -p flashq-http --bin client -- consumer fetch my-group news
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
cargo build && cargo test     # Build and run all tests (workspace)
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
