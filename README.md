# ⚡ FlashQ

A Kafka-inspired record queue system built in Rust with gRPC API. The project consists of four crates in a Cargo workspace:

- **`flashq-storage`** - Storage backend implementations (memory and file-based)
- **`flashq`** - Core queue library and API
- **`flashq-grpc`** - gRPC broker, producer, consumer, and client implementations
- **`flashq-cluster`** - Cluster coordination and metadata management

**Note: This is a hobby project for learning Rust - not for production use.**

## Features

- Topic-based messaging with keys and headers
- Partition-aware consumer groups with snapshot-based offset storage
- Time-based polling for historical data queries
- Configurable batch operations for high-throughput processing
- gRPC API with Protocol Buffers
- Real-time streaming subscriptions
- Cluster coordination with metadata management and heartbeat protocol
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

### gRPC Server & Client

**Start gRPC server:**
```bash
# In-memory storage (default)
cargo run -p flashq-broker --bin grpc-server           # Default port 50051
cargo run -p flashq-broker --bin grpc-server -- --port=50052

# File storage backend
cargo run -p flashq-broker --bin grpc-server -- --storage=file --data-dir=./data
```

**Basic gRPC client usage:**
```bash
# Post records
cargo run -p flashq-broker --bin grpc-client -- produce --topic=news --value="Hello gRPC!"

# Create consumer group and fetch
cargo run -p flashq-broker --bin grpc-client -- create-group --group-id=my-group
cargo run -p flashq-broker --bin grpc-client -- fetch-offset --group-id=my-group --topic=news

# Real-time streaming subscription
cargo run -p flashq-broker --bin grpc-client -- subscribe --group-id=my-group --topic=news
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

- **[API Reference](docs/api.md)** - Complete gRPC API documentation
- **[Performance](docs/performance.md)** - Benchmarking results and storage backend comparison
- **[Architecture](docs/architecture.md)** - Internal design and project structure
- **[Development Guide](docs/development.md)** - Development workflow and contribution guidelines

## License

MIT License - see [LICENSE](LICENSE) file for details.
