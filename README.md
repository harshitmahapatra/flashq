# Message Queue RS

A lightweight, networked message queue system built in Rust with HTTP REST API. Features topic-based messaging, FIFO ordering, and concurrent client support.

## Features

- **Topic-based messaging** - Organize messages by topics
- **HTTP REST API** - Easy integration with any HTTP client
- **Non-destructive polling** - Messages persist after being read
- **FIFO ordering** - Messages returned in order they were posted
- **Thread-safe** - Concurrent posting and polling support
- **Configurable ports** - Flexible deployment options
- **JSON communication** - Structured request/response format

## Architecture

The project follows Rust best practices with separate library and binary crates:

- **Library crate** (`src/lib.rs`) - Core message queue functionality
- **Interactive demo** (`src/main.rs`) - Menu-driven CLI for exploring functionality
- **Server binary** (`src/bin/server.rs`) - HTTP API server
- **Client binary** (`src/bin/client.rs`) - Command-line client
- **Integration tests** (`tests/integration_tests.rs`) - End-to-end API testing

## Dependencies

- **axum** - HTTP server framework
- **tokio** - Async runtime  
- **serde** - JSON serialization
- **reqwest** - HTTP client (rustls-tls)

## Quick Start

### Try the Interactive Demo

```bash
# Explore the message queue with a user-friendly menu
cargo run --bin message-queue-rs
```

The interactive demo provides a guided way to:
- Post messages to topics
- Poll messages with optional count limits  
- View created topics
- Run automated demonstrations

### Running the Server

```bash
# Default port (8080)
cargo run --bin server

# Custom port
cargo run --bin server 9090
```

### Using the Client

```bash
# Post a message
cargo run --bin client -- post news "Breaking news: Rust is awesome!"

# Poll messages from a topic
cargo run --bin client -- poll news

# Poll with count limit
cargo run --bin client -- poll news 5

# Using custom port
cargo run --bin client -- --port=9090 post news "Custom port message"
cargo run --bin client -- -p 9090 poll news
```

## API Reference

### Server Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/topics/{topic}/messages` | Post a message to a topic |
| `GET` | `/api/topics/{topic}/messages` | Poll messages from a topic |
| `GET` | `/health` | Health check endpoint |

### POST Message

**Request:**
```bash
curl -X POST http://127.0.0.1:8080/api/topics/news/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello, World!"}'
```

**Response:**
```json
{
  "id": 0,
  "timestamp": 1755635398
}
```

### Poll Messages

**Request:**
```bash
# Get all messages
curl http://127.0.0.1:8080/api/topics/news/messages

# Limit to 5 messages
curl http://127.0.0.1:8080/api/topics/news/messages?count=5
```

**Response:**
```json
{
  "messages": [
    {
      "id": 0,
      "content": "Hello, World!",
      "timestamp": 1755635398
    }
  ],
  "count": 1
}
```

## Client Usage

### Command Line Interface

```bash
# Show help
cargo run --bin client -- help

# Post messages
cargo run --bin client -- post <topic> <message>

# Poll messages
cargo run --bin client -- poll <topic> [count]

# Port configuration
cargo run --bin client -- --port=9090 <command>
cargo run --bin client -- -p 9090 <command>
```

### Examples

```bash
# Basic messaging
cargo run --bin client -- post alerts "System maintenance at 2PM"
cargo run --bin client -- post alerts "Maintenance complete"
cargo run --bin client -- poll alerts

# E-commerce notifications
cargo run --bin client -- post orders "New order #1234"
cargo run --bin client -- post orders "Order #1234 shipped"
cargo run --bin client -- poll orders 10

# Chat system
cargo run --bin client -- post chat "User Alice joined"
cargo run --bin client -- post chat "Alice: Hello everyone!"
cargo run --bin client -- poll chat
```

## Development

### Building

```bash
# Build all binaries
cargo build

# Build release version
cargo build --release

# Build specific binary
cargo build --bin server
cargo build --bin client
```

### Testing

```bash
# Run all tests (unit + integration)
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration_tests

# Run tests with output
cargo test -- --nocapture

# Test specific function
cargo test test_poll_messages_with_count_limit

# Test end-to-end workflow
cargo test test_end_to_end_workflow
```

## Production Deployment

### Building for Production

Build optimized release binaries for deployment:

```bash
# Build all binaries (recommended)
cargo build --release

# Build specific binaries
cargo build --release --bin server
cargo build --release --bin client

# Binaries will be in target/release/
ls -la target/release/
```

### Running Production Binaries

**Start the Server:**
```bash
# Default port (8080)
./target/release/server

# Custom port
./target/release/server 9090

# Run in background (Linux/macOS)
./target/release/server 8080 &

# Check if server is running
curl http://127.0.0.1:8080/health
```

**Use the Client:**
```bash
# Basic operations
./target/release/client post alerts "System starting up"
./target/release/client poll alerts

# With custom server port
./target/release/client --port=9090 post news "Production message"
./target/release/client -p 9090 poll news 10
```

### Installation Options

#### Option 1: Cargo Install (Recommended)
```bash
# Install from local source
cargo install --path . --bin server
cargo install --path . --bin client

# Binaries installed to ~/.cargo/bin (ensure it's in PATH)
# Now you can run from anywhere:
server 8080
client post news "Hello from installed binary"
```

#### Option 2: Manual Installation
```bash
# Copy to system binaries directory
sudo cp target/release/server /usr/local/bin/
sudo cp target/release/client /usr/local/bin/
sudo chmod +x /usr/local/bin/server
sudo chmod +x /usr/local/bin/client

# Run from anywhere
server 8080
client post news "System-wide installation"
```

## Performance Characteristics

- **Memory usage** - In-memory storage with O(1) append/read operations
- **Concurrency** - Thread-safe with Mutex protection
- **Network** - HTTP/1.1 with JSON serialization
- **Ordering** - FIFO guarantee within topics
- **Persistence** - Messages retained until process restart

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
