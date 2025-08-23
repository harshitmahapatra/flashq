# Architecture

This document describes the internal architecture and design decisions of the message-queue-rs project.

## Project Structure

The project follows Rust best practices with separate library and binary crates:

### Core Library (`src/lib.rs`)
- **Record** struct - Individual record with value, headers, key, and timestamp
- **MessageQueue** struct - Main queue implementation with topic-based organization (handles Record types)  
- **demo** module - Interactive CLI functionality (exposed publicly)
- **api** module - HTTP API data structures and serialization types
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### API Module (`src/api.rs`)
- **PostRecordRequest** - Request structure for posting records
- **PostRecordResponse** - Response with record offset and timestamp
- **RecordResponse** - Individual record in poll responses
- **FetchResponse** - Complete poll response with record array
- **PollQuery** - Query parameters for polling (count limits, offset positioning)
- **ErrorResponse** - Standardized error response format
- All structures use serde for JSON serialization/deserialization

### Binary Crates

#### Main Binary (`src/main.rs`)
- Lightweight entry point (2 lines) following Rust best practices
- Delegates execution to the demo module

#### Interactive Demo (`src/demo.rs`)
The demo module provides an educational interactive demonstration:

**Features:**
- Menu-driven interface with 5 clear options
- Post records interactively with input validation
- Poll records with topic selection and count limits
- Topic management with session statistics
- Quick demo mode with automated demonstration
- Graceful error handling and user-friendly messages

**Menu Options:**
1. Post a record - Interactive topic and content input
2. Poll records from a topic - Choose topic and record count limit  
3. View all topics - Show session statistics and topic overview
4. Run quick demo - Automated demonstration of core functionality
5. Exit - Clean program termination

#### HTTP Server Binary (`src/bin/server.rs`)
- REST API server built with Axum framework
- Endpoints for posting and polling records
- Health check endpoint for monitoring
- JSON request/response handling
- Error handling with proper HTTP status codes

#### CLI Client Binary (`src/bin/client.rs`)
- Command-line interface for interacting with HTTP server
- Support for posting and polling operations
- Configurable server port
- Built with reqwest HTTP client

### Test Suite (`tests/integration_tests.rs`)
- End-to-end workflow testing with multiple topics
- HTTP API validation with real server instances
- FIFO ordering verification 
- Count parameter testing for polling limits
- Error handling validation for invalid requests
- Health check endpoint testing

## Data Flow

1. **Record Creation**: Records are created with sequential offsets and ISO 8601 timestamps
2. **Topic Organization**: Records are stored in TopicLog structures organized by topic string keys
3. **Thread Safety**: All operations use Arc<Mutex<>> for safe concurrent access
4. **FIFO Ordering**: Append-only log with offset-based access ensures records are returned in posting order
5. **Non-destructive Polling**: Records remain in queue after being read
6. **HTTP Serialization**: API structures handle JSON conversion seamlessly

## Design Decisions

### Append-Only Log Architecture
- **TopicLog Structure**: Each topic maintains an append-only log with `Vec<RecordWithOffset>` and offset tracking
- **Offset Management**: Records are assigned sequential offsets (0, 1, 2...) within each topic
- **Consumer Groups**: Support for consumer group state with per-topic offset tracking
- **Immutable History**: Records are never modified or deleted, only appended

### Thread Safety
- Uses `Arc<Mutex<HashMap<String, TopicLog>>>` for topic storage with append-only semantics
- Consumer groups managed with `Arc<Mutex<HashMap<String, ConsumerGroup>>>`
- Single lock protects entire queue structure for simplicity
- Trade-off: Coarse-grained locking for implementation simplicity

### Memory Management
- In-memory storage only (records lost on restart)
- No built-in persistence or durability guarantees
- Suitable for development, testing, and ephemeral messaging needs

### API Design
- REST endpoints follow standard HTTP conventions
- JSON for structured communication
- Separate request/response types for type safety
- Optional query parameters for flexible polling

## Performance Characteristics

- **Memory Usage**: O(n) where n is total records across all topics
- **Time Complexity**: O(1) for posting, O(k) for polling k records
- **Concurrency**: Thread-safe but single lock may create contention
- **Network**: HTTP/1.1 with JSON serialization overhead
- **Ordering Guarantees**: FIFO within topics, no cross-topic ordering