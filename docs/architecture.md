# Architecture

This document describes the internal architecture and design decisions of the FlashQ project.

## Project Structure

The project follows Rust best practices with separate library and binary crates:

### Core Library (`src/lib.rs`)
- **Record** struct - Individual record with value, headers, key, and timestamp
- **RecordWithOffset** - Record with offset and timestamp for polling responses
- **MessageQueue** struct - Main queue implementation with topic-based organization (handles Record types)  
- **demo** module - Interactive CLI functionality (exposed publicly)
- **api** module - HTTP API data structures and serialization types
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### API Module (`src/api.rs`)
- **ProduceRequest** - Batch request structure containing array of records
- **ProduceResponse** - Response with array of offset information
- **FetchResponse** - Complete poll response with record array
- **PollQuery** - Query parameters for polling (count limits, offset positioning)
- **ErrorResponse** - OpenAPI-compliant structured error responses with semantic error codes
- **OffsetInfo** - Individual offset and timestamp information in batch responses
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
- Batch producer API supporting 1-1000 records per request
- Endpoints for posting and polling records
- Consumer group operations with offset management
- Health check endpoint for monitoring
- JSON request/response handling
- OpenAPI-compliant error handling with structured responses and semantic error codes
- Request validation with detailed error messages and field-specific context
- HTTP status code mapping (400, 404, 422, 500) based on error types
- Organized validation constants in `limits` module for maintainability

#### CLI Client Binary (`src/bin/client.rs`)
- Structured command-line interface with producer/consumer subcommands
- Built with clap for argument parsing and reqwest for HTTP communication
- Configurable server port with `--port` flag

**Command Structure:**
- **Health Check**: `health` - Server status verification
- **Producer Commands**: `producer records` - Batch record posting with optional keys/headers
- **Consumer Commands**: `consumer create/fetch/leave` - Consumer group lifecycle management  
- **Offset Commands**: `consumer offset get/commit` - Consumer group offset operations

**Features:**
- Batch posting from JSON files using `--batch` parameter
- Advanced consumer options (max-records, from-offset, include-headers)
- Comprehensive error handling with user-friendly messages
- Support for complex headers via `--header key=value` syntax

### Test Suite 
Comprehensive integration tests across multiple files:

#### Server Tests (`tests/server_tests.rs`, `tests/consumer_tests.rs`, `tests/producer_tests.rs`)
- End-to-end HTTP API workflow testing with multiple topics
- FIFO ordering verification and count parameter testing 
- Consumer group operations and offset management validation
- Comprehensive error handling including schema validation and edge cases
- Health check endpoint testing with real server instances

#### Client Integration Tests (`tests/client_tests.rs`)
- CLI client functionality verification against live server
- Consumer group operations: create, fetch, offset management, leave
- Batch posting with advanced options (keys, headers, file-based batches)
- Health check command validation
- Error handling for invalid commands and server communication failures

#### Test Utilities (`tests/test_helpers.rs`)
- Shared test server management and client binary verification
- Consistent test environment setup across all integration test suites

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

### Error Handling Architecture
- **OpenAPI Compliance**: Structured error responses with `error`, `message`, and optional `details` fields
- **Semantic Error Codes**: Machine-readable error identifiers (`validation_error`, `topic_not_found`, etc.)
- **HTTP Status Code Mapping**: Proper status codes based on error type (400/404/422/500)
- **Request Validation**: Multi-layer validation including schema, size limits, and pattern matching
- **Early Validation**: Invalid requests rejected before reaching business logic
- **Contextual Details**: Error responses include specific field information and limit details

## Performance Characteristics

- **Memory Usage**: O(n) where n is total records across all topics
- **Time Complexity**: O(1) for posting, O(k) for polling k records
- **Concurrency**: Thread-safe but single lock may create contention
- **Network**: HTTP/1.1 with JSON serialization overhead
- **Ordering Guarantees**: FIFO within topics, no cross-topic ordering