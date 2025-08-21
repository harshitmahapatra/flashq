# Architecture

This document describes the internal architecture and design decisions of the message-queue-rs project.

## Project Structure

The project follows Rust best practices with separate library and binary crates:

### Core Library (`src/lib.rs`)
- **Message** struct - Individual message with content, timestamp, and unique ID
- **MessageQueue** struct - Main queue implementation with topic-based organization  
- **demo** module - Interactive CLI functionality (exposed publicly)
- **api** module - HTTP API data structures and serialization types
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### API Module (`src/api.rs`)
- **PostMessageRequest** - Request structure for posting messages
- **PostMessageResponse** - Response with message ID and timestamp
- **MessageResponse** - Individual message in poll responses
- **PollMessagesResponse** - Complete poll response with message array
- **PollQuery** - Query parameters for polling (count limits)
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
- Post messages interactively with input validation
- Poll messages with topic selection and count limits
- Topic management with session statistics
- Quick demo mode with automated demonstration
- Graceful error handling and user-friendly messages

**Menu Options:**
1. Post a message - Interactive topic and content input
2. Poll messages from a topic - Choose topic and message count limit  
3. View all topics - Show session statistics and topic overview
4. Run quick demo - Automated demonstration of core functionality
5. Exit - Clean program termination

#### HTTP Server Binary (`src/bin/server.rs`)
- REST API server built with Axum framework
- Endpoints for posting and polling messages
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

1. **Message Creation**: Messages are created with unique incrementing IDs and timestamps
2. **Topic Organization**: Messages are stored in HashMap by topic string keys
3. **Thread Safety**: All operations use Arc<Mutex<>> for safe concurrent access
4. **FIFO Ordering**: VecDeque ensures messages are returned in posting order
5. **Non-destructive Polling**: Messages remain in queue after being read
6. **HTTP Serialization**: API structures handle JSON conversion seamlessly

## Design Decisions

### Thread Safety
- Uses `Arc<Mutex<HashMap<String, VecDeque<Message>>>>` for topic storage
- Single lock protects entire queue structure for simplicity
- Trade-off: Coarse-grained locking for implementation simplicity

### Memory Management
- In-memory storage only (messages lost on restart)
- No built-in persistence or durability guarantees
- Suitable for development, testing, and ephemeral messaging needs

### API Design
- REST endpoints follow standard HTTP conventions
- JSON for structured communication
- Separate request/response types for type safety
- Optional query parameters for flexible polling

## Performance Characteristics

- **Memory Usage**: O(n) where n is total messages across all topics
- **Time Complexity**: O(1) for posting, O(k) for polling k messages
- **Concurrency**: Thread-safe but single lock may create contention
- **Network**: HTTP/1.1 with JSON serialization overhead
- **Ordering Guarantees**: FIFO within topics, no cross-topic ordering