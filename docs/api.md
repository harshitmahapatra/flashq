# API Reference

Complete HTTP API documentation for the FlashQ server.

## Base URL

Default server runs on `http://127.0.0.1:8080`

## CLI Client

FlashQ includes a comprehensive CLI client that provides a structured interface for interacting with the server. The client uses subcommands for different operations:

### Producer Operations

```bash
# Post a simple message
cargo run --bin client -- producer records news "Breaking news message"

# Post with key and headers
cargo run --bin client -- producer records news "User update" --key "user123" --header "priority=high" --header "source=mobile"

# Batch post from JSON file
cargo run --bin client -- producer records news --batch records.json

# Use custom server port
cargo run --bin client -- --port 9090 producer records news "Custom port message"
```

### Consumer Operations

```bash
# Create consumer group
cargo run --bin client -- consumer create my-group

# Fetch messages for consumer group
cargo run --bin client -- consumer fetch my-group news

# Fetch with advanced options
cargo run --bin client -- consumer fetch my-group news --max-records 5 --from-offset 10 --include-headers true

# Leave consumer group  
cargo run --bin client -- consumer leave my-group
```

### Consumer Group Offset Management

```bash
# Get current offset for consumer group
cargo run --bin client -- consumer offset get my-group news

# Commit (advance) offset for consumer group
cargo run --bin client -- consumer offset commit my-group news 15
```

### Health Check

```bash
# Check server health
cargo run --bin client -- health
```

The CLI client includes comprehensive integration tests that verify all command functionality against a running server instance.

## Server Logging

The server provides different logging levels based on build type:

- **Debug builds** (`cargo run --bin server`): **TRACE** level logging
  - Detailed request/response logging for development
  - Shows all HTTP request details and internal operations
- **Release builds** (`./target/release/server`): **INFO** level logging  
  - Production-appropriate minimal logging
  - Shows server startup and critical events only

Logging level is automatically detected - no manual configuration needed.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/topics/{topic}/records` | Post a record to a topic |
| `GET` | `/topics/{topic}/messages` | Poll records from a topic |
| `POST` | `/consumer/{group_id}` | Create a new consumer group |
| `DELETE` | `/consumer/{group_id}` | Leave/delete a consumer group |
| `GET` | `/consumer/{group_id}/topics/{topic}/offset` | Get consumer group offset for a topic |
| `POST` | `/consumer/{group_id}/topics/{topic}/offset` | Update consumer group offset for a topic |
| `GET` | `/consumer/{group_id}/topics/{topic}` | Poll records for a consumer group |
| `GET` | `/health` | Health check endpoint |

## POST Records (Batch Producer)

Post one or more records to a specified topic in a single batch request.

**Endpoint:** `POST /topics/{topic}/records`

**Request Headers:**
```
Content-Type: application/json
```

**Single Record Request Body:**
```json
{
  "records": [
    {
      "key": null,
      "value": "Your message content here",
      "headers": null
    }
  ]
}
```

**Batch Request Body with Multiple Records:**
```json
{
  "records": [
    {
      "key": "user123",
      "value": "First message",
      "headers": {
        "priority": "high",
        "source": "mobile-app"
      }
    },
    {
      "key": "user456",
      "value": "Second message",
      "headers": {
        "priority": "medium",
        "source": "web-app"
      }
    }
  ]
}
```

**Success Response (200 OK):**
```json
{
  "offsets": [
    {
      "offset": 0,
      "timestamp": "2024-01-15T10:30:45Z"
    },
    {
      "offset": 1,
      "timestamp": "2024-01-15T10:30:45Z"
    }
  ]
}
```

**Error Response (400 Bad Request):**
```json
{
  "error": "Invalid request format"
}
```

**Batch Size Error Response (400 Bad Request):**
```json
{
  "error": "validation_error",
  "message": "Batch size exceeds maximum of 1000 records (got 1001)",
  "details": {
    "field": "records",
    "max_size": 1000,
    "actual_size": 1001
  }
}
```

**Record Validation Error Response (400 Bad Request):**
```json
{
  "error": "validation_error",
  "message": "Record at index 0 key exceeds maximum length of 1024 characters (got 1025)",
  "details": {
    "field": "records[0].key",
    "max_size": 1024,
    "actual_size": 1025
  }
}
```

### Example with curl

```bash
# Single record in batch format
curl -X POST http://127.0.0.1:8080/topics/news/records \
  -H "Content-Type: application/json" \
  -d '{"records": [{"key": null, "value": "Hello, World!", "headers": null}]}'

# Batch of multiple records
curl -X POST http://127.0.0.1:8080/topics/news/records \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {"key": "user123", "value": "Hello from mobile!", "headers": {"priority": "high", "device": "mobile"}},
      {"key": "user456", "value": "Another message", "headers": {"priority": "low", "device": "web"}}
    ]
  }'
```

## Poll Records

Retrieve records from a specified topic.

**Endpoint:** `GET /topics/{topic}/messages`

**Query Parameters:**
- `count` (optional): Maximum number of records to return
- `from_offset` (optional): Start polling from a specific offset (enables replay functionality)

**Success Response (200 OK):**
```json
{
  "records": [
    {
      "key": null,
      "value": "Hello, World!",
      "headers": null,
      "offset": 0,
      "timestamp": "2024-01-15T10:30:45Z"
    },
    {
      "key": "user123",
      "value": "Second record with metadata",
      "headers": {
        "priority": "high",
        "source": "mobile-app"
      },
      "offset": 1,
      "timestamp": "2024-01-15T10:30:46Z"
    }
  ],
  "count": 2
}
```

**Empty Topic Response (200 OK):**
```json
{
  "records": [],
  "count": 0
}
```

### Examples with curl

```bash
# Get all records from topic
curl http://127.0.0.1:8080/topics/news/messages

# Limit to 5 records
curl http://127.0.0.1:8080/topics/news/messages?count=5

# Replay from offset 10 (seek functionality)
curl http://127.0.0.1:8080/topics/news/messages?from_offset=10

# Replay from offset 5 with limit of 3 records
curl http://127.0.0.1:8080/topics/news/messages?from_offset=5&count=3

# Poll non-existent topic (returns empty)
curl http://127.0.0.1:8080/topics/nonexistent/messages
```

## Consumer Group Operations

Consumer groups enable coordinated consumption of records with offset tracking, similar to Kafka's consumer group model.

### Create Consumer Group

Create a new consumer group with a unique group ID.

**Endpoint:** `POST /consumer/{group_id}`

**Request Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "group_id": "my-consumer-group"
}
```

**Success Response (200 OK):**
```json
{
  "group_id": "my-consumer-group"
}
```

**Error Response (400 Bad Request):**
```json
{
  "error": "A consumer group already exists for group_id my-consumer-group"
}
```

#### Example with curl
```bash
curl -X POST http://127.0.0.1:8080/consumer/my-consumer-group \
  -H "Content-Type: application/json" \
  -d '{"group_id": "my-consumer-group"}'
```

### Delete Consumer Group

Leave and delete a consumer group.

**Endpoint:** `DELETE /consumer/{group_id}`

**Success Response (204 No Content):** *(Empty response body)*

**Error Response (404 Not Found):**
```json
{
  "error": "No consumer group exists for group_id nonexistent-group"
}
```

#### Example with curl
```bash
curl -X DELETE http://127.0.0.1:8080/consumer/my-consumer-group
```

### Get Consumer Group Offset

Retrieve the current offset for a consumer group on a specific topic.

**Endpoint:** `GET /consumer/{group_id}/topics/{topic}/offset`

**Success Response (200 OK):**
```json
{
  "group_id": "my-consumer-group",
  "topic": "news",
  "offset": 5
}
```

**Error Response (404 Not Found):**
```json
{
  "error": "No consumer group exists for group_id nonexistent-group"
}
```

#### Example with curl
```bash
curl http://127.0.0.1:8080/consumer/my-group/topics/news/offset
```

### Update Consumer Group Offset

Manually update the offset for a consumer group on a specific topic.

**Endpoint:** `POST /consumer/{group_id}/topics/{topic}/offset`

**Request Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "offset": 10
}
```

**Success Response (200 OK):** *(Empty response body)*

**Error Response (404 Not Found):**
```json
{
  "error": "No consumer group exists for group_id nonexistent-group"
}
```

#### Example with curl
```bash
curl -X POST http://127.0.0.1:8080/consumer/my-group/topics/news/offset \
  -H "Content-Type: application/json" \
  -d '{"offset": 10}'
```

### Poll Records for Consumer Group

Poll records for a consumer group starting from its current offset. The offset remains unchanged until explicitly committed using the update offset endpoint.

**Endpoint:** `GET /consumer/{group_id}/topics/{topic}`

**Query Parameters:**
- `count` (optional): Maximum number of records to return
- `from_offset` (optional): Start polling from a specific offset without updating the group's stored offset (replay mode)

**Success Response (200 OK):**
```json
{
  "records": [
    {
      "key": null,
      "value": "Record at offset 5",
      "headers": null,
      "offset": 5,
      "timestamp": "2024-01-15T10:35:20Z"
    },
    {
      "key": "user456",
      "value": "Record at offset 6",
      "headers": {
        "priority": "medium"
      },
      "offset": 6,
      "timestamp": "2024-01-15T10:35:21Z"
    }
  ],
  "count": 2,
  "new_offset": 7
}
```

**Error Response (404 Not Found):**
```json
{
  "error": "No consumer group exists for group_id nonexistent-group"
}
```

#### Examples with curl
```bash
# Poll all available records
curl http://127.0.0.1:8080/consumer/my-group/topics/news

# Limit to 3 records
curl http://127.0.0.1:8080/consumer/my-group/topics/news?count=3

# Replay from offset 5 (doesn't update group offset)
curl http://127.0.0.1:8080/consumer/my-group/topics/news?from_offset=5

# Replay from offset 10 with limit of 2 records
curl http://127.0.0.1:8080/consumer/my-group/topics/news?from_offset=10&count=2
```

## Health Check

Simple endpoint to verify server is running.

**Endpoint:** `GET /health`

**Success Response (200 OK):**
```json
{
  "status": "healthy"
}
```

### Example with curl

```bash
curl http://127.0.0.1:8080/health
```

## Data Types

### Message Record (Request)

Structure for posting new messages:

```json
{
  "key": "user123",                      // Optional: message key for routing/partitioning (max 1024 chars)
  "value": "Your message content here",  // Required: message payload (max 1MB)
  "headers": {                           // Optional: key-value metadata headers
    "priority": "high",                  // Each header value max 1024 chars
    "source": "mobile-app"
  }
}
```

### Message Response (Polling)

Structure returned when polling messages:

```json
{
  "key": "user123",                      // Optional message key
  "value": "Your message content here",  // Message payload content
  "headers": {                           // Optional metadata headers
    "priority": "high",
    "source": "mobile-app"
  },
  "offset": 0,                           // Sequential offset within topic (replaces 'id')
  "timestamp": "2024-01-15T10:30:45Z"    // ISO 8601 timestamp when message was created
}
```

### Error Responses

All API errors return OpenAPI-compliant structured error responses:

```json
{
  "error": "error_code_identifier",
  "message": "Human-readable error description", 
  "details": {
    "parameter": "field_name",
    "max_size": 1024,
    "actual_size": 1500
  }
}
```

**Error Response Fields:**
- `error` - Machine-readable error code for programmatic handling
- `message` - Human-readable error description  
- `details` - Optional additional context (field name, limits, etc.)

**Common Error Codes:**
- `validation_error` - Request validation failed (size limits, format issues)
- `invalid_parameter` - Parameter format/pattern validation failed
- `topic_not_found` - Requested topic does not exist  
- `group_not_found` - Consumer group does not exist
- `internal_error` - Server-side processing error

**HTTP Status Codes:**
- `200 OK` - Request successful, response includes data
- `201 Created` - Resource created successfully (consumer groups)
- `204 No Content` - Request successful, no response body (delete operations)
- `400 Bad Request` - Invalid request parameters or validation errors
- `404 Not Found` - Topic or consumer group not found
- `422 Unprocessable Entity` - Request format valid but semantically incorrect
- `500 Internal Server Error` - Server-side processing error

**Validation Rules:**
- Topic names: 1-255 characters, pattern `^[a-zA-Z0-9._][a-zA-Z0-9._-]*$`
- Consumer group IDs: 1-255 characters, same pattern as topics
- Record keys: Maximum 1024 characters
- Record values: Maximum 1MB (1,048,576 bytes)
- Header values: Maximum 1024 characters each (no limit on header count per record)
- Batch size: 1-1000 records per batch request
- Query parameter `max_records`: 1-10000 range
- Query parameter `timeout_ms`: 0-60000 range

## Message Ordering

- Messages within a topic are returned in FIFO (First In, First Out) order
- Message offsets are sequential within each topic, starting from 0
- Offsets are topic-specific (not globally unique across topics)
- Timestamps are ISO 8601 formatted strings representing message creation time
- No ordering guarantees exist between different topics

## Message Features

- **Keys**: Optional message keys enable routing and partitioning logic
- **Headers**: Key-value metadata for application-specific information
- **Offsets**: Sequential positioning within topics for precise message tracking
- **ISO 8601 Timestamps**: Human-readable timestamp format for better debugging
- **Replay Functionality**: Seek to specific offsets using `from_offset` parameter for both basic and consumer group polling

## Limitations

- **Persistence**: Messages are stored in memory only and lost on server restart
- **Topic Management**: Topics are created automatically when first message is posted
- **Message Limits**: 
  - Message value: Maximum 1MB per message
  - Message key: Maximum 1024 characters 
  - Header values: Maximum 1024 characters each
  - No limit on message count per topic
- **Concurrency**: Thread-safe but may have contention under high load
- **Validation**: Limited validation on key/header content formats