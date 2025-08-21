# API Reference

Complete HTTP API documentation for the message-queue-rs server.

## Base URL

Default server runs on `http://127.0.0.1:8080`

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/topics/{topic}/messages` | Post a message to a topic |
| `GET` | `/api/topics/{topic}/messages` | Poll messages from a topic |
| `GET` | `/health` | Health check endpoint |

## POST Message

Post a new message to a specified topic.

**Endpoint:** `POST /api/topics/{topic}/messages`

**Request Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "content": "Your message content here"
}
```

**Success Response (201 Created):**
```json
{
  "id": 0,
  "timestamp": 1755635398
}
```

**Error Response (400 Bad Request):**
```json
{
  "error": "Invalid request format"
}
```

### Example with curl

```bash
curl -X POST http://127.0.0.1:8080/api/topics/news/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello, World!"}'
```

## Poll Messages

Retrieve messages from a specified topic.

**Endpoint:** `GET /api/topics/{topic}/messages`

**Query Parameters:**
- `count` (optional): Maximum number of messages to return

**Success Response (200 OK):**
```json
{
  "messages": [
    {
      "id": 0,
      "content": "Hello, World!",
      "timestamp": 1755635398
    },
    {
      "id": 1, 
      "content": "Second message",
      "timestamp": 1755635399
    }
  ],
  "count": 2
}
```

**Empty Topic Response (200 OK):**
```json
{
  "messages": [],
  "count": 0
}
```

### Examples with curl

```bash
# Get all messages from topic
curl http://127.0.0.1:8080/api/topics/news/messages

# Limit to 5 messages
curl http://127.0.0.1:8080/api/topics/news/messages?count=5

# Poll non-existent topic (returns empty)
curl http://127.0.0.1:8080/api/topics/nonexistent/messages
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

### Message

Individual message structure returned in poll responses:

```json
{
  "id": 0,                    // Unique message ID (auto-increment)
  "content": "Message text",  // User-provided message content
  "timestamp": 1755635398     // Unix timestamp when message was created
}
```

### Error Responses

All API errors return a standardized error response:

```json
{
  "error": "Human-readable error description"
}
```

Common HTTP status codes:
- `400 Bad Request` - Invalid request format or missing required fields
- `500 Internal Server Error` - Server-side processing error

## Message Ordering

- Messages within a topic are returned in FIFO (First In, First Out) order
- Message IDs are globally unique and increment across all topics
- Timestamps represent message creation time in Unix epoch seconds
- No ordering guarantees exist between different topics

## Limitations

- **Persistence**: Messages are stored in memory only and lost on server restart
- **Topic Management**: Topics are created automatically when first message is posted
- **Message Limits**: No built-in limits on message count or content size
- **Concurrency**: Thread-safe but may have contention under high load