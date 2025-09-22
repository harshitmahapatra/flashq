# API Reference

API documentation for FlashQ services supporting both HTTP REST and gRPC protocols.

## Protocol Support

FlashQ provides two API protocols:
- **HTTP REST API**: JSON over HTTP (`http://127.0.0.1:8080`)
- **gRPC API**: Protocol Buffers over HTTP/2 (`http://127.0.0.1:50051`)

Both APIs provide identical functionality with the same data structures and operations.

## HTTP REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/topic/{topic}/record` | Post records to topic |
| `POST` | `/consumer/{group-id}` | Create consumer group |
| `DELETE` | `/consumer/{group-id}` | Delete consumer group |
| `GET` | `/consumer/{group-id}/topic/{topic}/record/offset` | Poll records by offset |
| `GET` | `/consumer/{group-id}/topic/{topic}/record/time` | Poll records by time |
| `GET/POST` | `/consumer/{group-id}/topic/{topic}/offset` | Get/set consumer group offset |
| `GET` | `/topics` | Get list of all topics |
| `GET` | `/health` | Health check |

## gRPC Services

FlashQ provides three gRPC services with identical functionality to the HTTP API:

### Producer Service
- `Produce(ProduceRequest) → ProduceResponse`

### Consumer Service
- `CreateConsumerGroup(ConsumerGroupId) → ConsumerGroupResponse`
- `DeleteConsumerGroup(ConsumerGroupId) → Empty`
- `FetchByOffset(FetchByOffsetRequest) → FetchResponse`
- `FetchByTime(FetchByTimeRequest) → FetchResponse`
- `CommitOffset(CommitOffsetRequest) → CommitOffsetResponse`
- `GetConsumerGroupOffset(GetOffsetRequest) → GetOffsetResponse`
- `Subscribe(FetchByOffsetRequest) → stream RecordWithOffset` **(streaming)**

### Admin Service
- `ListTopics(Empty) → ListTopicsResponse`
- `HighWaterMark(HighWaterMarkRequest) → HighWaterMarkResponse`
- `Health(Empty) → Empty`

## POST Records

**Endpoint:** `POST /topic/{topic}/record`

**Request:**
```json
{
  "records": [
    {
      "key": "user123",              // Optional (max 1024 chars)
      "value": "Record content",    // Required (max 1MB)
      "headers": {"priority": "high"} // Optional (values max 1024 chars)
    }
  ]
}
```

**Response (200):**
```json
{
  "offset": 0,
  "timestamp": "2024-01-15T10:30:45Z"
}
```

**Validation Error (400):**
```json
{
  "error": "validation_error",
  "message": "Batch size exceeds maximum of 1000 records",
  "details": {"field": "records", "max_size": 1000, "actual_size": 1001}
}
```

## Poll Records by Offset

**Endpoint:** `GET /consumer/{group-id}/topic/{topic}/record/offset`

**Query Parameters:**
- `max_records`: Maximum records to return (default: 100, max: 10000)
- `from_offset`: Start from specific offset (overrides current offset)
- `include_headers`: Include record headers (default: true)

**Response (200):**
```json
{
  "records": [
    {
      "key": "user123",
      "value": "Hello, World!",
      "headers": {"priority": "high"},
      "offset": 0,
      "timestamp": "2024-01-15T10:30:45Z"
    }
  ],
  "next_offset": 1,
  "high_water_mark": 5,
  "lag": 4
}
```

## Poll Records by Time

**Endpoint:** `GET /consumer/{group-id}/topic/{topic}/record/time`

**Query Parameters:**
- `from_time`: RFC3339 timestamp to start from (required) (e.g., `2025-01-01T00:00:00Z`)
- `max_records`: Maximum records to return (default: 100, max: 10000)
- `include_headers`: Include record headers (default: true)

**Response (200):**
```json
{
  "records": [
    {
      "key": "user123",
      "value": "Hello, World!",
      "headers": {"priority": "high"},
      "offset": 0,
      "timestamp": "2024-01-15T10:30:45Z"
    }
  ],
  "next_offset": 1,
  "high_water_mark": 5,
  "lag": 4
}
```

## Consumer Groups

Consumer groups track offsets per partition within topics. Current implementation defaults to partition 0 for backward compatibility.

**Create:** `POST /consumer/{group-id}` → `{"group_id": "analytics-processors"}`
**Delete:** `DELETE /consumer/{group-id}` → 204 No Content
**Get Offset:** `GET /consumer/{group-id}/topic/{topic}/offset` (defaults to partition 0)
**Set Offset:** `POST /consumer/{group-id}/topic/{topic}/offset` with `{"offset": 10, "metadata": "Processed batch #42"}` (defaults to partition 0)
**Poll by Offset:** `GET /consumer/{group-id}/topic/{topic}/record/offset?max_records=5&from_offset=10`
**Poll by Time:** `GET /consumer/{group-id}/topic/{topic}/record/time?from_time=2025-01-01T00:00:00Z&max_records=5`

**Note:** Partition-aware offset tracking is implemented internally but HTTP API currently operates on partition 0 for simplicity.

## Get Topics

**Endpoint:** `GET /topics`

**Response (200):**
```json
{
  "topics": ["news", "orders", "analytics"]
}
```

## Health Check

**Endpoint:** `GET /health` → `{"status": "healthy"}`

## Validation Limits

- **Topics/Groups**: 1-255 chars, pattern `^[a-zA-Z0-9._][a-zA-Z0-9._-]*$`
- **Record keys**: Max 1024 chars
- **Record values**: Max 1MB  
- **Header values**: Max 1024 chars each
- **Batch size**: 1-1000 records
- **Query params**: `max_records` (1-10000), `timeout_ms` (0-60000)

## Error Format

```json
{
  "error": "validation_error",
  "message": "Human-readable description",
  "details": {"field": "records[0].key", "max_size": 1024}
}
```

**Status codes:** 200, 400 (validation), 404 (not found), 422 (semantic), 500 (internal)

## gRPC Client Examples

### Producer Operations
```bash
# Single record
cargo run -p flashq-grpc --bin grpc-client -- produce --topic=news --value="Hello gRPC!"

# With key and headers
cargo run -p flashq-grpc --bin grpc-client -- produce --topic=news --value="Important news" --key=breaking --header="priority=high"

# Multiple records
cargo run -p flashq-grpc --bin grpc-client -- produce --topic=news --value="News 1" --value="News 2" --value="News 3"
```

### Consumer Operations
```bash
# Consumer group lifecycle
cargo run -p flashq-grpc --bin grpc-client -- create-group --group-id=analytics
cargo run -p flashq-grpc --bin grpc-client -- delete-group --group-id=analytics

# Fetch by offset
cargo run -p flashq-grpc --bin grpc-client -- fetch-offset --group-id=analytics --topic=news --max-records=10

# Fetch by time
cargo run -p flashq-grpc --bin grpc-client -- fetch-time --group-id=analytics --topic=news --from-time="2025-01-01T00:00:00Z"

# Offset management
cargo run -p flashq-grpc --bin grpc-client -- commit-offset --group-id=analytics --topic=news --offset=42
cargo run -p flashq-grpc --bin grpc-client -- get-offset --group-id=analytics --topic=news

# Real-time streaming (Ctrl+C to stop)
cargo run -p flashq-grpc --bin grpc-client -- subscribe --group-id=analytics --topic=news
```

### Admin Operations
```bash
# List topics
cargo run -p flashq-grpc --bin grpc-client -- list-topics

# Topic high water mark
cargo run -p flashq-grpc --bin grpc-client -- high-water-mark --topic=news

# Health check
cargo run -p flashq-grpc --bin grpc-client -- connect
```

## Protocol Buffer Schema

The gRPC API uses Protocol Buffers v3 with the following key message types:

```protobuf
message Record {
  string key = 1; // optional
  string value = 2; // UTF-8 payload
  map<string, string> headers = 3; // optional
}

message RecordWithOffset {
  Record record = 1;
  uint64 offset = 2;
  string timestamp = 3; // RFC3339
}

message ProduceRequest {
  string topic = 1;
  repeated Record records = 2;
}

message FetchResponse {
  repeated RecordWithOffset records = 1;
  uint64 next_offset = 2;
  uint64 high_water_mark = 3;
  uint64 lag = 4;
}
```

Full schema available in `crates/flashq-grpc/proto/flashq.proto`.