# API Reference

API documentation for FlashQ gRPC services.

## Protocol Support

FlashQ provides a gRPC API using Protocol Buffers over HTTP/2 (`http://127.0.0.1:50051`).

## gRPC Services

FlashQ provides three gRPC services:

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

## Data Structures

### Record
- `key`: Optional (max 1024 chars)
- `value`: Required (max 1MB)
- `headers`: Optional map (values max 1024 chars)

### RecordWithOffset
- `record`: Record
- `offset`: uint64
- `timestamp`: RFC3339 string

### Consumer Groups
Consumer groups track offsets per partition within topics. Current implementation defaults to partition 0 for backward compatibility.

**Note:** Partition-aware offset tracking is implemented internally but currently operates on partition 0 for simplicity.

## Validation Limits

- **Topics/Groups**: 1-255 chars, pattern `^[a-zA-Z0-9._][a-zA-Z0-9._-]*$`
- **Record keys**: Max 1024 chars
- **Record values**: Max 1MB
- **Header values**: Max 1024 chars each
- **Batch size**: 1-1000 records
- **Query params**: `max_records` (1-10000)

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