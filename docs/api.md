# API Reference

HTTP API documentation for FlashQ server (`http://127.0.0.1:8080`).

## Endpoints

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

**Create:** `POST /consumer/{group-id}` → `{"group_id": "analytics-processors"}`  
**Delete:** `DELETE /consumer/{group-id}` → 204 No Content  
**Get Offset:** `GET /consumer/{group-id}/topic/{topic}/offset`  
**Set Offset:** `POST /consumer/{group-id}/topic/{topic}/offset` with `{"offset": 10, "metadata": "Processed batch #42"}`  
**Poll by Offset:** `GET /consumer/{group-id}/topic/{topic}/record/offset?max_records=5&from_offset=10`  
**Poll by Time:** `GET /consumer/{group-id}/topic/{topic}/record/time?from_time=2025-01-01T00:00:00Z&max_records=5`

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