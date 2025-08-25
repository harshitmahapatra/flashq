# API Reference

HTTP API documentation for FlashQ server (`http://127.0.0.1:8080`).

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/topics/{topic}/records` | Post records to topic |
| `GET` | `/topics/{topic}/records` | Poll records from topic |
| `POST` | `/consumer/{group_id}` | Create consumer group |
| `DELETE` | `/consumer/{group_id}` | Delete consumer group |
| `GET/POST` | `/consumer/{group_id}/topics/{topic}/offset` | Get/set consumer group offset |
| `GET` | `/consumer/{group_id}/topics/{topic}` | Poll records for consumer group |
| `GET` | `/health` | Health check |

## POST Records

**Endpoint:** `POST /topics/{topic}/records`

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
  "offsets": [
    {"offset": 0, "timestamp": "2024-01-15T10:30:45Z"}
  ]
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

## Poll Records

**Endpoint:** `GET /topics/{topic}/records`

**Query Parameters:**
- `max_records`: Maximum records to return
- `from_offset`: Start from specific offset (replay)

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

**Create:** `POST /consumer/{group_id}` with `{"group_id": "my-group"}`  
**Delete:** `DELETE /consumer/{group_id}` → 204 No Content  
**Get Offset:** `GET /consumer/{group_id}/topics/{topic}/offset`  
**Set Offset:** `POST /consumer/{group_id}/topics/{topic}/offset` with `{"offset": 10}`  
**Poll:** `GET /consumer/{group_id}/topics/{topic}?max_records=5&from_offset=10`

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