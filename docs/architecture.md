# Architecture

Internal architecture and design overview of FlashQ.

## System Overview

```mermaid
graph TD
    A[CLI Client] -->|HTTP| B[HTTP Server]
    C[Interactive Demo] --> D[FlashQ Core]
    B --> D
    
    D --> E[Topics Map]
    D --> F[Consumer Groups]
    
    E --> G[TopicLog Trait]
    F --> H[ConsumerGroup Trait]
    
    subgraph "Storage Backends"
        I[InMemoryTopicLog]
        J[FileTopicLog + WAL]
        K[InMemoryConsumerGroup]
        L[FileConsumerGroup]
    end
    
    G --> I
    G --> J
    H --> K
    H --> L
```

## Project Structure

**Core Components:**
- `FlashQ`: Topic-based record storage with pluggable backend support
- `Record/RecordWithOffset`: Record structures for requests/responses
- `TopicLog` trait: Storage abstraction for append-only topic logs
- `ConsumerGroup` trait: Storage abstraction for offset management
- `StorageBackend`: Factory with directory locking and backend selection
- `FileTopicLog`: File-based storage with write-ahead logging and crash recovery
- `InMemoryTopicLog`: Fast in-memory storage implementation
- Error handling: Comprehensive error types with structured logging
- HTTP server: REST API with validation and consumer groups
- CLI client: Structured command interface
- Interactive demo: Educational exploration tool

**Key Features:**
- Thread-safe concurrent access (`Arc<Mutex<>>`)
- Trait-based storage abstraction for pluggable backends
- File storage with write-ahead logging and configurable sync modes
- Directory locking prevents concurrent access to file storage
- Crash recovery from persisted write-ahead logs
- Comprehensive error handling with structured logging
- Owned data returns for improved performance and safety
- OpenAPI-compliant validation and error handling
- Comprehensive integration test coverage for HTTP and storage layers

## Data Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server  
    participant Q as FlashQ
    participant T as TopicLog
    
    C->>S: POST /topics/news/records
    S->>Q: post_record()
    Q->>T: append(record)
    T-->>Q: offset
    Q-->>S: success response
    S-->>C: {"offset": N}
    
    C->>S: GET /topics/news/records
    S->>Q: poll_records()  
    Q->>T: get_records_from_offset()
    T-->>Q: Vec<RecordWithOffset>
    Q-->>S: records array
    S-->>C: {"records": [...]}
```

**Key Principles:**
- Sequential offsets with ISO 8601 timestamps
- Append-only logs ensure FIFO ordering  
- Non-destructive polling (records persist)
- Thread-safe with `Arc<Mutex<>>`
- Write-ahead logging for durability and crash recovery

## Write-Ahead Log (WAL)

**File Storage Architecture:**
- **WAL Structure**: Records written sequentially with length prefixes for recovery
- **Commit Thresholds**: Configurable batching (default: 10 records) before sync
- **Sync Modes**: `Always`, `Periodic`, `Never` for different durability guarantees  
- **Crash Recovery**: Rebuilds state from WAL during startup, handles partial writes
- **Directory Locking**: Process-level locks prevent concurrent access to storage directory

**WAL Format:**
```
[4-byte length][8-byte offset][serialized record][4-byte length][8-byte offset][serialized record]...
```

## Design Decisions

**Architecture Choices:**
- **Storage abstraction**: Trait-based pluggable backends with memory and file implementations
- **Write-ahead logging**: Durability with configurable performance trade-offs
- **Directory locking**: Prevents data corruption from concurrent processes
- **Error handling**: Comprehensive error types with context preservation
- **Owned data**: Returns `Vec<RecordWithOffset>` vs references
- **Safe casting**: `try_into()` with bounds checking
- **Append-only logs**: Immutable history, FIFO ordering

## Performance Characteristics

**Complexity:**
- Memory storage: O(n) total records
- File storage: O(1) append, O(k) for k records read
- Post: O(1) append operation (with optional WAL sync)
- Poll: O(k) for k records
- Concurrency: Single lock bottleneck per storage backend

**Trade-offs:**
- **Memory vs File**: Speed vs persistence
- **WAL sync modes**: Durability vs performance
- **Directory locking**: Safety vs multi-process access
- **Commit thresholds**: Write batching vs durability
- FIFO ordering vs parallelism

## Storage Backend Comparison

| Feature | Memory | File |
|---------|--------|------|
| **Speed** | Fastest | Fast with WAL batching |
| **Persistence** | None | Full durability |
| **Recovery** | No | Crash recovery from WAL |
| **Concurrency** | Multi-thread only | Multi-process safe |
| **Resource Usage** | RAM only | Disk + minimal RAM |