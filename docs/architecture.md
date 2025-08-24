# Architecture

Internal architecture and design overview of FlashQ.

## System Overview

```mermaid
graph TD
    A[CLI Client] -->|HTTP| B[HTTP Server]
    C[Interactive Demo] --> D[MessageQueue]
    B --> D
    D --> E[TopicLog 1]
    D --> F[TopicLog 2]
    D --> G[TopicLog N]
    B --> H[Consumer Groups]
    H --> I[Group Offsets]
    
    subgraph "Storage Layer"
        E --> J[Records 0,1,2...]
        F --> K[Records 0,1,2...]
        G --> L[Records 0,1,2...]
    end
```

## Project Structure

**Core Components:**
- `MessageQueue`: Topic-based record storage with offset tracking
- `Record/RecordWithOffset`: Message structures for requests/responses  
- HTTP server: REST API with validation and consumer groups
- CLI client: Structured command interface
- Interactive demo: Educational exploration tool

**Key Features:**
- Thread-safe concurrent access (`Arc<Mutex<>>`)
- OpenAPI-compliant validation and error handling
- Comprehensive integration test coverage

## Data Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server  
    participant Q as MessageQueue
    participant T as TopicLog
    
    C->>S: POST /topics/news/records
    S->>Q: append_records()
    Q->>T: add records with offsets
    T-->>Q: [offset_0, offset_1]
    Q-->>S: success response
    S-->>C: {"offsets": [...]}
    
    C->>S: GET /topics/news/messages
    S->>Q: poll_records()  
    Q->>T: get records from offset
    T-->>Q: [RecordWithOffset...]
    Q-->>S: records array
    S-->>C: {"records": [...]}
```

**Key Principles:**
- Sequential offsets with ISO 8601 timestamps
- Append-only logs ensure FIFO ordering  
- Non-destructive polling (records persist)
- Thread-safe with `Arc<Mutex<>>`

## Design Decisions

```mermaid
graph LR
    A[Record] -->|append| B[TopicLog]
    B --> C[Vec RecordWithOffset]
    B --> D[next_offset]
    
    E[Consumer Group] --> F[HashMap topic→offset]
    
    subgraph "Thread Safety"
        G[Arc Mutex MessageQueue] 
        H[Arc Mutex ConsumerGroups]
    end
    
    subgraph "Storage"
        B
        I[Topic 1]
        J[Topic 2]
        K[Topic N]
    end
```

**Architecture Choices:**
- **Append-only logs**: Immutable history, FIFO ordering
- **Coarse-grained locking**: Single mutex for simplicity vs performance
- **In-memory storage**: Fast access, no persistence
- **OpenAPI validation**: Structured errors with semantic codes

## Performance Characteristics

**Complexity:**
- Memory: O(n) total records
- Post: O(1) append operation
- Poll: O(k) for k records
- Concurrency: Single lock bottleneck

**Trade-offs:**
- Simplicity vs scalability
- Memory speed vs persistence  
- FIFO ordering vs parallelism