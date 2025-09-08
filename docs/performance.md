# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage - Batched** (Recommended for High Volume)
- **Throughput**: 69.2K - 2.35M records/sec 
- **Latency**: 425 µs - 14.5 ms
- **Memory**: 2.69 MB - 28.3 MB
- **Best for**: Bulk processing, high-throughput analytics

**File Storage - Batched** (Recommended for Persistent High Volume)
- **Throughput**: 50.2K - 357K records/sec
- **Latency**: 2.80 ms - 19.9 ms  
- **Memory**: 1.46 MB - 14.3 MB
- **Best for**: Persistent bulk messaging, high-volume audit logs

**Memory Storage - Single Record** (Fast, Volatile)
- **Throughput**: 74.0K - 136.5K records/sec
- **Latency**: 7.32 ms - 13.5 ms 
- **Memory**: 20.6 MB - 55.3 MB
- **Best for**: Real-time processing, individual record handling

**File Storage - Single Record** (Persistent, Stable)
- **Throughput**: 10.3K - 41.5K records/sec  
- **Latency**: 24.1 ms - 96.9 ms
- **Memory**: 7.57 MB - 32.0 MB
- **Best for**: Durable individual messaging, audit logs

**Time-Based Polling Performance**
- **Memory**: 78.4K - 88.4K records/sec (12.6 - 12.8 ms latency)
- **File**: 10.5K - 10.6K records/sec (94.1 - 95.3 ms latency)
- **Best for**: Historical data retrieval, time-range queries, replay functionality


> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-volume processing | ✅ 2.35M records/sec (batched) | ✅ 357K records/sec (batched) |
| High-frequency trading | ✅ Sub-14ms latency | ❌ 24-97ms latency |
| Real-time analytics | ✅ 136.5K records/sec (single) | ❌ 41.5K records/sec (single) |
| Time-based queries | ✅ 88.4K records/sec | ✅ 10.6K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

#### Batched Operations (Recommended)

**Write Operations**

| Storage | Operation | Batch Size | Throughput | Latency | Memory |
|---------|-----------|------------|------------|---------|--------|
| Memory | Batch write | 1K records | 2.35M/sec | 425 µs | 2.69 MB |
| Memory | Batch write | 10K records | 69.2K/sec | 14.5 ms | 26.5 MB |
| File | Batch write | 1K records | 357K/sec | 2.80 ms | 1.46 MB |
| File | Batch write | 10K records | 50.2K/sec | 19.9 ms | 14.3 MB |

**Read Operations - Offset-Based Polling**

| Storage | Operation | Batch Size | Throughput | Latency | Memory |
|---------|-----------|------------|------------|---------|--------|
| Memory | Batch read (offset) | 10K records | 126K/sec | 7.94 ms | 40.6 MB |
| File | Batch read (offset) | 10K records | 35.4K/sec | 28.3 ms | 38.0 MB |

**Read Operations - Time-Based Polling**

| Storage | Operation | Query Position | Throughput | Latency | Memory |
|---------|-----------|----------------|------------|---------|--------|
| Memory | Time-based read | From start | 79.4K/sec | 12.6 ms | 55.5 MB |
| Memory | Time-based read | From middle | 78.4K/sec | 12.8 ms | 55.5 MB |
| Memory | Time-based read | From end | 77.1K/sec | 13.0 ms | 54.3 MB |
| File | Time-based read | From start | 10.6K/sec | 94.1 ms | 32.4 MB |
| File | Time-based read | From middle | 10.6K/sec | 94.3 ms | 32.4 MB |
| File | Time-based read | From end | 10.5K/sec | 95.3 ms | 30.4 MB |

**Read Operations - Time-Based Polling (Baseline)**

| Storage | Operation | Query Position | Throughput | Latency | Memory |
|---------|-----------|----------------|------------|---------|--------|
| Memory | Time-based baseline | From start | 109K/sec | 9.16 ms | 39.3 MB |
| Memory | Time-based baseline | From middle | 138K/sec | 7.23 ms | 32.9 MB |
| Memory | Time-based baseline | From end | 181K/sec | 5.52 ms | 26.5 MB |
| File | Time-based baseline | From start | 34.3K/sec | 29.2 ms | 38.0 MB |
| File | Time-based baseline | From middle | 40.2K/sec | 24.9 ms | 26.9 MB |
| File | Time-based baseline | From end | 49.4K/sec | 20.3 ms | 15.3 MB |

#### Single Record Operations
| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 136.5K/sec | 7.32 ms | 20.6 MB |
| Memory | Empty topic write | 422K/sec | 2.37 ms | 13.5 MB |
| Memory | Large dataset read | 74.0K/sec | 13.5 ms | 55.3 MB |
| Memory | Large dataset write | 79.5K/sec | 12.6 ms | 56.6 MB |
| File | Empty topic read | 30.2K/sec | 33.1 ms | 19.3 MB |
| File | Empty topic write | 41.5K/sec | 24.1 ms | 7.57 MB |
| File | Large file read | 10.6K/sec | 93.8 ms | 32.0 MB |
| File | Large file write | 10.3K/sec | 96.9 ms | 31.1 MB |

## Quick Comparison

**Batched vs Single Record Performance**:
- **Batched operations**: 1.4-57x faster than single records
- **File batched**: Achieves 357K-50.2K records/sec (vs 10.3-41.5K single)
- **Memory batched**: Achieves 2.35M-69.2K records/sec (vs 74.0-422K single)

**Offset-Based vs Time-Based Polling**:
- **Memory**: Offset-based (126K/sec) vs Time-based (77-79K/sec) - **1.6x faster**
- **File**: Offset-based (35.4K/sec) vs Time-based (10.5-10.6K/sec) - **3.4x faster**
- **Time-based baseline**: Memory (109-181K/sec), File (34.3-49.4K/sec)

| Operation Type | Memory Performance | File Performance | When to Use |
|---------------|-------------------|------------------|-------------|
| Batched Write | 69.2K - 2.35M/sec | 50.2K - 357K/sec | Bulk processing, high volume |
| Offset-Based Read | 126K/sec | 35.4K/sec | Sequential processing, consume from last position |
| Time-Based Read | 77-79K/sec | 10.5-10.6K/sec | Historical queries, replay from timestamp |
| Single Record | 74.0K - 422K/sec | 10.3K - 41.5K/sec | Real-time individual records |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>100K records/sec)
- Time-based analytics requiring fast historical queries (77-79K records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage with time-based indexing
- When you can accept 24-97ms latencies (10.6K records/sec for time queries)

### Capacity Planning
- **Memory**: ~20.6-55.3 MB RAM per 1000 records
- **File**: ~7.57-32.0 MB RAM + disk space for persistence
- Both scale predictably with dataset size


---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks.