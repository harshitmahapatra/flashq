# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage - Batched** (Recommended for High Volume)
- **Throughput**: 66.2K - 2.7M records/sec 
- **Latency**: 370 µs - 15.1 ms
- **Memory**: 2.69 MB - 28.3 MB
- **Best for**: Bulk processing, high-throughput analytics

**File Storage - Batched** (Recommended for Persistent High Volume)
- **Throughput**: 50.5K - 393K records/sec
- **Latency**: 2.41 ms - 19.8 ms  
- **Memory**: 1.46 MB - 14.3 MB
- **Best for**: Persistent bulk messaging, high-volume audit logs

**Memory Storage - Single Record** (Fast, Volatile)
- **Throughput**: 55.5K - 439K records/sec
- **Latency**: 2.28 ms - 18.0 ms 
- **Memory**: 13.5 MB - 31.3 MB
- **Best for**: Real-time processing, individual record handling

**File Storage - Single Record** (Persistent, Stable)
- **Throughput**: 11.0K - 44.0K records/sec  
- **Latency**: 22.7 ms - 91.2 ms
- **Memory**: 7.42 MB - 31.1 MB
- **Best for**: Durable individual messaging, audit logs

**Time-Based Polling Performance**
- **Memory**: 67.1K - 157K records/sec (5.15 - 13.5 ms latency)
- **File**: 10.8K - 10.9K records/sec (91.6 - 93.4 ms latency)
- **Best for**: Historical data retrieval, time-range queries, replay functionality


> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-volume processing | ✅ 2.7M records/sec (batched) | ✅ 393K records/sec (batched) |
| High-frequency trading | ✅ Sub-19ms latency | ❌ 23-91ms latency |
| Real-time analytics | ✅ 439K records/sec (single) | ❌ 44.0K records/sec (single) |
| Time-based queries | ✅ 157K records/sec | ✅ 10.9K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

#### Batched Operations (Recommended)
| Storage | Operation | Batch Size | Throughput | Latency | Memory |
|---------|-----------|------------|------------|---------|--------|
| Memory | Batch write | 1K records | 2.7M/sec | 396 µs | 2.69 MB |
| Memory | Batch write | 10K records | 66.2K/sec | 15.1 ms | 26.5 MB |
| Memory | Batch read | 10K records | 51.1K/sec | 19.6 ms | 40.6 MB |
| File | Batch write | 1K records | 393K/sec | 2.55 ms | 1.46 MB |
| File | Batch write | 10K records | 50.5K/sec | 19.8 ms | 14.3 MB |
| File | Batch read | 10K records | 34.1K/sec | 29.3 ms | 38.0 MB |

#### Single Record Operations
| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 138.4K/sec | 7.22 ms | 20.6 MB |
| Memory | Empty topic write | 439K/sec | 2.28 ms | 13.5 MB |
| Memory | Large dataset read | 74.1K/sec | 13.5 ms | 55.3 MB |
| Memory | Large dataset write | 55.5K/sec | 18.0 ms | 56.6 MB |
| File | Empty topic read | 32.5K/sec | 30.8 ms | 19.3 MB |
| File | Empty topic write | 44.0K/sec | 22.7 ms | 7.42 MB |
| File | Large file read | 11.4K/sec | 87.5 ms | 32.0 MB |
| File | Large file write | 11.0K/sec | 91.2 ms | 31.1 MB |

#### Time-Based Polling Operations
| Storage | Operation | Throughput | Latency | Memory |
|---------|-----------|------------|---------|--------|
| Memory | Time read start | 74.0K/sec | 13.5 ms | 39.3 MB |
| Memory | Time read middle | 67.1K/sec | 14.9 ms | 55.5 MB |
| Memory | Time read end | 73.5K/sec | 13.6 ms | 54.3 MB |
| File | Time read start | 10.7K/sec | 93.4 ms | 32.4 MB |
| File | Time read middle | 10.8K/sec | 93.3 ms | 32.4 MB |
| File | Time read end | 10.8K/sec | 92.5 ms | 30.4 MB |

## Quick Comparison

**Batched vs Single Record Performance**:
- **Batched operations**: 1.2-49x faster than single records
- **File batched**: Achieves 393K-50.5K records/sec (vs 11-44K single)
- **Memory batched**: Achieves 2.7M-66.2K records/sec (vs 55.5-439K single)

| Operation Type | Memory Performance | File Performance | When to Use |
|---------------|-------------------|------------------|-------------|
| Batched (Recommended) | 66.2K - 2.7M/sec | 50.5K - 393K/sec | Bulk processing, high volume |
| Single Record | 55.5K - 439K/sec | 11K - 44K/sec | Real-time individual records |
| Time-Based Polling | 67.1K - 74.0K/sec | 10.7K - 10.8K/sec | Historical queries, replay |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>100K records/sec)
- Time-based analytics requiring fast historical queries (67-74K records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage with time-based indexing
- When you can accept 23-91ms latencies (10.8K records/sec for time queries)

### Capacity Planning
- **Memory**: ~13.5-31.3 MB RAM per 1000 records
- **File**: ~7.4-32.0 MB RAM + disk space for persistence
- Both scale predictably with dataset size


---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks.