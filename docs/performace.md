# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 133K - 2.77M records/sec
- **Latency**: 180 µs - 820 µs 
- **Memory**: 726 KB - 3.3 MB
- **Best for**: Real-time processing, temporary queues

**File Storage** (Persistent, Slower)
- **Throughput**: 101 - 2,970 records/sec  
- **Latency**: 168 ms - 990 ms
- **Memory**: 539 KB - 4.4 MB
- **Best for**: Durable messaging, audit logs

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-ms latency | ❌ Too slow |
| Real-time analytics | ✅ 2.7M records/sec | ❌ Only 3K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Small batch (500 records) | 2.77M/sec | 180 µs | 726 KB |
| Memory | Large dataset (2K context) | 133K/sec | 752 µs | 3.3 MB |
| File | Small batch (500 records) | 2.97K/sec | 168 ms | 539 KB |
| File | Large dataset (2K context) | 101/sec | 990 ms | 4.4 MB |

## Quick Comparison

**Memory is 930x faster** than file storage for most operations.

| Metric | Memory Advantage | When to Choose File |
|--------|------------------|-------------------|
| Speed | 930-1300x faster | When you need persistence |
| Latency | Sub-millisecond | Can tolerate 100ms+ |
| Memory | Similar usage | Need crash recovery |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>100K records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage
- When you can accept ~100ms latencies

### Capacity Planning
- **Memory**: ~3-5 MB RAM per 1000 records
- **File**: ~1-4 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks