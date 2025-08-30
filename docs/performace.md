# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 1.4K - 6.2K records/sec
- **Latency**: 164 µs - 714 µs 
- **Memory**: 726 KB - 3.3 MB
- **Best for**: Real-time processing, temporary queues

**File Storage** (Persistent, Optimized)
- **Throughput**: 49 - 250 records/sec  
- **Latency**: 4.0 ms - 20 ms
- **Memory**: 1.3 MB - 2.3 MB
- **Best for**: Durable messaging, audit logs

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-ms latency | ❌ 4-20ms latency |
| Real-time analytics | ✅ 6K records/sec | ❌ 250 records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 1.86K/sec | 537 µs | 1.4 MB |
| Memory | Empty topic write | 6.11K/sec | 164 µs | 726 KB |
| Memory | Large dataset read | 1.35K/sec | 741 µs | 3.0 MB |
| Memory | Large dataset write | 1.40K/sec | 714 µs | 3.3 MB |
| File | Empty topic read | 226/sec | 4.4 ms | 1.3 MB |
| File | Empty topic write | 249/sec | 4.0 ms | 4.9 KB |
| File | Large file read | 51/sec | 19.6 ms | 2.3 MB |
| File | Large file write | 49/sec | 20.3 ms | 1.1 MB |

## Quick Comparison

**Memory is 8-27x faster** than file storage for most operations.

| Metric | Memory Advantage | When to Choose File |
|--------|------------------|-------------------|
| Speed | 8-27x faster | When you need persistence |
| Latency | Sub-millisecond | Can tolerate 4-20ms |
| Memory | Similar usage | Need crash recovery |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>1K records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage
- When you can accept 4-20ms latencies

### Capacity Planning
- **Memory**: ~1-3 MB RAM per 1000 records
- **File**: ~1-2 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks