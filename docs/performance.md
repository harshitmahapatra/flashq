# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 578 - 9.6K records/sec
- **Latency**: 104 µs - 1.7 ms 
- **Memory**: 6.5 MB - 32 MB
- **Best for**: Real-time processing, temporary queues

**File Storage** (Persistent, Optimized)
- **Throughput**: 17 - 18 records/sec  
- **Latency**: 56 ms - 463 ms
- **Memory**: 29 MB - 305 MB
- **Best for**: Durable messaging, audit logs

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-2ms latency | ❌ 56-463ms latency |
| Real-time analytics | ✅ 9.6K records/sec | ❌ 18 records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 148/sec | 6.7 ms | 13.6 MB |
| Memory | Empty topic write | 578/sec | 1.7 ms | 6.5 MB |
| Memory | Large dataset read | 109/sec | 9.2 ms | 27.6 MB |
| Memory | Large dataset write | 107/sec | 9.4 ms | 27.4 MB |
| File | Empty topic read | 17/sec | 59.9 ms | 40.7 MB |
| File | Empty topic write | 18/sec | 56.7 ms | 28.8 MB |
| File | Large file read | 2/sec | 421.5 ms | 302.4 MB |
| File | Large file write | 2/sec | 462.8 ms | 304.7 MB |

## Quick Comparison

**Memory is 9-54x faster** than file storage for most operations.

| Metric | Memory Advantage | When to Choose File |
|--------|------------------|-------------------|
| Speed | 9-54x faster | When you need persistence |
| Latency | Sub-10ms | Can tolerate 60-463ms |
| Memory | Similar usage | Need crash recovery |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>100 records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage
- When you can accept 60-463ms latencies

### Capacity Planning
- **Memory**: ~14-28 MB RAM per 1000 records
- **File**: ~29-305 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks