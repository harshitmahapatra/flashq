# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 578 - 108 records/sec
- **Latency**: 1.7 ms - 9.3 ms 
- **Memory**: 6.5 MB - 27.4 MB
- **Best for**: Real-time processing, temporary queues

**File Storage** (Persistent, Optimized)
- **Throughput**: 15.1 - 45.5 records/sec  
- **Latency**: 18.2 ms - 66.2 ms
- **Memory**: 12.5 MB - 52.5 MB
- **Best for**: Durable messaging, audit logs

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-10ms latency | ❌ 18-66ms latency |
| Real-time analytics | ✅ 578 records/sec | ❌ 45 records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 147/sec | 6.8 ms | 13.6 MB |
| Memory | Empty topic write | 578/sec | 1.7 ms | 6.5 MB |
| Memory | Large dataset read | 108/sec | 9.3 ms | 27.6 MB |
| Memory | Large dataset write | 109/sec | 9.1 ms | 27.4 MB |
| File | Empty topic read | 45.5/sec | 22.0 ms | 24.4 MB |
| File | Empty topic write | 54.9/sec | 18.2 ms | 12.5 MB |
| File | Large file read | 14.9/sec | 67.2 ms | 73.1 MB |
| File | Large file write | 15.1/sec | 66.2 ms | 52.5 MB |

## Quick Comparison

**Memory is 3-10x faster** than file storage for most operations.

| Metric | Memory Advantage | When to Choose File |
|--------|------------------|-------------------|
| Speed | 3-10x faster | When you need persistence |
| Latency | Sub-10ms | Can tolerate 18-67ms |
| Memory | Comparable usage | Need crash recovery |

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
- When you can accept 18-67ms latencies

### Capacity Planning
- **Memory**: ~6.5-28 MB RAM per 1000 records
- **File**: ~12-73 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks