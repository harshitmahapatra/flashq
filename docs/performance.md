# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 99.4K - 547K records/sec
- **Latency**: 1.8 ms - 10.1 ms 
- **Memory**: 6.5 MB - 32.1 MB
- **Best for**: Real-time processing, temporary queues

**File Storage** (Persistent, Optimized)
- **Throughput**: 10K - 40.8K records/sec  
- **Latency**: 24.5 ms - 99.7 ms
- **Memory**: 12.8 MB - 53.9 MB
- **Best for**: Durable messaging, audit logs

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-11ms latency | ❌ 25-100ms latency |
| Real-time analytics | ✅ 547K records/sec | ❌ 40.8K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 143.5K/sec | 7.0 ms | 13.6 MB |
| Memory | Empty topic write | 547K/sec | 1.8 ms | 6.5 MB |
| Memory | Large dataset read | 99.4K/sec | 10.1 ms | 27.6 MB |
| Memory | Large dataset write | 100K/sec | 10.0 ms | 27.4 MB |
| File | Empty topic read | 30K/sec | 33.3 ms | 24.7 MB |
| File | Empty topic write | 40.8K/sec | 24.5 ms | 12.8 MB |
| File | Large file read | 10.5K/sec | 95.4 ms | 53.7 MB |
| File | Large file write | 10K/sec | 99.7 ms | 53.9 MB |

## Quick Comparison

**Memory is 13-55x faster** than file storage for most operations.

| Metric | Memory Advantage | When to Choose File |
|--------|------------------|-------------------|
| Speed | 13-55x faster | When you need persistence |
| Latency | Sub-11ms | Can tolerate 25-100ms |
| Memory | Lower peak usage | Need crash recovery |

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
- When you can accept 25-100ms latencies

### Capacity Planning
- **Memory**: ~6.5-32 MB RAM per 1000 records
- **File**: ~13-54 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks