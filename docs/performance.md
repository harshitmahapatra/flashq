# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 100.6K - 544K records/sec
- **Latency**: 1.84 ms - 9.94 ms 
- **Memory**: 6.57 MB - 32.1 MB
- **Best for**: Real-time processing, temporary queues

**File Storage - Standard** (Persistent, Stable)
- **Throughput**: 10.3K - 42.7K records/sec  
- **Latency**: 23.4 ms - 96.8 ms
- **Memory**: 12.9 MB - 54.0 MB
- **Best for**: Durable messaging, audit logs


> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-frequency trading | ✅ Sub-10ms latency | ❌ 23-97ms latency |
| Real-time analytics | ✅ 544K records/sec | ❌ 42.7K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 143.9K/sec | 6.95 ms | 13.6 MB |
| Memory | Empty topic write | 544K/sec | 1.84 ms | 6.57 MB |
| Memory | Large dataset read | 105.3K/sec | 9.50 ms | 27.6 MB |
| Memory | Large dataset write | 100.6K/sec | 9.94 ms | 27.5 MB |
| File | Empty topic read | 30.3K/sec | 33.0 ms | 25.4 MB |
| File | Empty topic write | 42.7K/sec | 23.4 ms | 12.9 MB |
| File | Large file read | 10.8K/sec | 92.7 ms | 53.9 MB |
| File | Large file write | 10.3K/sec | 96.8 ms | 54.0 MB |

## Quick Comparison

**Memory vs File Storage Performance**:
- **File Storage**: Memory is 13-53x faster

| Backend | Speed vs Memory | Latency | When to Use |
|---------|----------------|---------|-------------|
| Memory | Baseline | Sub-10ms | High throughput, temporary data |
| File | 13-53x slower | 23-97ms | Persistence needed |

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
- When you can accept 23-97ms latencies

### Capacity Planning
- **Memory**: ~6.6-32.1 MB RAM per 1000 records
- **File**: ~12.9-54.0 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks.