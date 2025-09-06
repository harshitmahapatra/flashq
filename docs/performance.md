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

**File Storage - io_uring** (Experimental, Linux-only)
- **Throughput**: 797 - 3.33K records/sec  
- **Latency**: 300ms - 1.26s
- **Memory**: 12.9 MB - 54.0 MB
- **Status**: Not optimized, slower than standard

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage (Standard) | File Storage (io_uring) |
|----------|----------------|-------------------------|------------------------|
| High-frequency trading | ✅ Sub-10ms latency | ❌ 23-97ms latency | ❌ 300ms-1.3s latency |
| Real-time analytics | ✅ 544K records/sec | ❌ 42.7K records/sec | ❌ 3.3K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like | ❌ Slow, experimental |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 143.9K/sec | 6.95 ms | 13.6 MB |
| Memory | Empty topic write | 544K/sec | 1.84 ms | 6.57 MB |
| Memory | Large dataset read | 105.3K/sec | 9.50 ms | 27.6 MB |
| Memory | Large dataset write | 100.6K/sec | 9.94 ms | 27.5 MB |
| File (Std) | Empty topic read | 30.3K/sec | 33.0 ms | 25.4 MB |
| File (Std) | Empty topic write | 42.7K/sec | 23.4 ms | 12.9 MB |
| File (Std) | Large file read | 10.8K/sec | 92.7 ms | 53.9 MB |
| File (Std) | Large file write | 10.3K/sec | 96.8 ms | 54.0 MB |
| File (io_uring) | Empty topic read | 3.23K/sec | 310ms | 25.4 MB |
| File (io_uring) | Empty topic write | 3.33K/sec | 301ms | 12.9 MB |
| File (io_uring) | Large file read | 840/sec | 1.19s | 53.9 MB |
| File (io_uring) | Large file write | 797/sec | 1.26s | 54.0 MB |

## Quick Comparison

**Memory vs File Storage Performance**:
- **Standard File**: Memory is 13-53x faster
- **io_uring File**: Memory is 68-683x faster (io_uring needs optimization)

| Backend | Speed vs Memory | Latency | When to Use |
|---------|----------------|---------|-------------|
| Memory | Baseline | Sub-10ms | High throughput, temporary data |
| File (Standard) | 13-53x slower | 23-97ms | Persistence needed |
| File (io_uring) | 68-683x slower | 300ms-1.3s | ❌ Not recommended yet |

## Production Guidance

### Choose Memory Storage For:
- Real-time applications (trading, gaming)
- High-throughput processing (>100K records/sec)
- Temporary data that doesn't need persistence
- Development and testing environments

### Choose File Storage (Standard) For:  
- Message queues that must survive restarts
- Audit logs and compliance requirements
- Long-term data storage
- When you can accept 23-97ms latencies

### Avoid File Storage (io_uring) For Now:
- Currently unoptimized, 68-683x slower than memory
- Use standard file storage instead until optimized

### Capacity Planning
- **Memory**: ~6.6-32.1 MB RAM per 1000 records
- **File**: ~12.9-54.0 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks. io_uring implementation needs optimization.