# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage** (Fast, Volatile)
- **Throughput**: 98.2K - 549K records/sec
- **Latency**: 1.8 ms - 10.2 ms 
- **Memory**: 6.5 MB - 32.1 MB
- **Best for**: Real-time processing, temporary queues

**File Storage - Standard** (Persistent, Stable)
- **Throughput**: 10.5K - 42.6K records/sec  
- **Latency**: 23.5 ms - 95.1 ms
- **Memory**: 12.8 MB - 53.9 MB
- **Best for**: Durable messaging, audit logs

**File Storage - io_uring** (Experimental, Linux-only)
- **Throughput**: 807 - 3.33K records/sec  
- **Latency**: 300ms - 1.24s
- **Memory**: 12.8 MB - 53.9 MB
- **Status**: Not optimized, slower than standard

> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage (Standard) | File Storage (io_uring) |
|----------|----------------|-------------------------|------------------------|
| High-frequency trading | ✅ Sub-11ms latency | ❌ 23-95ms latency | ❌ 300ms-1.2s latency |
| Real-time analytics | ✅ 549K records/sec | ❌ 42.6K records/sec | ❌ 3.3K records/sec |
| Message queues | ⚠️ Data loss risk | ✅ Persistent | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like | ❌ Slow, experimental |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 140.4K/sec | 7.1 ms | 13.6 MB |
| Memory | Empty topic write | 549K/sec | 1.8 ms | 6.5 MB |
| Memory | Large dataset read | 98.2K/sec | 10.2 ms | 27.6 MB |
| Memory | Large dataset write | 102K/sec | 9.8 ms | 27.4 MB |
| File (Std) | Empty topic read | 30.6K/sec | 32.7 ms | 25.3 MB |
| File (Std) | Empty topic write | 42.6K/sec | 23.5 ms | 12.8 MB |
| File (Std) | Large file read | 10.9K/sec | 91.4 ms | 53.9 MB |
| File (Std) | Large file write | 10.5K/sec | 95.2 ms | 53.9 MB |
| File (io_uring) | Empty topic read | 3.25K/sec | 308ms | 25.3 MB |
| File (io_uring) | Empty topic write | 3.33K/sec | 300ms | 12.8 MB |
| File (io_uring) | Large file read | 840/sec | 1.19s | 53.9 MB |
| File (io_uring) | Large file write | 807/sec | 1.24s | 53.9 MB |

## Quick Comparison

**Memory vs File Storage Performance**:
- **Standard File**: Memory is 13-52x faster
- **io_uring File**: Memory is 68-680x faster (io_uring needs optimization)

| Backend | Speed vs Memory | Latency | When to Use |
|---------|----------------|---------|-------------|
| Memory | Baseline | Sub-11ms | High throughput, temporary data |
| File (Standard) | 13-52x slower | 23-95ms | Persistence needed |
| File (io_uring) | 68-680x slower | 300ms-1.2s | ❌ Not recommended yet |

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
- When you can accept 23-95ms latencies

### Avoid File Storage (io_uring) For Now:
- Currently unoptimized, 68-680x slower than memory
- Use standard file storage instead until optimized

### Capacity Planning
- **Memory**: ~6.5-32 MB RAM per 1000 records
- **File**: ~13-54 MB RAM + disk space for persistence
- Both scale predictably with dataset size

---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks. io_uring implementation needs optimization.