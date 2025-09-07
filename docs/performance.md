# FlashQ Performance Overview

Quick performance comparison between memory and file storage backends.

## At a Glance

**Memory Storage - Batched** (Recommended for High Volume)
- **Throughput**: 144K - 2.1M records/sec 
- **Latency**: 475 µs - 6.91 ms
- **Memory**: 2.68 MB - 28.6 MB
- **Best for**: Bulk processing, high-throughput analytics

**File Storage - Batched** (Recommended for Persistent High Volume)
- **Throughput**: 48.1K - 374K records/sec
- **Latency**: 2.67 ms - 20.8 ms  
- **Memory**: 1.94 MB - 19.1 MB
- **Best for**: Persistent bulk messaging, high-volume audit logs

**Memory Storage - Single Record** (Fast, Volatile)
- **Throughput**: 89.2K - 471K records/sec
- **Latency**: 2.12 ms - 11.2 ms 
- **Memory**: 7.67 MB - 32.0 MB
- **Best for**: Real-time processing, individual record handling

**File Storage - Single Record** (Persistent, Stable)
- **Throughput**: 11.0K - 44.6K records/sec  
- **Latency**: 22.4 ms - 90.7 ms
- **Memory**: 7.57 MB - 34.8 MB
- **Best for**: Durable individual messaging, audit logs


> ⚠️ **Note**: We're actively working on optimizing file storage performance in upcoming releases.

## Performance Details

### When to Use Each Backend

| Scenario | Memory Storage | File Storage |
|----------|----------------|--------------|
| High-volume processing | ✅ 2.1M records/sec (batched) | ✅ 374K records/sec (batched) |
| High-frequency trading | ✅ Sub-12ms latency | ❌ 22-91ms latency |
| Real-time analytics | ✅ 471K records/sec (single) | ❌ 44.6K records/sec (single) |
| Message queues | ⚠️ Data loss risk | ✅ Persistent |
| Audit logs | ❌ No persistence | ✅ Durable |
| Development/testing | ✅ Fast iterations | ✅ Production-like |

### Benchmark Results

**Test Setup**: 1KB records with keys and headers, 100 iterations each

#### Batched Operations (Recommended)
| Storage | Operation | Batch Size | Throughput | Latency | Memory |
|---------|-----------|------------|------------|---------|--------|
| Memory | Batch write | 1K records | 2.1M/sec | 475 µs | 2.68 MB |
| Memory | Batch write | 10K records | 144K/sec | 6.91 ms | 26.4 MB |
| Memory | Batch read | 10K records | 96.5K/sec | 10.4 ms | 40.6 MB |
| File | Batch write | 1K records | 374K/sec | 2.67 ms | 1.94 MB |
| File | Batch write | 10K records | 48.1K/sec | 20.8 ms | 19.1 MB |
| File | Batch read | 10K records | 33.8K/sec | 29.6 ms | 42.8 MB |

#### Single Record Operations
| Storage | Scenario | Throughput | Latency | Memory |
|---------|----------|------------|---------|--------|
| Memory | Empty topic read | 143.9K/sec | 6.95 ms | 20.4 MB |
| Memory | Empty topic write | 471K/sec | 2.12 ms | 13.3 MB |
| Memory | Large dataset read | 89.2K/sec | 11.2 ms | 54.6 MB |
| Memory | Large dataset write | 85.3K/sec | 11.7 ms | 55.8 MB |
| File | Empty topic read | 32.3K/sec | 31.0 ms | 21.5 MB |
| File | Empty topic write | 44.6K/sec | 22.4 ms | 9.66 MB |
| File | Large file read | 11.0K/sec | 131 ms | 40.9 MB |
| File | Large file write | 11.0K/sec | 90.7 ms | 40.5 MB |

## Quick Comparison

**Batched vs Single Record Performance**:
- **Batched operations**: 4-44x faster than single records
- **File batched**: Achieves 374K-48.1K records/sec (vs 11-44.6K single)
- **Memory batched**: Achieves 2.1M-144K records/sec (vs 85-471K single)

| Operation Type | Memory Performance | File Performance | When to Use |
|---------------|-------------------|------------------|-------------|
| Batched (Recommended) | 144K - 2.1M/sec | 48.1K - 374K/sec | Bulk processing, high volume |
| Single Record | 85K - 471K/sec | 11K - 44.6K/sec | Real-time individual records |

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
- When you can accept 22-131ms latencies

### Capacity Planning
- **Memory**: ~7.7-32.0 MB RAM per 1000 records
- **File**: ~7.6-40.9 MB RAM + disk space for persistence
- Both scale predictably with dataset size


---

**Benchmark Environment**: Linux WSL2 with 1KB test records  
**Framework**: Divan with full memory profiling  
**Note**: File storage uses optimal settings (no fsync) for these benchmarks.