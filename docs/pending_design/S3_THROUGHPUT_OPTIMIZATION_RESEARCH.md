# S3 Throughput Optimization Research for Thunderduck

**Date:** 2025-12-18
**Status:** Research Complete, Architecture Decision Pending
**Priority:** High (critical for Delta Lake write performance)

---

## Executive Summary

This document consolidates research on maximizing S3 upload throughput for Thunderduck's Delta Lake write path. Key findings:

1. **DuckDB does NOT use AWS CRT** - uses httplib + OpenSSL, achieving ~1/3 of potential network throughput
2. **EC2 instance selection matters** - R8gn/M8gn offer 600 Gbps but require 2 ENIs
3. **DuckDB Parquet adoption strategy** is optimal - bypass DuckDB's S3 layer entirely
4. **Parallel upload with s5cmd or boto3+CRT** achieves 12-26x better throughput than AWS CLI

---

## Part 1: DuckDB and delta-rs S3 Implementation Analysis

### Critical Finding: Neither Uses AWS CRT

**Both DuckDB and delta-rs lack AWS CRT optimization**, limiting S3 throughput to ~1-2 GB/s regardless of network bandwidth.

### Does DuckDB Use AWS CRT?

**Answer: NO**

DuckDB uses a custom S3 implementation:

| Component | DuckDB Implementation | AWS CRT Alternative |
|-----------|----------------------|---------------------|
| HTTP Library | httplib v0.14.3 + OpenSSL | AWS CRT C libraries |
| S3 Protocol | Custom AWS4-HMAC-SHA256 | AWS-optimized |
| Multipart Uploads | Basic support | Highly optimized |
| Parallel Transfers | Configurable threads | Built-in, aggressive |
| Throughput | ~1/3 network capacity | 2-6x improvement |

### DuckDB S3 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DuckDB S3 Access                       │
│                                                             │
│  ┌─────────────────┐         ┌─────────────────────────┐   │
│  │  aws extension  │         │    httpfs extension     │   │
│  │                 │         │                         │   │
│  │  AWS SDK C++    │         │  httplib v0.14.3        │   │
│  │  (credentials   │────────►│  + OpenSSL              │   │
│  │   only)         │         │  (actual S3 requests)   │   │
│  └─────────────────┘         └─────────────────────────┘   │
│                                                             │
│  NOT using: aws-c-s3, aws-crt-cpp, AWS CRT Transfer Manager│
└─────────────────────────────────────────────────────────────┘
```

### Why DuckDB Doesn't Use AWS CRT

1. **Simplicity**: httplib is lightweight, header-only
2. **Compatibility**: Works with S3-compatible services (MinIO, R2, GCS, lakeFS)
3. **Dependencies**: AWS CRT adds heavy C library dependencies
4. **Design Philosophy**: Core DuckDB targets zero external dependencies

### DuckDB S3 Configuration Parameters

```sql
-- Multipart upload settings
SET s3_uploader_max_parts_per_file = 10000;
SET s3_uploader_max_filesize = '5GB';
SET s3_uploader_thread_limit = 50;

-- Performance settings
SET enable_object_cache = true;
PRAGMA threads = 16;
```

### Performance Implications

| Metric | DuckDB httpfs | AWS CRT-based tools |
|--------|---------------|---------------------|
| Single-stream throughput | ~200-400 MB/s | ~1 GB/s |
| Parallel efficiency | Good | Excellent |
| Network saturation | ~30-40% | ~80-90% |
| Multipart optimization | Basic | Advanced (auto-tuning) |

---

## Part 1b: delta-rs S3 Implementation Analysis

### Does delta-rs Use AWS CRT?

**Answer: NO**

delta-rs uses the `object_store` crate (Apache Arrow project) for S3 access:

```
delta-rs
    └── object_store crate
            └── reqwest (HTTP client)
                    └── hyper (low-level HTTP)

NOT using: AWS CRT, aws-sdk-rust, aws-c-s3, mountpoint-s3-client
```

### Why delta-rs Doesn't Use CRT

1. **Design philosophy**: object_store minimizes dependencies by implementing S3 protocol directly
2. **No feature flag**: No option exists to enable CRT
3. **Community priority**: Low priority for maintainers (see [arrow-rs#5143](https://github.com/apache/arrow-rs/issues/5143))

### Performance Comparison

| Component | S3 Client | CRT | Throughput |
|-----------|-----------|-----|------------|
| DuckDB httpfs | httplib + OpenSSL | ❌ | ~1-2 GB/s |
| **delta-rs** | object_store → reqwest | ❌ | ~1-2 GB/s |
| s5cmd | AWS CRT | ✅ | ~6-12 GB/s |
| boto3 + CRT | AWS CRT | ✅ | ~4-6 GB/s |
| mountpoint-s3 | S3CrtClient | ✅ | ~6+ GB/s |

### Path to CRT Support in delta-rs

**Option 1: mountpoint-s3-client (exists but unstable)**
- Provides `S3CrtClient` built on AWS CRT
- Marked "not intended for general-purpose use"
- Interface may change without notice

**Option 2: Implement CRT ObjectStore backend**
1. Create new `ObjectStore` implementation wrapping CRT
2. Add feature flag to object_store crate
3. Propagate feature to delta-rs
4. **Estimated effort:** 2-4 weeks for experienced Rust developer

**Option 3: Wait for upstream support**
- Issue [arrow-rs#5143](https://github.com/apache/arrow-rs/issues/5143) tracks aws-sdk-rust integration
- No timeline, low priority

### Implications for MERGE Operations

Since delta-rs handles S3 internally for MERGE:
- **Reads from target table**: ~1-2 GB/s (object_store/reqwest)
- **Writes to S3**: ~1-2 GB/s (object_store/reqwest)
- **Cannot bypass** for MERGE operations

For large MERGE operations, the S3 read/write throughput becomes the bottleneck, not the DuckDB → delta-rs data transfer.

---

## Part 1c: S3 Mountpoint + delta-rs Analysis

### Concept

Use AWS S3 Mountpoint (FUSE filesystem) to mount S3 bucket locally, then write Delta tables via delta-rs's local filesystem support. This would leverage Mountpoint's AWS CRT-based high-throughput S3 client (~6-12 GB/s).

### Verdict: NOT VIABLE

**Critical Blocker:** Delta Lake requires atomic rename for transaction log commits. S3 Mountpoint does NOT support atomic rename on general purpose S3 buckets.

### Condition Analysis

| Condition | Status | Details |
|-----------|--------|---------|
| delta-rs local fs support | ⚠️ Partial | Single-writer only |
| Optimistic concurrency | ❌ **BLOCKER** | No atomic rename |
| POSIX operations | ❌ **BLOCKER** | Missing rename, locks |
| Mount time | ✅ Pass | Fast, lazy-loading |

### Why It Fails

```
Delta Commit Process:
1. Write data files
2. Write temp file: _delta_log/.tmp_000005.json
3. ATOMIC RENAME → 000005.json  ← MOUNTPOINT REJECTS THIS
```

S3 Mountpoint returns an error instead of emulating rename (which would be non-atomic and dangerous for transaction logs).

### S3 Express One Zone Exception

S3 Express One Zone (June 2025) added `RenameObject` API:
- Mountpoint v1.19.0+ supports atomic rename for Express One Zone
- **However:** delta-rs hasn't implemented support yet
- Same-AZ requirement limits flexibility
- Higher cost than S3 Standard

### Performance Trade-offs (If It Worked)

| Aspect | Mountpoint | Native delta-rs |
|--------|------------|-----------------|
| Throughput | ~6-12 GB/s (CRT) | ~1-2 GB/s (reqwest) |
| Small files | ~10x slower | Optimized |
| Metadata | Up to 1s stale | Immediate |

### Recommendation

Do NOT use S3 Mountpoint for Delta Lake operations. Use:
1. **Native delta-rs S3** - Works today with DynamoDB locking
2. **Parquet + s5cmd** - For APPEND operations requiring CRT throughput
3. **Wait for S3 Express support** - Future option if delta-rs adds support

---

## Part 2: EC2 Instance Specifications for S3 Uploads

### Graviton4 Instance Comparison

| Instance | vCPU | Memory | Network | NVMe Storage | NVMe Write | Max ENIs |
|----------|------|--------|---------|--------------|------------|----------|
| **R8g.48xlarge** | 192 | 1,536 GiB | 50 Gbps | None (RAM) | ~500 GB/s† | 15 |
| **R8gd.48xlarge** | 192 | 1,536 GiB | 50 Gbps | 11.4 TB | ~12 GB/s | 15 |
| **R8gn.48xlarge** | 192 | 1,536 GiB | **600 Gbps**‡ | None (RAM) | ~500 GB/s† | 24 |
| **M8g.48xlarge** | 192 | 768 GiB | 50 Gbps | None (RAM) | ~500 GB/s† | 15 |
| **M8gd.48xlarge** | 192 | 768 GiB | 50 Gbps | 11.4 TB | ~12 GB/s | 15 |
| **M8gn.48xlarge** | 192 | 768 GiB | **600 Gbps**‡ | None (RAM) | ~500 GB/s† | 24 |
| **I8g.48xlarge** | 192 | 1,536 GiB | 100 Gbps | 45 TB | ~20 GB/s | 15 |

†RAM disk limited by DDR5 memory bandwidth (536 GB/s theoretical)
‡Requires 2 ENIs on separate network cards (300 Gbps each)

### Bottleneck Analysis

| Instance | Local Write | S3 Upload | **Bottleneck** | Effective Rate |
|----------|-------------|-----------|----------------|----------------|
| R8g.48xlarge | RAM: ~100 GB/s | 50 Gbps = 6.25 GB/s | **Network** | 6.25 GB/s |
| R8gd.48xlarge | NVMe: ~12 GB/s | 50 Gbps = 6.25 GB/s | **Network** | 6.25 GB/s |
| R8gn.48xlarge | RAM: ~100 GB/s | 600 Gbps = 75 GB/s | **Network*** | 75 GB/s |
| I8g.48xlarge | NVMe: ~20 GB/s | 100 Gbps = 12.5 GB/s | **Network** | 12.5 GB/s |

*Requires 2 ENIs with 150+ concurrent connections

### 1 TB Upload Time Comparison

| Instance | Write Phase | Upload Phase | Total Time | Approx Cost |
|----------|-------------|--------------|------------|-------------|
| R8g.48xlarge | 10s (RAM) | 160s | **~165s** | $0.75 |
| R8gd.48xlarge | 83s (NVMe) | 160s | **~165s** | $0.85 |
| R8gn.48xlarge | 10s (RAM) | 13s | **~15s** | $0.25 |
| I8g.48xlarge | 50s (NVMe) | 80s | **~85s** | $0.60 |

---

## Part 3: Network and Kernel Tuning

### Required Kernel Parameters

```bash
# /etc/sysctl.conf - for 100+ Gbps throughput
net.core.rmem_max = 134217728          # 128 MB
net.core.wmem_max = 134217728          # 128 MB
net.core.rmem_default = 134217728
net.core.wmem_default = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 87380 134217728
net.core.netdev_max_backlog = 30000
net.ipv4.tcp_window_scaling = 1
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_sack = 1
```

### ENA Driver Tuning

```bash
# Increase ring buffers (may cause <1ms interruption)
ethtool -G eth0 rx 16384

# Enable jumbo frames within VPC
ip link set dev eth0 mtu 9001

# Check current settings
ethtool -g eth0
ethtool -k eth0
```

### ENA Express (25 Gbps single flow)

- Available on R8g/R8gd/R8gn/M8g/M8gd/M8gn (>12xlarge)
- Increases single-flow limit: 5 Gbps → 25 Gbps (same AZ)
- Zero additional cost
- Requires ENA driver v2.2.9+

---

## Part 4: S3 Upload Optimization

### Concurrency Requirements

| Network BW | GB/s | Concurrent Requests | Files × Parts |
|------------|------|---------------------|---------------|
| 50 Gbps | 6.25 GB/s | 12-15 | 4 files × 4 parts |
| 100 Gbps | 12.5 GB/s | 25-30 | 8 files × 4 parts |
| 600 Gbps | 75 GB/s | 150-180 | 32 files × 6 parts |

**Formula:** `concurrent_requests ≈ bandwidth_GB_s / 0.085`

### S3 Multipart Configuration

```python
# For 50 Gbps instances
transfer_config_50gbps = boto3.s3.transfer.TransferConfig(
    multipart_threshold=25 * 1024 * 1024,      # 25 MB
    multipart_chunksize=25 * 1024 * 1024,      # 25 MB
    max_concurrency=4,                          # Per file
    use_threads=True
)
upload_threads = 4  # 4 files × 4 parts = 16 concurrent

# For 100 Gbps instances
transfer_config_100gbps = boto3.s3.transfer.TransferConfig(
    multipart_threshold=25 * 1024 * 1024,
    multipart_chunksize=25 * 1024 * 1024,
    max_concurrency=8,                          # Per file
    use_threads=True
)
upload_threads = 4  # 4 files × 8 parts = 32 concurrent

# For 600 Gbps instances (requires 2 ENIs)
transfer_config_600gbps = boto3.s3.transfer.TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=8,
    use_threads=True
)
upload_threads_per_eni = 12  # 12 × 8 × 2 ENIs = 192 concurrent
```

### Tool Performance Comparison

| Tool | vs AWS CLI | Best For |
|------|------------|----------|
| **s5cmd** | 12-26x faster | Production bulk transfers |
| **AWS CRT (boto3)** | 2-6x faster | SDK integration |
| **AWS CLI** | Baseline | Simple tasks |
| **DuckDB httpfs** | ~0.3-0.5x | Query + light transfers |

### s5cmd Configuration

```bash
# Install
go install github.com/peak/s5cmd/v2@latest

# Optimal settings for high throughput
s5cmd --numworkers 32 cp /mnt/nvme/staging/* s3://bucket/prefix/

# With specific concurrency
s5cmd --concurrency 16 --numworkers 32 sync /local/ s3://bucket/
```

### Avoiding S3 503 SlowDown

1. **Distribute across prefixes** - 3,500 PUT/sec per prefix limit
2. **Exponential backoff with jitter** - Random delay prevents synchronized retries
3. **Gradual ramp-up** - Start at 50% target rate, increase over 15-30 seconds
4. **Monitor and adapt** - Watch for 503 responses, back off dynamically

---

## Part 5: Recommended Architecture

### Optimal Write Path (Bypassing DuckDB's S3)

Since DuckDB's S3 implementation doesn't use AWS CRT, the optimal architecture bypasses it entirely:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              THUNDERDUCK                                    │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Runtime                                 │ │
│  │                                                                        │ │
│  │  COPY (SELECT * FROM results)                                         │ │
│  │  TO '/mnt/nvme/staging/' (FORMAT PARQUET, PARTITION_BY (...))         │ │
│  │                                                                        │ │
│  │  [Uses local filesystem - full speed, no S3 bottleneck]               │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                         NVMe or RAM disk                                   │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │              Parallel Upload (s5cmd or boto3 + CRT)                   │ │
│  │                                                                        │ │
│  │  - 16-32 upload threads                                               │ │
│  │  - S3 multipart with 8-16 parts per file                             │ │
│  │  - AWS CRT for 2-6x throughput improvement                           │ │
│  │  - Near network line-rate saturation                                  │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                          ~6-75 GB/s to S3                                  │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │              Delta Transaction Coordinator (delta-rs)                  │ │
│  │                                                                        │ │
│  │  - Collects uploaded file paths                                       │ │
│  │  - Extracts Parquet stats from footers                               │ │
│  │  - Commits single atomic transaction to _delta_log/                   │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?

| Approach | Throughput | Reason |
|----------|------------|--------|
| DuckDB → S3 direct | ~2 GB/s | httplib bottleneck, no CRT |
| DuckDB → NVMe → s5cmd → S3 | ~6 GB/s (50 Gbps) | Bypasses DuckDB S3, uses CRT |
| DuckDB → NVMe → s5cmd → S3 | ~12 GB/s (100 Gbps) | I8g with optimized uploads |
| DuckDB → RAM → s5cmd → S3 | ~75 GB/s (600 Gbps) | R8gn with 2 ENIs |

### Implementation Priority

1. **Phase 1**: DuckDB → NVMe → boto3 parallel upload → delta-rs commit
2. **Phase 2**: Replace boto3 with s5cmd for 12-26x improvement
3. **Phase 3**: Add RAM disk support for instances without NVMe
4. **Phase 4**: Multi-ENI support for 600 Gbps instances

---

## Part 6: Instance Selection Guide

### Decision Matrix

| Use Case | Recommended Instance | Rationale |
|----------|---------------------|-----------|
| Standard (≤50 GB) | R8g.24xlarge | RAM staging, cost-effective |
| Large (50-500 GB) | R8gd.48xlarge | NVMe prevents memory pressure |
| Very large (500 GB-5 TB) | I8g.48xlarge | 45 TB NVMe + 100 Gbps |
| Ultra-high throughput | R8gn.48xlarge | 600 Gbps (complex 2-ENI setup) |
| Cost-optimized | R8gd.24xlarge | Good balance NVMe + network |

### RAM vs NVMe Staging

**RAM Staging (R8g, M8g, R8gn, M8gn):**
- Faster write (~100 GB/s vs ~12 GB/s)
- Limited capacity (preserve memory for OS/app)
- Best for: Small-medium datasets, high-throughput instances

**NVMe Staging (R8gd, M8gd, I8g):**
- Large capacity (11-45 TB)
- No memory pressure
- Best for: Large datasets, sustained workloads

---

## References

### AWS Documentation
- [EC2 Instance Network Bandwidth](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html)
- [S3 Performance Guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html)
- [AWS CRT Performance Blog](https://aws.amazon.com/blogs/storage/improving-amazon-s3-throughput-for-the-aws-cli-and-boto3-with-the-aws-common-runtime/)
- [ENA Express](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html)
- [ENA Driver Tuning](https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst)

### DuckDB Documentation
- [httpfs Extension](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
- [S3 API Support](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api)
- [AWS Extension](https://duckdb.org/docs/stable/core_extensions/aws)

### Tools
- [s5cmd GitHub](https://github.com/peak/s5cmd) - 12-26x faster than AWS CLI
- [delta-rs](https://github.com/delta-io/delta-rs) - Rust Delta Lake implementation

### Benchmarks
- [s5cmd vs AWS CLI Benchmarks](https://engineering.doit.com/save-time-and-money-on-s3-data-transfers-surpass-aws-cli-performance-by-80x-f20ad286d6d7)
- [100G Networking in AWS Deep Dive](https://toonk.io/aws-network-performance-deep-dive/index.html)
