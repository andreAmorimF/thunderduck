# Delta Lake Integration for Thunderduck

**Date:** 2025-12-17
**Status:** Research Complete, Architecture Decision Pending
**Priority:** High (core lakehouse functionality)

## Problem Statement

Thunderduck needs to read Delta Lake tables and perform writes and merge operations on existing Delta Lake tables. While DuckDB has a Delta extension, it is **read-only** and built on the experimental Delta Kernel. This document explores alternative integration paths.

---

## Current State of DuckDB Delta Support

### DuckDB Delta Extension
- **Read support:** Yes, via `delta_scan()` function
- **Write support:** No
- **Built on:** delta-kernel-rs (experimental)
- **Platforms:** Linux AMD64/ARM64, macOS Intel/Apple Silicon only

### DuckDB Iceberg Extension (v1.4.2+)
- **Read support:** Full
- **Write support:** INSERT, UPDATE, DELETE (merge-on-read only)
- **MERGE INTO:** Not supported on Iceberg tables
- **Limitations:** No writes to partitioned/sorted tables

**Sources:**
- [DuckDB Delta Extension](https://duckdb.org/docs/stable/core_extensions/delta)
- [DuckDB Iceberg Writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb)

---

## Integration Options Analysis

### Option 1: Spark Cluster as Delta Proxy

**Concept:** Thunderduck acts as a Spark Connect client, delegating Delta read/write/merge operations to a remote Spark cluster.

#### Architecture
```
┌─────────────────┐     Spark Connect      ┌─────────────────┐
│   Thunderduck   │ ←──────────────────────→ │  Spark Cluster  │
│    (Client)     │     (Arrow IPC)        │  (Delta Lake)   │
└─────────────────┘                        └─────────────────┘
```

#### Implementation Approaches

**1a. Spark Connect Client (Protocol Level)**
- Thunderduck already understands Spark Connect protocol
- Could forward Delta operations to a real Spark cluster
- Use Delta Connect library (available for Scala/Python)

```java
// Conceptual: Forward Delta operations to Spark
if (isDeltaTableOperation(plan)) {
    return sparkConnectClient.execute(plan);
}
```

**1b. DuckDB Extension with Arrow Flight**
- Create DuckDB extension: `delta_write()`, `delta_merge()`
- Extension connects to Spark via Arrow Flight or Spark Connect
- Data transferred as Arrow batches

```sql
-- Hypothetical syntax
CALL delta_write('s3://bucket/table', (SELECT * FROM transformed));
CALL delta_merge('s3://bucket/table', source_data, 'id = source.id');
```

#### Pros
- Full Delta Lake compatibility (uses native Spark/Delta)
- Supports all Delta features: MERGE, UPDATE, DELETE, time travel, schema evolution
- Production-proven path
- Can leverage existing Spark infrastructure

#### Cons
- Requires separate Spark cluster (operational overhead)
- Network latency for data transfer
- Cost of running Spark cluster
- Complexity of managing two systems

#### Effort: Medium-High

---

### Option 2: Unity Catalog Managed Iceberg Tables

**Concept:** Use Databricks Unity Catalog with Managed Iceberg Tables. Write as Iceberg via DuckDB, Unity Catalog maintains Delta metadata automatically.

#### Architecture
```
┌─────────────────┐    Iceberg REST API    ┌─────────────────┐
│   Thunderduck   │ ←──────────────────────→ │  Unity Catalog  │
│   (DuckDB)      │                        │  (IRC Server)   │
└─────────────────┘                        └─────────────────┘
                                                   │
                                                   ▼
                                           ┌─────────────────┐
                                           │   S3/ADLS/GCS   │
                                           │  (Parquet Data) │
                                           └─────────────────┘
```

#### How It Works
1. **UniForm (Iceberg Reads):** Delta tables expose Iceberg metadata automatically
2. **Managed Iceberg Tables:** Unity Catalog manages tables as native Iceberg
3. **DuckDB Iceberg Extension:** Connects via Iceberg REST Catalog (IRC)
4. **Credential Vending:** Unity Catalog handles S3/ADLS credentials

#### Current Support (2025)
| Operation | Iceberg Tables | Delta Tables (UniForm) |
|-----------|---------------|------------------------|
| Read | ✅ Full | ✅ Full |
| Write | ✅ INSERT/UPDATE/DELETE | ❌ Read-only |
| MERGE | ❌ Not supported | ❌ Not supported |

#### Pros
- Leverages DuckDB's native Iceberg write support
- No separate compute cluster needed
- Unity Catalog provides governance, lineage, access control
- Single copy of data serves both formats

#### Cons
- **No MERGE support** in DuckDB Iceberg extension
- Requires Databricks/Unity Catalog (vendor lock-in)
- Managed Iceberg Tables still in Public Preview
- Delta clients can only read (not write) UniForm tables

#### Verdict: **Not viable** for merge operations currently

---

### Option 3: delta-rs Direct Integration

**Concept:** Use delta-rs (Rust Delta Lake implementation) directly in Thunderduck for Delta operations, bypassing DuckDB extensions.

#### Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                      Thunderduck                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │   DuckDB    │←──→│    Arrow    │←──→│    delta-rs     │ │
│  │  (Query)    │    │   Tables    │    │ (Read/Write/    │ │
│  │             │    │             │    │  Merge)         │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                      ┌─────────────────┐
                      │   S3/Local FS   │
                      │  (Delta Table)  │
                      └─────────────────┘
```

#### delta-rs Capabilities (2025)
| Operation | Support |
|-----------|---------|
| Read | ✅ Full |
| Write (append) | ✅ Full |
| Write (overwrite) | ✅ Full |
| DELETE | ✅ With predicates |
| UPDATE | ✅ Full |
| MERGE | ✅ Full |
| OPTIMIZE | ✅ Compaction + Z-order |
| VACUUM | ✅ Full |
| Time Travel | ✅ Full |

#### Implementation

**Java Integration via JNI or Process:**
```java
// Option A: JNI wrapper around delta-rs
DeltaTable table = DeltaTable.open("s3://bucket/table");
table.merge(sourceArrowTable)
    .whenMatchedUpdate(...)
    .whenNotMatchedInsert(...)
    .execute();

// Option B: Python subprocess with delta-rs
ProcessBuilder pb = new ProcessBuilder("python", "delta_merge.py", ...);
```

**Python Integration (simpler):**
```python
from deltalake import DeltaTable, write_deltalake

# Read into DuckDB
arrow_table = DeltaTable("s3://bucket/table").to_pyarrow_table()
result = duckdb.query("SELECT * FROM arrow_table WHERE ...").arrow()

# Write back to Delta
write_deltalake("s3://bucket/table", result, mode="merge",
    delta_merge_options={
        "predicate": "target.id = source.id",
        "source_alias": "source",
        "target_alias": "target"
    }
).when_matched_update_all().when_not_matched_insert_all().execute()
```

#### Pros
- Full Delta Lake support without Spark
- No JVM dependency (pure Rust)
- Arrow-native (efficient data transfer)
- Active community, frequent updates
- Works with any cloud storage

#### Cons
- Requires integration work (JNI or subprocess)
- Not as battle-tested as Spark for complex merges
- Schema evolution support still maturing
- Two-pass merge can be slower for large datasets

#### Effort: Medium

---

### Option 4: Polars as Delta Bridge

**Concept:** Use Polars (which has delta-rs integration) as an intermediary for Delta operations.

#### Polars Delta Capabilities
```python
import polars as pl

# Read Delta table
df = pl.read_delta("s3://bucket/table")

# Transform with Polars or convert to DuckDB
result = df.filter(pl.col("status") == "active")

# Write with merge
result.write_delta("s3://bucket/table", mode="merge",
    delta_merge_options={
        "predicate": "target.id = source.id",
        "source_alias": "source",
        "target_alias": "target"
    }
).when_matched_update_all().when_not_matched_insert_all().execute()
```

#### Pros
- Polars has native merge support via delta-rs
- Polars ↔ DuckDB interop via Arrow
- Pure Python/Rust stack

#### Cons
- Adds another dependency (Polars)
- Similar to delta-rs direct integration
- Schema evolution not fully supported

#### Verdict: Essentially same as Option 3, with extra layer

---

### Option 5: DuckLake with Format Conversion

**Concept:** Use DuckLake as the native format, with periodic conversion to Delta for external compatibility.

#### Architecture
```
┌─────────────────┐                        ┌─────────────────┐
│   Thunderduck   │ ───── writes ─────────→│    DuckLake     │
│   (DuckDB)      │                        │   (Native)      │
└─────────────────┘                        └─────────────────┘
                                                   │
                                           periodic export
                                                   ▼
                                           ┌─────────────────┐
                                           │   Delta Lake    │
                                           │  (via delta-rs) │
                                           └─────────────────┘
```

#### DuckLake Features (v0.3, 2025)
- Full read/write/merge support in DuckDB
- Iceberg interoperability (metadata-level copy)
- Delta Lake interoperability planned (not yet available)
- SQL-based metadata (simpler operations)

#### Pros
- Native DuckDB format (best performance)
- Full transactional support
- Simple operational model

#### Cons
- DuckLake is new (v0.3), ecosystem still developing
- Delta interoperability not yet implemented
- Creates two copies of data
- External tools can't read DuckLake natively

#### Verdict: **Not recommended** as primary Delta strategy

---

### Option 6: Hybrid Architecture (Recommended)

**Concept:** Combine multiple approaches based on operation type.

#### Architecture
```
                    ┌─────────────────────────────────────┐
                    │           Thunderduck               │
                    │  ┌─────────────────────────────┐   │
                    │  │     Delta Operation Router   │   │
                    │  └─────────────────────────────┘   │
                    │       │              │              │
                    │       ▼              ▼              │
                    │  ┌─────────┐   ┌──────────────┐   │
                    │  │ DuckDB  │   │   delta-rs   │   │
                    │  │ (Read)  │   │(Write/Merge) │   │
                    │  └─────────┘   └──────────────┘   │
                    └─────────────────────────────────────┘
                              │              │
                              └──────┬───────┘
                                     ▼
                             ┌─────────────────┐
                             │   Delta Table   │
                             │  (S3/ADLS/GCS)  │
                             └─────────────────┘
```

#### Operation Routing
| Operation | Handler | Rationale |
|-----------|---------|-----------|
| Read/Scan | DuckDB delta extension | Optimized, parallel reads |
| Append | delta-rs | Simple, fast |
| Overwrite | delta-rs | Straightforward |
| DELETE | delta-rs | Predicate-based delete |
| UPDATE | delta-rs | Full support |
| MERGE | delta-rs | Full MERGE support |
| Complex queries | DuckDB + delta-rs | Query in DuckDB, write via delta-rs |

#### Implementation
```java
public class DeltaOperationRouter {
    private final DuckDBRuntime duckdb;
    private final DeltaRsClient deltaRs;  // JNI or subprocess

    public void execute(DeltaPlan plan) {
        switch (plan.getOperationType()) {
            case READ:
                return duckdb.execute("SELECT * FROM delta_scan('" + plan.getPath() + "')");
            case MERGE:
                ArrowTable source = duckdb.execute(plan.getSourceQuery()).toArrow();
                return deltaRs.merge(plan.getPath(), source, plan.getMergeOptions());
            // ...
        }
    }
}
```

#### Pros
- Best of both worlds: DuckDB reads + delta-rs writes
- No Spark dependency
- Full Delta Lake support
- Can add Spark proxy later for complex cases

#### Cons
- Two libraries to maintain
- Need to keep delta-rs and DuckDB delta extension in sync

---

### Option 7: DuckDB Airport Extension + Arrow Flight Server

**Concept:** Use DuckDB's Airport extension to connect to an external Arrow Flight server that handles Delta Lake operations.

#### Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                      Thunderduck                                │
│  ┌─────────────────┐    Airport Extension    ┌───────────────┐ │
│  │     DuckDB      │ ←──────────────────────→│ Flight Server │ │
│  │    (Query)      │      Arrow Flight       │  (delta-rs)   │ │
│  └─────────────────┘                         └───────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                      ┌─────────────────┐
                      │   Delta Table   │
                      │  (S3/Local FS)  │
                      └─────────────────┘
```

#### Key Capabilities
- **DuckCon #6 Demo:** Query.Farm demonstrated Delta Lake write support via Airport extension
- **Python Flight Server:** Query.Farm's `python-flight-server` framework simplifies implementation
- **DoExchange Protocol:** Airport supports INSERT, UPDATE, DELETE via bidirectional Arrow Flight DoExchange
- **Streaming:** True streaming via RecordBatch iteration, no full materialization required

#### Transport Options

| Transport | Throughput | Latency | Use Case |
|-----------|------------|---------|----------|
| gRPC over TCP | ~1 GB/s | ~100μs | Cross-host, standard |
| gRPC over Unix Socket | ~1.3-1.5 GB/s | ~50μs | Same-host (20-50% faster) |
| RDMA (InfiniBand/RoCE) | ~6 GB/s | 1-2μs | HPC clusters |
| Arrow Dissociated IPC | ~6 GB/s | 1-2μs | Experimental, RDMA-based |

#### AWS EC2 Network Considerations

| Instance | EFA Support | RDMA | Best Transport |
|----------|-------------|------|----------------|
| R8g.24xlarge+ | Limited EFA | No | Unix Socket |
| R8gd | Limited EFA | No | Unix Socket |
| R8gn | Full EFA | Yes | libfabric/RDMA |
| R7gn | Full EFA | Yes | libfabric/RDMA |

**Note:** R8g/R8gd use Nitro v5, which supports EFA on 24xlarge+ but lacks full RDMA. R8gn uses Nitro v6 with full RDMA support.

#### Pros
- Native DuckDB integration via Airport extension
- High-performance Arrow Flight transport
- Streaming support (no full result materialization)
- Can leverage Unix Socket for same-host performance boost

#### Cons
- Requires running separate Flight server process
- Airport extension still evolving
- RDMA requires specific instance types

#### Effort: Medium

---

### Option 8: High-Performance Transport with libfabric/RDMA

**Concept:** For HPC/ML workloads requiring maximum throughput, use Arrow's experimental Dissociated IPC protocol with libfabric for RDMA-based data transfer.

#### What is libfabric?
- Low-level communication library providing unified API for high-performance fabrics
- Supports: InfiniBand, RoCE v2, iWARP, AWS EFA, Cray Slingshot
- Enables: Kernel bypass, zero-copy, CPU offload to network hardware
- Used by: Open MPI, MPICH, NCCL, and HPC/ML frameworks

#### Arrow Dissociated IPC Protocol
- **Status:** Experimental (Arrow 15.0+)
- **Concept:** Splits IPC messages into metadata + body, allowing body to be transferred via RDMA
- **Zero-copy:** Data buffers transferred directly between GPU/memory without CPU involvement

#### Performance Characteristics
| Metric | TCP | RDMA |
|--------|-----|------|
| Throughput | 1-2 GB/s | 6+ GB/s |
| Latency | 50-100μs | 1-2μs |
| CPU overhead | High | Near-zero |

#### AWS EFA Integration
- libfabric's `efa` provider enables RDMA-style operations on AWS
- Requires EFA-enabled instances: R8gn, R7gn, P5, Trn1, etc.
- EFA bypasses kernel network stack for HPC-class performance

#### When to Use
- Cross-node Delta Lake operations in HPC/ML clusters
- Large batch transfers (>100MB) where latency matters
- GPU-to-GPU data movement via GDS (GPU Direct Storage)

#### Verdict: **Future enhancement** for HPC deployments

---

### Option 9: S3 Mountpoint + delta-rs (Evaluated and Rejected)

**Concept:** Mount S3 bucket via AWS S3 Mountpoint (FUSE), write Delta tables through filesystem interface using AWS CRT's high-performance S3 client.

#### Viability Analysis

Four conditions must be met for this approach to work:

| Condition | Status | Details |
|-----------|--------|---------|
| 1. delta-rs local filesystem support | ⚠️ Partial | Works for single-writer only |
| 2. Optimistic concurrency control | ❌ **BLOCKER** | No atomic rename on standard S3 |
| 3. Required POSIX operations | ❌ **BLOCKER** | Missing atomic rename, file locks |
| 4. Fast mount time | ✅ Pass | Lazy-loading, not size-dependent |

#### Critical Blocker: Atomic Rename

Delta Lake transaction commits require atomic rename:

```
Delta Commit Process:
1. Write data files to table directory
2. Write temp commit file: _delta_log/.tmp_000005.json
3. ATOMIC RENAME: .tmp_000005.json → 000005.json  ← FAILS ON MOUNTPOINT
```

**S3 Mountpoint atomic rename support by storage class:**

| S3 Storage Class | Atomic Rename | Delta Lake Compatible |
|------------------|---------------|----------------------|
| S3 Standard | ❌ Rejected | ❌ No |
| S3 Intelligent-Tiering | ❌ Rejected | ❌ No |
| **S3 Express One Zone** | ✅ Supported (Mountpoint v1.19.0+) | ⚠️ Potentially |

#### POSIX Operations Gap

```
Required by Delta Lake        Mountpoint Support
─────────────────────────     ──────────────────
File read                     ✅ Supported
File write (sequential)       ✅ Supported
Directory listing             ✅ Supported
ATOMIC RENAME                 ❌ S3 Standard / ✅ Express One Zone
File locking (flock/fcntl)    ❌ Not supported
Random writes (seek+write)    ❌ Not supported
Directory rename              ❌ Not supported
```

#### S3 Express One Zone Exception

AWS added atomic rename support for S3 Express One Zone (June 2025):
- Mountpoint v1.19.0+ supports `RenameObject` API
- Single-digit millisecond latency
- **Limitation:** Same availability zone only, higher cost

**However:** delta-rs has NOT implemented S3 Express One Zone support yet. Would require:
1. Detect S3 Express One Zone storage class
2. Use native rename instead of copy-then-delete
3. Skip DynamoDB locking (unnecessary with atomic rename)
4. **Estimated effort:** 1-2 weeks

#### Performance Concerns

| Aspect | Mountpoint | Native delta-rs S3 |
|--------|------------|-------------------|
| Small file reads (tx log) | ~10x slower | Optimized |
| Metadata staleness | Up to 1 second | Immediate |
| Throughput (CRT) | ~6-12 GB/s | ~1-2 GB/s |

#### Verdict: **NOT VIABLE** for production

**Reason:** Atomic rename not supported on general purpose S3 buckets. Delta Lake cannot commit transactions through Mountpoint.

**Future possibility:** If delta-rs adds S3 Express One Zone support, this could become viable for same-AZ deployments with Mountpoint v1.19.0+.

---

## Recommendation

### Short-term (MVP): Option 3/6 - delta-rs Integration

1. Use DuckDB's delta extension for reads (`delta_scan`)
2. Use delta-rs (via Python subprocess or JNI) for writes and merges
3. Arrow as the interchange format

**Why:**
- No external infrastructure required
- Full MERGE support
- Production-ready library
- Can be implemented incrementally

### Medium-term: Option 7 - Airport Extension + Flight Server

For improved performance and native DuckDB integration:
1. Implement Python Flight server using delta-rs
2. Connect via DuckDB Airport extension
3. Use Unix Socket for same-host deployments

**Why:**
- Native DuckDB integration (no subprocess)
- Streaming support without full materialization
- ~20-50% faster than TCP via Unix Socket

### Long-term: Option 1 - Spark Connect Proxy (Optional)

For enterprise deployments that already have Spark clusters:
1. Add Spark Connect client capability
2. Route complex Delta operations to Spark
3. Keep simple operations in delta-rs

**Why:**
- Leverages existing infrastructure
- Handles edge cases delta-rs might not cover
- Familiar to Spark users

### Future: Option 8 - libfabric/RDMA (HPC Deployments)

For HPC/ML clusters requiring maximum throughput:
1. Deploy on R8gn/R7gn instances with EFA
2. Use Arrow Dissociated IPC with libfabric
3. Cross-node RDMA at 6+ GB/s

**Why:**
- 6x throughput improvement over TCP
- Sub-microsecond latency
- Zero-copy transfers

---

## High-Throughput Architecture by Operation Type

### Critical Distinction: APPEND vs MERGE

The optimal architecture differs significantly based on operation type:

| Operation | Optimal Data Format | Why |
|-----------|-------------------|-----|
| **APPEND** | Parquet files (adopt) | Skip re-serialization, parallel S3 upload |
| **MERGE INTO** | Arrow IPC (streaming/mmap) | delta-rs accepts Arrow directly, avoid double serialization |

---

## MERGE INTO: Arrow IPC Architecture (Recommended)

### Why Arrow IPC for MERGE?

MERGE operations require delta-rs to:
1. Read source data (from DuckDB)
2. Read target table (from S3)
3. Execute merge logic (match, update, insert, delete)
4. Write new Parquet files (only affected partitions)

**Key insight:** delta-rs `merge()` natively accepts `RecordBatchReader` (Arrow streaming). Using Parquet as intermediate adds unnecessary serialization:

```
PARQUET PATH (suboptimal for MERGE):
DuckDB → Arrow → Parquet serialize → NVMe → Parquet deserialize → Arrow → delta-rs

ARROW IPC PATH (optimal for MERGE):
DuckDB → Arrow → Arrow IPC (optional mmap) → delta-rs
```

### Data Transfer Overhead Comparison

| Source Size | Parquet Intermediate | Arrow IPC (mmap) | Arrow Streaming |
|-------------|---------------------|------------------|-----------------|
| 10 GB | 15-20s | 5-10s | **1-2s** |
| 100 GB | 80-150s | 20-40s | **10-20s** |
| 1 TB | 150-300s | 70-120s | Memory pressure |

### Architecture: Arrow Streaming for MERGE (< 100 GB)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              THUNDERDUCK                                    │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Runtime                                 │ │
│  │   SELECT ... FROM source_data WHERE ...                               │ │
│  │   → Arrow RecordBatch iterator (streaming)                            │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                    Arrow C Data Interface / IPC                            │
│                    (streaming batches, ~256 MB buffer)                     │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                    delta-rs Merge Engine                               │ │
│  │                                                                        │ │
│  │   dt.merge(                                                           │ │
│  │       source=record_batch_reader,  # Streaming from DuckDB           │ │
│  │       predicate="target.id = source.id",                             │ │
│  │       source_alias="source",                                          │ │
│  │       target_alias="target"                                           │ │
│  │   ).when_matched_update_all()                                         │ │
│  │    .when_not_matched_insert_all()                                     │ │
│  │    .execute()                                                          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                    delta-rs handles S3 writes internally                   │
│                    (only affected partitions rewritten)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Architecture: Arrow IPC Memory-Mapped for MERGE (100 GB - 1 TB)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              THUNDERDUCK                                    │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Runtime                                 │ │
│  │   COPY (SELECT ...) TO '/mnt/nvme/merge_source.arrow' (FORMAT arrow)  │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                    Arrow IPC file on NVMe (uncompressed)                   │
│                    Memory-mapped for near-zero-copy access                 │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                    delta-rs Merge Engine                               │ │
│  │                                                                        │ │
│  │   # Memory-map Arrow IPC file (near-zero-copy)                        │ │
│  │   mmap = pa.memory_map('/mnt/nvme/merge_source.arrow')                │ │
│  │   reader = pa.ipc.open_file(mmap).to_batches()                        │ │
│  │                                                                        │ │
│  │   dt.merge(source=reader, predicate=..., ...)                         │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Implementation: Optimal MERGE with delta-rs

```python
import pyarrow as pa
from deltalake import DeltaTable

def execute_merge(duckdb_conn, source_query: str, delta_table_uri: str,
                  merge_predicate: str, source_size_gb: float):
    """
    Execute MERGE INTO using optimal data transfer method.

    Args:
        duckdb_conn: DuckDB connection
        source_query: SQL query producing source data
        delta_table_uri: s3://bucket/path/to/delta/table
        merge_predicate: e.g., "target.id = source.id"
        source_size_gb: Estimated source data size for method selection
    """

    if source_size_gb < 100:
        # Small-medium: Arrow streaming (lowest latency)
        arrow_table = duckdb_conn.execute(source_query).fetch_arrow_table()
        reader = pa.RecordBatchReader.from_batches(
            arrow_table.schema,
            arrow_table.to_batches(max_chunksize=100_000)
        )
    else:
        # Large: Arrow IPC on NVMe with memory-mapping
        ipc_path = "/mnt/nvme/merge_source.arrow"
        duckdb_conn.execute(f"COPY ({source_query}) TO '{ipc_path}' (FORMAT arrow)")
        mmap = pa.memory_map(ipc_path)
        ipc_reader = pa.ipc.open_file(mmap)
        reader = ipc_reader.to_batches()

    # Execute merge - delta-rs handles S3 operations internally
    dt = DeltaTable(delta_table_uri)
    (dt.merge(
        source=reader,
        predicate=merge_predicate,
        source_alias="source",
        target_alias="target"
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute())

    return dt.version()
```

---

## APPEND Operations: DuckDB Parquet Adoption

### Key Insight

For append-only writes, Delta Lake = **Parquet files** + **transaction log** (`_delta_log/*.json`)

DuckDB can write optimized, partitioned Parquet files directly. Rather than re-serializing data through delta-rs, we can **adopt** DuckDB's Parquet files directly into a Delta table—no data processing, just upload and register.

### Architecture: Parallel Upload + Delta Adoption

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              THUNDERDUCK                                    │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Runtime                                 │ │
│  │                                                                        │ │
│  │  COPY (SELECT * FROM results)                                         │ │
│  │  TO '/mnt/nvme/staging/' (                                            │ │
│  │      FORMAT PARQUET,                                                   │ │
│  │      PARTITION_BY (year, month),     -- Hive-style partitioning       │ │
│  │      ROW_GROUP_SIZE 100000,          -- Optimize for Delta            │ │
│  │      COMPRESSION 'zstd'                                                │ │
│  │  );                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                    NVMe: Standard Parquet files                            │
│                    /mnt/nvme/staging/year=2024/month=01/data_0.parquet    │
│                    /mnt/nvme/staging/year=2024/month=02/data_0.parquet    │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │              Multi-threaded Upload + Delta Adoption                    │ │
│  │                                                                        │ │
│  │   Thread Pool (16-32 threads)                                         │ │
│  │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                    │ │
│  │   │Upload 0 │ │Upload 1 │ │Upload 2 │ │Upload N │                    │ │
│  │   │S3 multi-│ │S3 multi-│ │S3 multi-│ │S3 multi-│                    │ │
│  │   │part PUT │ │part PUT │ │part PUT │ │part PUT │                    │ │
│  │   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                    │ │
│  │        └───────────┴─────┬─────┴───────────┘                          │ │
│  │                          ▼                                             │ │
│  │   ┌──────────────────────────────────────────────────────────────┐   │ │
│  │   │              Delta Transaction Coordinator                    │   │ │
│  │   │  - Collects: file paths, sizes, partition values, stats      │   │ │
│  │   │  - Commits: Single atomic transaction to _delta_log/         │   │ │
│  │   └──────────────────────────────────────────────────────────────┘   │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                              ┌─────────────┐
                              │     S3      │
                              │ Delta Table │
                              └─────────────┘
```

### Why This Matters: Performance Comparison

| Approach | 1 TB Write | CPU | Memory | Notes |
|----------|------------|-----|--------|-------|
| Re-serialize via delta-rs | ~15-20 min | High | High | Data processed twice |
| **Adopt DuckDB Parquet** | ~5-7 min | Low | Low | Just upload + metadata |
| **Speedup** | **~3x faster** | | | |

### Implementation with delta-rs

```python
from deltalake import DeltaTable
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import boto3
import glob

class DeltaAdoptionWriter:
    """Adopt DuckDB-written Parquet files into Delta table."""

    def __init__(self, table_uri: str, upload_threads: int = 16):
        self.table_uri = table_uri
        self.upload_threads = upload_threads
        self.s3 = boto3.client('s3')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=16,  # Parts per file
        )

    def execute(self, staging_dir: str, mode: str = "append"):
        # 1. Discover Parquet files DuckDB wrote
        parquet_files = glob.glob(f"{staging_dir}/**/*.parquet", recursive=True)

        # 2. Upload all files in parallel (16 threads × 16 parts = 256 concurrent)
        with ThreadPoolExecutor(max_workers=self.upload_threads) as executor:
            s3_paths = list(executor.map(self._upload_file, parquet_files))

        # 3. Commit to Delta - just metadata, no data processing
        dt = DeltaTable(self.table_uri)
        dt.add_files(s3_paths)  # delta-rs extracts stats from Parquet footer

        return dt.version()
```

### Throughput Optimization

```python
# Maximum parallelism for S3 uploads
# 16 files × 16 parts per file = 256 concurrent S3 operations
# At ~10 MB/s per S3 stream ≈ 2.5 GB/s aggregate (near network line rate)

transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=8 * 1024 * 1024,   # 8 MB threshold
    multipart_chunksize=8 * 1024 * 1024,   # 8 MB parts
    max_concurrency=16,                     # 16 parts per file upload
    use_threads=True
)
```

### Same-Node Optimization

For single-node deployments (all writers on same EC2 instance):

| Transport | Use Case | Why |
|-----------|----------|-----|
| Shared memory / mmap | DuckDB → Writers | Fastest, zero-copy |
| NVMe staging | Large result sets | Handles memory pressure |
| Unix Domain Socket | If Flight needed | 20-50% faster than TCP |
| **Not needed: RDMA** | Same-node | Adds overhead, no benefit |

**Note:** RDMA (libfabric/EFA) only benefits cross-node communication. For same-node writer pools, shared memory or NVMe staging is optimal.

### When to Use Each Approach

| Scenario | Recommended Approach |
|----------|---------------------|
| Small writes (< 1 GB) | Direct delta-rs write |
| Medium writes (1-100 GB) | DuckDB Parquet + single-threaded adoption |
| Large writes (100 GB - 1 TB) | DuckDB Parquet + parallel upload + adoption |
| Very large (> 1 TB) | Multi-node with partitioned writes |

---

## EC2 Instance Selection for Delta Lake Workloads

### Important: Neither DuckDB nor delta-rs Use AWS CRT

**Both DuckDB and delta-rs lack AWS CRT optimization:**

| Component | S3 Client | CRT Support | Throughput |
|-----------|-----------|-------------|------------|
| DuckDB httpfs | httplib + OpenSSL | ❌ No | ~1-2 GB/s |
| delta-rs | object_store → reqwest | ❌ No | ~1-2 GB/s |
| s5cmd | AWS CRT | ✅ Yes | ~6-12 GB/s |
| boto3 + CRT | AWS CRT | ✅ Yes | ~4-6 GB/s |

**delta-rs architecture:**
```
delta-rs → object_store crate → reqwest → hyper
           (Apache Arrow)       (HTTP)    (low-level)
```

**Why delta-rs doesn't use CRT:**
- object_store was designed to minimize dependencies
- Implements S3 protocol directly via HTTP rather than wrapping AWS SDK
- No feature flag exists to enable CRT
- Issue [arrow-rs#5143](https://github.com/apache/arrow-rs/issues/5143) tracks potential aws-sdk-rust integration

**Path to CRT support in delta-rs (if needed):**
1. Use `mountpoint-s3-client` crate (provides S3CrtClient, but "not for general use")
2. Implement new ObjectStore backend wrapping CRT
3. Add feature flag propagation through object_store → delta-rs
4. **Estimated effort:** 2-4 weeks

**For maximum S3 performance:**
- **APPEND**: Bypass both DuckDB and delta-rs S3 → use s5cmd/boto3+CRT for uploads
- **MERGE**: Accept delta-rs throughput (~1-2 GB/s) OR implement CRT backend

### Graviton4 Instance Comparison

| Instance | vCPU | Memory | Network | NVMe Storage | Best For |
|----------|------|--------|---------|--------------|----------|
| **R8g.48xlarge** | 192 | 1,536 GiB | 50 Gbps | None (RAM staging) | Memory-heavy, moderate I/O |
| **R8gd.48xlarge** | 192 | 1,536 GiB | 50 Gbps | 11.4 TB | Large datasets, sustained |
| **R8gn.48xlarge** | 192 | 1,536 GiB | **600 Gbps**† | None (RAM staging) | Ultra-high throughput |
| **M8g.48xlarge** | 192 | 768 GiB | 50 Gbps | None (RAM staging) | Balanced workloads |
| **M8gd.48xlarge** | 192 | 768 GiB | 50 Gbps | 11.4 TB | General purpose + staging |
| **M8gn.48xlarge** | 192 | 768 GiB | **600 Gbps**† | None (RAM staging) | Network-intensive |
| **I8g.48xlarge** | 192 | 1,536 GiB | 100 Gbps | 45 TB | Storage-intensive |

†Requires 2 ENIs on separate network cards (300 Gbps each)

### Bottleneck Analysis

| Instance | Local Write | S3 Upload | Bottleneck | Effective Rate |
|----------|-------------|-----------|------------|----------------|
| R8g/M8g | RAM: ~100 GB/s | 50 Gbps = 6.25 GB/s | **Network** | 6.25 GB/s |
| R8gd/M8gd | NVMe: ~12 GB/s | 50 Gbps = 6.25 GB/s | **Network** | 6.25 GB/s |
| R8gn/M8gn | RAM: ~100 GB/s | 600 Gbps = 75 GB/s | **Network**† | 75 GB/s |
| I8g | NVMe: ~20 GB/s | 100 Gbps = 12.5 GB/s | **Network** | 12.5 GB/s |

### Instance Selection Guide

| Use Case | Recommended | Rationale |
|----------|-------------|-----------|
| MERGE < 100 GB | R8g.24xlarge | Arrow streaming, no staging needed |
| MERGE 100 GB - 1 TB | R8gd.48xlarge | NVMe for Arrow IPC staging |
| APPEND < 100 GB | R8g.24xlarge | RAM staging sufficient |
| APPEND 100 GB - 1 TB | I8g.48xlarge | 45 TB NVMe + 100 Gbps |
| Ultra-high throughput | R8gn.48xlarge | 600 Gbps (complex 2-ENI setup) |

### RAM vs NVMe Staging Decision

| Factor | RAM Staging | NVMe Staging |
|--------|-------------|--------------|
| Capacity | Limited by instance memory | 11-45 TB available |
| Speed | ~100 GB/s write | ~12-20 GB/s write |
| Memory pressure | Yes, reserve for app | No impact |
| Crash recovery | Data lost | Files persist |
| Best for | Small-medium, streaming | Large datasets |

---

## Operating System and Network Tuning

### Required Kernel Parameters

For maximum S3 throughput, configure TCP buffers in `/etc/sysctl.conf`:

```bash
# TCP buffer sizes for 100+ Gbps throughput
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

# Apply immediately
sudo sysctl -p
```

### ENA Driver Tuning

```bash
# Increase ring buffers (may cause <1ms interruption)
sudo ethtool -G eth0 rx 16384

# Enable jumbo frames within VPC (reduces packet overhead)
sudo ip link set dev eth0 mtu 9001

# Verify settings
ethtool -g eth0
ethtool -k eth0
```

### ENA Express (25 Gbps Single Flow)

For instances supporting ENA Express (R8g/R8gd/R8gn/M8g/M8gd/M8gn >12xlarge):
- Increases single TCP flow: 5 Gbps → 25 Gbps (same AZ only)
- Reduces P99.9 tail latency by up to 85%
- Zero additional cost, requires ENA driver v2.2.9+

---

## S3 Upload Optimization for delta-rs

### Concurrency Requirements

| Network BW | Target Rate | Concurrent Requests | Configuration |
|------------|-------------|---------------------|---------------|
| 50 Gbps | 6.25 GB/s | 12-15 | 4 files × 4 parts |
| 100 Gbps | 12.5 GB/s | 25-30 | 8 files × 4 parts |
| 600 Gbps | 75 GB/s | 150-180 | 32 files × 6 parts |

**Formula:** `concurrent_requests ≈ bandwidth_GB_s / 0.085`

### S3 Multipart Configuration for APPEND

```python
import boto3

# For 50 Gbps instances (R8g, R8gd, M8g, M8gd)
transfer_config_50gbps = boto3.s3.transfer.TransferConfig(
    multipart_threshold=25 * 1024 * 1024,     # 25 MB
    multipart_chunksize=25 * 1024 * 1024,     # 25 MB parts
    max_concurrency=4,                         # Per-file concurrency
    use_threads=True
)
UPLOAD_THREADS = 4  # 4 files × 4 parts = 16 concurrent uploads

# For 100 Gbps instances (I8g)
transfer_config_100gbps = boto3.s3.transfer.TransferConfig(
    multipart_threshold=25 * 1024 * 1024,
    multipart_chunksize=25 * 1024 * 1024,
    max_concurrency=8,
    use_threads=True
)
UPLOAD_THREADS = 4  # 4 files × 8 parts = 32 concurrent uploads
```

### High-Performance Upload Tools

| Tool | Speedup vs AWS CLI | Best For |
|------|-------------------|----------|
| **s5cmd** | 12-26x faster | Production bulk transfers |
| **boto3 + CRT** | 2-6x faster | SDK integration |
| **AWS CLI** | Baseline | Simple tasks |

```bash
# s5cmd for maximum throughput (APPEND operations)
s5cmd --numworkers 32 cp /mnt/nvme/staging/* s3://bucket/delta-table/

# With concurrency control
s5cmd --concurrency 16 --numworkers 32 sync /local/ s3://bucket/
```

### Avoiding S3 503 SlowDown Errors

1. **Distribute across prefixes** - 3,500 PUT/sec per prefix limit
2. **Exponential backoff with jitter** - Random delay prevents synchronized retries
3. **Gradual ramp-up** - Start at 50% target rate, increase over 15-30 seconds

```python
import random
import time

def upload_with_backoff(upload_func, max_retries=5):
    for attempt in range(max_retries):
        try:
            return upload_func()
        except ClientError as e:
            if e.response['Error']['Code'] == 'SlowDown':
                # Exponential backoff with jitter
                delay = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries exceeded")
```

---

## Performance Summary

### 1 TB Operation Time Estimates

| Operation | Instance | Method | Time |
|-----------|----------|--------|------|
| **MERGE** | R8gd.48xlarge | Arrow IPC mmap | ~3-5 min* |
| **MERGE** | R8g.48xlarge | Arrow streaming | ~2-4 min* |
| **APPEND** | R8gd.48xlarge | Parquet + s5cmd | ~3 min |
| **APPEND** | I8g.48xlarge | Parquet + s5cmd | ~1.5 min |
| **APPEND** | R8gn.48xlarge | Parquet + s5cmd | ~15 sec† |

*Plus target table read time from S3 (depends on table size)
†Requires 2-ENI configuration with 150+ concurrent uploads

### Throughput by Architecture

| Architecture | Throughput | Notes |
|--------------|------------|-------|
| DuckDB → S3 direct | ~2 GB/s | httplib bottleneck |
| DuckDB → NVMe → s5cmd → S3 | 6-12 GB/s | Bypasses DuckDB S3 |
| DuckDB → Arrow → delta-rs (MERGE) | Limited by S3 reads | delta-rs handles S3 |

---

## Implementation Plan

### Phase 1: delta-rs Read/Write (4-6 weeks)
1. Add Python delta-rs integration to Thunderduck
2. Implement `delta_write()` command via subprocess
3. Support append/overwrite modes
4. Add E2E tests for Delta write operations

### Phase 2: MERGE Support (2-3 weeks)
1. Implement `delta_merge()` command
2. Support WHEN MATCHED UPDATE/DELETE
3. Support WHEN NOT MATCHED INSERT
4. Add merge E2E tests

### Phase 3: Production Hardening (2-3 weeks)
1. JNI wrapper for delta-rs (eliminate subprocess overhead)
2. Schema evolution support
3. Concurrent write handling
4. Performance optimization

### Phase 4 (Optional): Spark Connect Proxy
1. Implement Spark Connect client in Thunderduck
2. Route complex operations to Spark
3. Fallback for unsupported delta-rs features

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| delta-rs bugs in merge | Medium | High | Comprehensive testing, fallback to Spark |
| Schema evolution issues | Medium | Medium | Validate schemas before merge |
| Performance at scale | Medium | Medium | Benchmark, optimize batch sizes |
| delta-rs API changes | Low | Medium | Pin versions, abstract interface |

---

## References

### DuckDB
- [DuckDB Delta Extension](https://duckdb.org/docs/stable/core_extensions/delta)
- [DuckDB Iceberg Writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb)
- [DuckLake 0.3](https://ducklake.select/2025/09/17/ducklake-03/)
- [DuckDB Roadmap](https://duckdb.org/roadmap)

### Delta Lake
- [delta-rs GitHub](https://github.com/delta-io/delta-rs)
- [delta-kernel-rs GitHub](https://github.com/delta-io/delta-kernel-rs)
- [Delta Connect](https://docs.delta.io/latest/delta-spark-connect.html)
- [Delta Lake without Spark](https://delta.io/blog/delta-lake-without-spark/)

### Unity Catalog / Iceberg
- [Databricks Iceberg Support](https://www.databricks.com/blog/announcing-full-apache-iceberg-support-databricks)
- [Unity Catalog Managed Tables](https://docs.databricks.com/aws/en/tables/managed)
- [UniForm (Iceberg Reads)](https://docs.databricks.com/aws/en/delta/uniform)

### Polars
- [Polars write_delta](https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_delta.html)
- [Polars Delta Merge Guide](https://stuffbyyuki.com/upsert-and-merge-with-delta-lake-tables-in-python-polars/)

### Arrow Flight / Airport
- [DuckDB Airport Extension](https://github.com/Query-farm/duckdb-airport-extension)
- [Query.Farm Python Flight Server](https://github.com/Query-farm/python-flight-server)
- [DuckCon #6 Presentations](https://duckdb.org/2024/12/05/duckcon6.html)
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)

### High-Performance Networking
- [libfabric Documentation](https://ofiwg.github.io/libfabric/)
- [Arrow Dissociated IPC](https://arrow.apache.org/docs/format/DissociatedIPC.html)
- [AWS Elastic Fabric Adapter](https://aws.amazon.com/hpc/efa/)
- [gRPC Unix Domain Sockets](https://grpc.io/docs/guides/custom-name-resolution/)

### AWS EC2 & S3 Performance
- [EC2 Instance Network Bandwidth](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html)
- [S3 Performance Guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html)
- [AWS CRT Performance Blog](https://aws.amazon.com/blogs/storage/improving-amazon-s3-throughput-for-the-aws-cli-and-boto3-with-the-aws-common-runtime/)
- [ENA Express](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html)
- [ENA Driver Tuning](https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst)
- [s5cmd GitHub](https://github.com/peak/s5cmd) - 12-26x faster than AWS CLI
- [100G Networking in AWS Deep Dive](https://toonk.io/aws-network-performance-deep-dive/index.html)

### DuckDB S3 Implementation
- [DuckDB httpfs Extension](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
- [DuckDB S3 API](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api)
- Note: DuckDB uses httplib + OpenSSL (not AWS CRT) - achieves ~30-40% of potential throughput

---

## Appendix: delta-rs Merge Example

```python
from deltalake import DeltaTable
import pyarrow as pa

# Source data (e.g., from DuckDB query result)
source_data = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "updated_at": ["2025-01-01", "2025-01-02", "2025-01-03"]
})

# Open target table
dt = DeltaTable("s3://bucket/customers")

# Perform merge
(dt.merge(
    source=source_data,
    predicate="target.id = source.id",
    source_alias="source",
    target_alias="target"
)
.when_matched_update(
    updates={
        "name": "source.name",
        "updated_at": "source.updated_at"
    }
)
.when_not_matched_insert(
    updates={
        "id": "source.id",
        "name": "source.name",
        "updated_at": "source.updated_at"
    }
)
.execute())
```
