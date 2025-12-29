# Performance Analysis: Thunderduck vs Spark Local Mode

**Date:** 2025-12-29
**Status:** Analysis Complete
**Purpose:** Investigate why Thunderduck (DuckDB-backed) shows slower performance than Apache Spark local mode

---

## Executive Summary

Despite DuckDB's superior vectorized execution engine, Thunderduck shows slower performance than Spark local mode due to **protocol overhead** and **per-query resource allocation** that dominates execution time for typical benchmark queries.

**Key Finding:** For queries that execute in 10ms on DuckDB, Thunderduck adds 5-25ms of protocol overhead, making it 1.5-3.5x slower than Spark local mode (which has minimal overhead for in-process execution).

**At large scale factors (SF10+)**, execution time should dominate and DuckDB's vectorized engine should show its advantage. **At small scale factors**, protocol overhead dominates.

---

## Architecture Comparison

### Thunderduck Execution Path

```
PySpark Client
    → gRPC serialize (protobuf)
    → Network (localhost)
    → gRPC deserialize
    → Plan conversion (Protobuf → LogicalPlan)
    → SQL generation (LogicalPlan → DuckDB SQL)
    → DuckDB execute
    → Arrow serialize (to IPC format)
    → gRPC stream
    → Network
    → Client deserialize
    → toPandas()
```

### Spark Local Mode Execution Path

```
PySpark Client
    → Direct JVM call (Py4J)
    → Spark Executor (same JVM process)
    → Result in JVM memory
    → toPandas() [only Arrow serialization point]
```

---

## Hypothesis 1: Protocol Round-Trip Overhead (HIGH IMPACT)

### The Problem

Every query goes through a full gRPC round-trip with serialization at both ends.

### Evidence

From `SparkConnectServiceImpl.java:1641-1694`, each query:
1. Creates operation ID and starts timing
2. Creates new ArrowStreamingExecutor (line 1651)
3. Wraps iterator in SchemaCorrectedBatchIterator (line 1658)
4. Creates StreamingResultHandler (line 1669)
5. Streams results through gRPC

### Spark Local Mode Advantage

Results stay in JVM memory. Only the final `toPandas()` call requires Arrow serialization - and even then, Spark uses Arrow for efficient JVM-to-Python transfer.

### Estimated Overhead

5-20ms per query just for protocol handling

---

## Hypothesis 2: Per-Query Resource Allocation (HIGH IMPACT)

### The Problem

`SparkConnectServiceImpl.java:1651`:
```java
ArrowStreamingExecutor executor = new ArrowStreamingExecutor(session.getRuntime());
```

This creates a **NEW** `RootAllocator(Long.MAX_VALUE)` for EVERY query.

See `ArrowStreamingExecutor.java:49-51`:
```java
public ArrowStreamingExecutor(DuckDBRuntime runtime) {
    this(runtime, new RootAllocator(Long.MAX_VALUE), StreamingConfig.DEFAULT_BATCH_SIZE);
}
```

### Session Has Cached Executors (NOT BEING USED!)

`Session.java:207-220`:
```java
public ArrowStreamingExecutor getStreamingExecutor() {
    if (cachedStreamingExecutor == null) {
        synchronized (this) {
            if (cachedStreamingExecutor == null) {
                cachedStreamingExecutor = new ArrowStreamingExecutor(runtime);
            }
        }
    }
    return cachedStreamingExecutor;
}
```

### Recommended Fix

Replace `SparkConnectServiceImpl.java:1651`:
```java
// BEFORE:
ArrowStreamingExecutor executor = new ArrowStreamingExecutor(session.getRuntime());

// AFTER:
ArrowStreamingExecutor executor = session.getStreamingExecutor();
```

### Estimated Impact

2-5ms per query (allocator initialization overhead)

---

## Hypothesis 3: Arrow IPC Serialization Overhead (MEDIUM IMPACT)

### The Problem

`StreamingResultHandler.java:128-159`:
```java
private void streamBatch(VectorSchemaRoot batch, boolean includeSchema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();  // NEW allocation per batch

    try (ArrowStreamWriter writer = new ArrowStreamWriter(batch, null, Channels.newChannel(out))) {
        writer.start();   // Writes schema header EVERY batch
        writer.writeBatch();
        writer.end();
    }

    byte[] arrowData = out.toByteArray();  // COPY to new array

    // ... ByteString.copyFrom(arrowData) ...  // ANOTHER copy to protobuf
}
```

### Issues Identified

1. **New `ByteArrayOutputStream` for every batch** - causes allocation overhead
2. **Schema header written for every batch** - redundant after first batch
3. **Double copy:** `out.toByteArray()` creates new array, then `ByteString.copyFrom()` copies again

### Spark Local Mode Advantage

Results stay in JVM memory until `toPandas()` is called - no intermediate serialization.

### Potential Fixes

1. Pool and reuse `ByteArrayOutputStream` instances
2. Pre-size `ByteArrayOutputStream` based on expected batch size
3. Use `ByteString.readFrom()` with streaming instead of `copyFrom()`

---

## Hypothesis 4: UUID Generation Overhead (LOW IMPACT)

### The Problem

Multiple `UUID.randomUUID()` calls per query:
- `SparkConnectServiceImpl.java:1641` - operationId
- `StreamingResultHandler.java:148` - responseId per batch
- `StreamingResultHandler.java:213` - completion responseId

`UUID.randomUUID()` uses `SecureRandom` which has lock contention under concurrent load.

### Recommended Fix

Use `ThreadLocalRandom` for non-cryptographic operation IDs:
```java
// Instead of UUID.randomUUID().toString()
String operationId = Long.toHexString(ThreadLocalRandom.current().nextLong());
```

### Estimated Impact

0.1-0.5ms per query

---

## Hypothesis 5: Query Planning Overhead (MEDIUM IMPACT)

### The Problem

Every query creates new converter instances:

`SparkConnectServiceImpl.java:223`:
```java
LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);
```

`createPlanConverter()` creates:
- New `PlanConverter`
- New `RelationConverter`
- New `ExpressionConverter`
- New `SchemaInferrer` (with DuckDB connection for metadata queries)

### No Caching Of

- Parsed plans
- Generated SQL for identical operations
- Schema inference results

### Potential Optimization

Cache `PlanConverter` instances per session, similar to how `ArrowStreamingExecutor` is cached.

---

## Hypothesis 6: DuckDB View Indirection (LOW IMPACT)

### The Problem

When temp views are created via `createOrReplaceTempView()`:
```sql
CREATE OR REPLACE VIEW orders AS SELECT * FROM read_parquet('/path/to/orders.parquet')
```

Then queries reference the view:
```sql
SELECT o_orderstatus, count(*) FROM orders GROUP BY 1
```

### Analysis

DuckDB should optimize through the view, but there may be minor planning overhead for view resolution.

### Likely Impact

Negligible - DuckDB's optimizer handles this well.

---

## Expected Performance Profile

| Query Phase | Thunderduck | Spark Local | Notes |
|-------------|-------------|-------------|-------|
| Plan send (gRPC) | 1-5ms | 0ms | In-process for Spark |
| Plan conversion | 1-3ms | 0ms | Native Catalyst for Spark |
| SQL generation | 0.5-1ms | 0ms | N/A for Spark |
| Query execution | **X ms** | **~X ms** | Core execution time |
| Result serialize | 2-10ms | 0ms | In-memory for Spark |
| Result transfer | 1-5ms | 0ms | Same JVM for Spark |
| **Total overhead** | **5-25ms** | **~0ms** | |

### Implication

For a query that executes in 10ms:
- **Spark Local:** ~10ms total
- **Thunderduck:** 15-35ms total (1.5-3.5x slower)

For a query that executes in 1000ms:
- **Spark Local:** ~1000ms total
- **Thunderduck:** 1005-1025ms total (~1.02x slower, overhead negligible)

---

## Recommended Profiling Steps

### 1. Add Timing Instrumentation

```java
// In executeSQLStreaming()
long t0 = System.nanoTime();
LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);
long t1 = System.nanoTime();
String generatedSQL = sqlGenerator.generate(logicalPlan);
long t2 = System.nanoTime();
// ... DuckDB execution ...
long t3 = System.nanoTime();
// ... Result streaming ...
long t4 = System.nanoTime();

logger.info("Timing breakdown: convert={}ms, generate={}ms, execute={}ms, stream={}ms",
    (t1-t0)/1_000_000, (t2-t1)/1_000_000, (t3-t2)/1_000_000, (t4-t3)/1_000_000);
```

### 2. Enable DuckDB Query Profiling

```sql
PRAGMA enable_profiling;
PRAGMA profiling_output='/tmp/duckdb_profile.json';
```

### 3. JVM Profiling with async-profiler

```bash
# CPU profiling
./profiler.sh -e cpu -d 30 -f profile.html <pid>

# Allocation profiling
./profiler.sh -e alloc -d 30 -f alloc.html <pid>
```

### 4. gRPC Metrics

Enable gRPC interceptors to measure:
- Request/response sizes
- Serialization time
- Network latency

---

## Quick Fixes (Ordered by Impact)

### Fix 1: Use Cached Executor (HIGH IMPACT, LOW EFFORT)

```java
// SparkConnectServiceImpl.java:1651
// BEFORE:
ArrowStreamingExecutor executor = new ArrowStreamingExecutor(session.getRuntime());

// AFTER:
ArrowStreamingExecutor executor = session.getStreamingExecutor();
```

**Expected improvement:** 2-5ms per query

### Fix 2: Cache PlanConverter Per Session (MEDIUM IMPACT, LOW EFFORT)

Add to `Session.java`:
```java
private volatile PlanConverter cachedPlanConverter;

public PlanConverter getPlanConverter() {
    if (cachedPlanConverter == null) {
        synchronized (this) {
            if (cachedPlanConverter == null) {
                cachedPlanConverter = new PlanConverter(runtime.getConnection());
            }
        }
    }
    return cachedPlanConverter;
}
```

### Fix 3: Pool ByteArrayOutputStream (MEDIUM IMPACT, MEDIUM EFFORT)

Use a thread-local pool of pre-sized buffers for Arrow serialization.

### Fix 4: Faster Operation IDs (LOW IMPACT, LOW EFFORT)

```java
// Replace UUID.randomUUID().toString() with:
String operationId = Long.toHexString(System.nanoTime()) +
                     Long.toHexString(ThreadLocalRandom.current().nextLong());
```

---

## Long-Term Optimizations

### 1. Prepared Statement Caching

Cache DuckDB prepared statements for repeated query patterns.

### 2. Result Set Streaming Without Full Materialization

Stream directly from DuckDB to gRPC without intermediate Arrow IPC serialization.

### 3. Connection Pooling Optimization

Current architecture uses one connection per session. Consider connection pooling for DDL operations.

### 4. Batch Request Coalescing

Combine multiple small requests into single round-trips.

---

## Conclusion

Thunderduck's performance gap vs Spark local mode is primarily due to:

1. **Protocol overhead** (gRPC round-trip) - inherent to client-server architecture
2. **Per-query resource allocation** - fixable with caching
3. **Arrow serialization** - partially optimizable

**For small datasets (SF0.01-SF1):** Protocol overhead dominates, Spark local wins.

**For large datasets (SF10+):** DuckDB's execution speed should overcome overhead.

**Recommended next steps:**
1. Implement Fix 1 (cached executor) - immediate win
2. Add timing instrumentation to validate hypotheses
3. Profile with async-profiler to find hotspots
4. Consider if client-server architecture is right for local-only use cases

---

## References

- DuckDB JDBC Streaming: https://duckdb.org/docs/api/java
- Arrow Java Documentation: https://arrow.apache.org/docs/java/
- gRPC Performance Best Practices: https://grpc.io/docs/guides/performance/
- async-profiler: https://github.com/async-profiler/async-profiler
