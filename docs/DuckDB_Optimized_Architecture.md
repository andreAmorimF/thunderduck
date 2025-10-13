# High-Performance DuckDB-Optimized Architecture

## Executive Summary

This document refines the original design to maximize performance on DuckDB while maintaining simplicity, elegance, and future extensibility to Spark Connect server mode. Focus is on in-process, single-node, multi-core execution optimized for Intel and AWS Graviton (i8g, r8g, i4i) instances.

## Key Design Principles

1. **Performance First**: Zero-copy data paths, vectorized execution, SIMD optimization
2. **Simplicity**: Direct DuckDB targeting initially, abstract to Calcite only when needed
3. **Elegance**: Clean layer separation, minimal abstraction overhead
4. **Future-Ready**: Architecture supports both embedded and Spark Connect modes

## Recommended Architecture: Three-Layer Design

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: Spark API Facade (Lazy Plan Builder)             │
│  - DataFrame/Dataset API                                    │
│  - Column expressions                                       │
│  - Builds logical plan tree (NOT Calcite RelNode)          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Layer 2: Translation & Optimization Engine                 │
│  - Logical Plan → DuckDB SQL (direct, no Calcite initially)│
│  - Query optimization (pushdown, fusion)                    │
│  - Type mapping & function registry                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Layer 3: DuckDB Execution Engine (In-Process)             │
│  - Vectorized execution                                     │
│  - Multi-core parallelism                                   │
│  - Zero-copy Arrow interop                                  │
│  - SIMD-optimized operations                               │
└─────────────────────────────────────────────────────────────┘
```

## Major Design Changes from Original

### 1. **Skip Calcite Initially** (Add Later as Abstraction)

**Why:**
- DuckDB has excellent SQL dialect and optimizer
- Calcite adds ~5-15% overhead for single-engine targeting
- Simpler codebase, faster development
- Can add Calcite later as abstraction layer for multi-engine support

**When to Add Calcite:**
- When targeting multiple SQL engines (Postgres, ClickHouse, etc.)
- When needing advanced cross-engine optimizations
- Phase 2 or 3, not Phase 1

**Architecture Evolution:**
```java
// Phase 1: Direct DuckDB translation
LogicalPlan → DuckDB SQL → DuckDB Execution

// Phase 2: Abstract with Calcite (if needed)
LogicalPlan → Calcite RelNode → DuckDB SQL → DuckDB Execution
           ↘ PostgreSQL SQL → Postgres Execution
           ↘ Substrait → Multi-Engine
```

### 2. **Zero-Copy Arrow Integration**

**Critical for Performance:**
DuckDB has native Arrow support - use it everywhere!

```java
public class ArrowDataFrame implements DataFrame {
    private final DuckDBConnection connection;
    private final LogicalPlan plan;
    private ArrowStreamReader arrowReader; // Reusable

    @Override
    public List<Row> collect() {
        // Zero-copy path: DuckDB → Arrow → Row wrapper
        try (ArrowStreamReader reader = executeToArrow(plan)) {
            return new ArrowRowIterator(reader); // No materialization
        }
    }

    @Override
    public void write(String path, String format) {
        if ("parquet".equals(format)) {
            // Direct path: DuckDB → Parquet (no intermediate copy)
            String sql = translateToSQL(plan);
            connection.execute("COPY (" + sql + ") TO '" + path + "' (FORMAT PARQUET)");
        }
    }
}
```

### 3. **Hardware-Aware Execution**

#### Intel-Specific Optimizations
```java
public class ExecutionConfig {
    // Detect Intel vs Graviton at runtime
    private static final boolean IS_ARM = System.getProperty("os.arch").contains("aarch64");
    private static final boolean HAS_AVX512 = detectAVX512(); // Intel i4i
    private static final boolean HAS_AVX2 = detectAVX2();     // Intel i8g/r8g

    public static DuckDBConfig createOptimizedConfig() {
        var config = DuckDBConfig.create();

        // Thread pool: cores - 1 (leave one for OS)
        int cores = Runtime.getRuntime().availableProcessors();
        config.setThreads(Math.max(1, cores - 1));

        // Memory: 80% of available heap for i8g/r8g (memory-optimized)
        long maxMemory = Runtime.getRuntime().maxMemory();
        config.setMemoryLimit((long)(maxMemory * 0.8));

        // Enable SIMD
        if (HAS_AVX512) {
            // DuckDB automatically uses AVX-512 on Intel
            config.setOption("enable_simd", true);
        } else if (IS_ARM) {
            // DuckDB automatically uses NEON on Graviton
            config.setOption("enable_simd", true);
        }

        return config;
    }
}
```

#### AWS Graviton (ARM) Optimizations
```java
// DuckDB 1.1+ has excellent ARM NEON SIMD support
// Just ensure:
// 1. Use DuckDB native builds (not JVM-only)
// 2. Enable SIMD in config
// 3. Use vectorized batch sizes aligned to cache lines (64 bytes)

public static final int ARM_CACHE_LINE = 64;
public static final int VECTOR_BATCH_SIZE = IS_ARM ? 1024 : 2048; // ARM: smaller L1 cache
```

### 4. **Simplified Type System**

DuckDB has rich type system - map directly!

```java
public class TypeMapper {
    // Direct mapping - no intermediate layer
    private static final Map<DataType, String> SPARK_TO_DUCKDB = Map.ofEntries(
        entry(DataTypes.ByteType, "TINYINT"),        // DuckDB HAS tinyint!
        entry(DataTypes.ShortType, "SMALLINT"),
        entry(DataTypes.IntegerType, "INTEGER"),
        entry(DataTypes.LongType, "BIGINT"),
        entry(DataTypes.FloatType, "FLOAT"),
        entry(DataTypes.DoubleType, "DOUBLE"),
        entry(DataTypes.StringType, "VARCHAR"),
        entry(DataTypes.BooleanType, "BOOLEAN"),
        entry(DataTypes.DateType, "DATE"),
        entry(DataTypes.TimestampType, "TIMESTAMP"),  // DuckDB: microsecond precision (like Spark!)

        // Complex types - DuckDB native support
        entry(DataTypes.createArrayType(DataTypes.IntegerType), "INTEGER[]"),
        entry(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), "MAP(VARCHAR, INTEGER)")
    );

    public static String toDuckDBType(DataType sparkType) {
        if (sparkType instanceof DecimalType dt) {
            return String.format("DECIMAL(%d,%d)", dt.precision(), dt.scale());
        }
        return SPARK_TO_DUCKDB.get(sparkType);
    }
}
```

### 5. **Function Mapping: Leverage DuckDB Built-ins**

DuckDB has 500+ functions - most Spark functions have direct equivalents!

```java
public class FunctionRegistry {
    // Most Spark functions map 1:1 to DuckDB
    private static final Map<String, String> DIRECT_MAPPINGS = Map.ofEntries(
        // String functions
        entry("upper", "upper"),
        entry("lower", "lower"),
        entry("substring", "substring"),
        entry("trim", "trim"),
        entry("concat", "concat"),
        entry("length", "length"),
        entry("regexp_extract", "regexp_extract"),  // DuckDB has this!
        entry("regexp_replace", "regexp_replace"),

        // Math functions
        entry("abs", "abs"),
        entry("ceil", "ceil"),
        entry("floor", "floor"),
        entry("round", "round"),
        entry("sqrt", "sqrt"),
        entry("exp", "exp"),
        entry("log", "ln"),
        entry("pow", "pow"),

        // Date functions
        entry("year", "year"),
        entry("month", "month"),
        entry("day", "day"),
        entry("hour", "hour"),
        entry("minute", "minute"),
        entry("second", "second"),
        entry("date_add", "date_add"),
        entry("date_sub", "date_sub"),
        entry("datediff", "datediff"),

        // Aggregates
        entry("sum", "sum"),
        entry("avg", "avg"),
        entry("min", "min"),
        entry("max", "max"),
        entry("count", "count"),
        entry("stddev", "stddev"),
        entry("variance", "variance"),

        // Array functions (DuckDB 0.10+)
        entry("array_contains", "list_contains"),
        entry("array_distinct", "list_distinct"),
        entry("array_sort", "list_sort"),
        entry("size", "len")
    );

    // Only create UDFs for functions WITHOUT DuckDB equivalents
    private static final Set<String> NEEDS_UDF = Set.of(
        "array_zip",        // Complex, no DuckDB equivalent
        "transform",        // Lambda functions - needs UDF
        "filter",          // Lambda functions - needs UDF
        "aggregate"        // Lambda functions - needs UDF
    );
}
```

### 6. **Numerical Semantics: Use DuckDB Features**

```java
public class NumericalSemantics {
    /**
     * Integer division: Spark uses Java truncation semantics
     * DuckDB: Standard SQL division (returns DECIMAL by default)
     *
     * Solution: Use DuckDB's // operator (integer division) or cast
     */
    public String translateIntegerDivision(Expression left, Expression right) {
        // Option 1: Use DuckDB's integer division operator
        return String.format("(%s // %s)", translate(left), translate(right));

        // Option 2: Explicit casting
        // return String.format("CAST(%s / %s AS INTEGER)", translate(left), translate(right));
    }

    /**
     * Overflow behavior: Spark wraps, DuckDB errors by default
     * Solution: Use DuckDB's overflow handling
     */
    public void configureOverflowBehavior(DuckDBConnection conn) {
        // DuckDB 0.10+ supports overflow mode configuration
        conn.execute("SET integer_overflow_mode = 'wrap'");  // Match Spark semantics
    }

    /**
     * Null handling: Both use three-valued logic correctly
     * DuckDB matches SQL standard (and Spark) here - no changes needed
     */
}
```

### 7. **Query Optimization: DuckDB-Native**

```java
public class QueryOptimizer {
    /**
     * DuckDB has excellent optimizer - let it work!
     * Just ensure we generate optimization-friendly SQL
     */

    // Predicate pushdown: DuckDB does automatically
    // No need for manual pushdown with proper SQL generation

    // Projection pushdown: DuckDB does automatically
    // Just generate SELECT with only needed columns

    // Join reordering: DuckDB's cost-based optimizer handles this
    // Provide good statistics if available

    public void provideStatistics(DuckDBConnection conn, String tableName, TableStats stats) {
        // Help DuckDB's optimizer with stats
        String sql = String.format(
            "ALTER TABLE %s SET STATISTICS (rows=%d, distinct=%d)",
            tableName, stats.rowCount(), stats.distinctValues()
        );
        conn.execute(sql);
    }

    // Filter fusion: Combine multiple filters into one
    public String optimizeFilters(List<Expression> filters) {
        if (filters.isEmpty()) return null;
        if (filters.size() == 1) return translate(filters.get(0));

        // Combine with AND
        return filters.stream()
            .map(this::translate)
            .collect(Collectors.joining(" AND ", "(", ")"));
    }
}
```

### 8. **Multi-Core Parallelism**

```java
public class ParallelExecution {
    /**
     * DuckDB automatically parallelizes queries
     * Key: proper thread pool configuration
     */

    public static void configureParallelism(DuckDBConnection conn) {
        int cores = Runtime.getRuntime().availableProcessors();

        // DuckDB automatically uses all cores for:
        // - Table scans
        // - Aggregations
        // - Joins
        // - Sorting

        // Just set thread count
        conn.execute("SET threads TO " + (cores - 1));

        // Enable parallel CSV/Parquet reading
        conn.execute("SET parallel_csv_read = true");
        conn.execute("SET parallel_parquet = true");
    }

    /**
     * For i8g/r8g instances with many cores:
     * - Let DuckDB manage intra-query parallelism
     * - Use connection pooling for inter-query parallelism
     */

    public static class ConnectionPool {
        private final Queue<DuckDBConnection> pool;
        private final int poolSize;

        public ConnectionPool(String dbPath) {
            // Pool size: sqrt(cores) is good heuristic for mixed workloads
            this.poolSize = (int) Math.ceil(Math.sqrt(
                Runtime.getRuntime().availableProcessors()
            ));

            this.pool = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < poolSize; i++) {
                pool.add(createConnection(dbPath));
            }
        }
    }
}
```

### 9. **Memory Management for Large Instances**

```java
public class MemoryConfig {
    /**
     * r8g instances: memory-optimized (up to 1024 GiB)
     * i8g instances: storage-optimized with high memory
     * i4i instances: storage-optimized with NVMe
     */

    public static void configureMemory(DuckDBConnection conn, InstanceType type) {
        long heapMemory = Runtime.getRuntime().maxMemory();
        long systemMemory = getSystemMemory(); // Read from /proc/meminfo on Linux

        switch (type) {
            case R8G -> {
                // Memory-optimized: use generous memory limit
                long duckdbMemory = (long)(systemMemory * 0.7); // 70% of system
                conn.execute("SET memory_limit = '" + formatMemory(duckdbMemory) + "'");

                // Enable aggressive caching
                conn.execute("SET temp_directory = '/mnt/ramdisk'"); // Use tmpfs if available
            }

            case I8G, I4I -> {
                // Storage-optimized: moderate memory, excellent NVMe
                long duckdbMemory = (long)(systemMemory * 0.5); // 50% of system
                conn.execute("SET memory_limit = '" + formatMemory(duckdbMemory) + "'");

                // Use NVMe for temp tables
                conn.execute("SET temp_directory = '/mnt/nvme0n1/duckdb_temp'");

                // Enable memory-mapped I/O for large datasets
                conn.execute("SET enable_mmap = true");
            }
        }
    }
}
```

### 10. **Future Spark Connect Architecture**

**Shared Core Design:**

```
┌─────────────────────────────────────────────────────────────┐
│  Execution Mode 1: Embedded API (Phase 1)                  │
│  ┌──────────────┐                                          │
│  │ Spark API    │ → Logical Plan → DuckDB SQL → Execute   │
│  │ Facade       │                                          │
│  └──────────────┘                                          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Execution Mode 2: Spark Connect Server (Future)           │
│  ┌──────────────┐      ┌────────────────────────┐         │
│  │ Spark Client │ gRPC │ Custom Connect Server  │         │
│  │ (standard)   │─────→│ - Protobuf decoder     │         │
│  └──────────────┘      │ - Session manager      │         │
│                        │ - Arrow serialization  │         │
│                        └────────────────────────┘         │
│                                   ↓                         │
│                    Logical Plan → DuckDB SQL → Execute     │
└─────────────────────────────────────────────────────────────┘

        SHARED COMPONENTS (Both modes use these)
┌─────────────────────────────────────────────────────────────┐
│ • Logical Plan representation                               │
│ • SQL translation engine                                    │
│ • Type mapper                                              │
│ • Function registry                                        │
│ • DuckDB execution engine wrapper                          │
│ • Arrow interop layer                                      │
└─────────────────────────────────────────────────────────────┘
```

**Implementation Strategy:**

```java
// Shared core: Abstract execution engine
public interface QueryExecutor {
    ArrowBatch execute(LogicalPlan plan);
    void registerTempView(String name, LogicalPlan plan);
    Schema inferSchema(LogicalPlan plan);
}

// Embedded mode
public class EmbeddedExecutor implements QueryExecutor {
    private final DuckDBConnection conn;

    @Override
    public ArrowBatch execute(LogicalPlan plan) {
        String sql = translator.toSQL(plan);
        return conn.executeToArrow(sql);
    }
}

// Spark Connect mode (future)
public class SparkConnectServer extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    private final QueryExecutor executor; // Same executor!
    private final SessionManager sessions;

    @Override
    public void executePlan(ExecutePlanRequest request, StreamObserver<ExecutePlanResponse> observer) {
        // 1. Decode Spark Connect protobuf → LogicalPlan
        LogicalPlan plan = ProtobufDecoder.decode(request.getPlan());

        // 2. Execute using same executor as embedded mode
        ArrowBatch result = executor.execute(plan);

        // 3. Stream results back
        observer.onNext(createResponse(result));
        observer.onCompleted();
    }
}
```

## Performance Targets

### Baseline Expectations

```
Workload: TPC-H Scale Factor 10 (10GB)
Hardware: AWS r8g.4xlarge (16 vCPUs, 128GB RAM)

Operation              | DuckDB Target  | Spark 3.5 (local mode)
-----------------------|----------------|------------------------
Q1 (Scan + Agg)       | 0.5-1s         | 3-5s
Q3 (Join + Agg)       | 1-2s           | 8-12s
Q6 (Selective scan)   | 0.1-0.3s       | 1-2s
Q21 (Complex joins)   | 3-5s           | 20-30s

Memory Usage          | 2-4GB          | 15-20GB
Cold Start            | <100ms         | 5-10s
```

**Why DuckDB is Faster:**
1. Vectorized execution (process 1000s of rows at once)
2. Column-oriented storage (better cache utilization)
3. SIMD instructions (2-4x throughput)
4. Zero-copy Arrow paths
5. No JVM GC overhead for data processing

## Recommended Tech Stack

```xml
<dependencies>
    <!-- DuckDB: Latest stable -->
    <dependency>
        <groupId>org.duckdb</groupId>
        <artifactId>duckdb_jdbc</artifactId>
        <version>1.1.3</version> <!-- Latest as of 2024 -->
    </dependency>

    <!-- Arrow: Zero-copy data interchange -->
    <dependency>
        <groupId>org.apache.arrow</groupId>
        <artifactId>arrow-vector</artifactId>
        <version>17.0.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.arrow</groupId>
        <artifactId>arrow-memory-netty</artifactId>
        <version>17.0.0</version>
    </dependency>

    <!-- Spark API: For facade interfaces -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>3.5.3</version>
        <scope>provided</scope> <!-- Only for interface definitions -->
    </dependency>

    <!-- Calcite: FUTURE - Add in Phase 2/3 for multi-engine support -->
    <!--
    <dependency>
        <groupId>org.apache.calcite</groupId>
        <artifactId>calcite-core</artifactId>
        <version>1.37.0</version>
    </dependency>
    -->
</dependencies>
```

## Development Phases (Revised)

### Phase 1: DuckDB-Direct Foundation (Weeks 1-3)
- Logical plan representation (NOT Calcite)
- Direct SQL translation for DuckDB
- Basic operations: select, filter, project
- Type mapping system
- Arrow integration
- Hardware-aware configuration

**Deliverable**: Working embedded API with 80%+ performance of native DuckDB

### Phase 2: Expression System (Weeks 4-6)
- Complete function registry
- Expression translation
- Arithmetic, string, date functions
- Aggregations
- Window functions

**Deliverable**: Full DataFrame API coverage for common operations

### Phase 3: Advanced Features (Weeks 7-9)
- Complex joins
- Subqueries
- UDFs for unsupported functions
- Query optimization passes
- Performance tuning

**Deliverable**: Production-ready embedded engine

### Phase 4: Spark Connect Mode (Weeks 10-12)
- gRPC server implementation
- Protobuf decoding
- Session management
- Multi-client support
- Arrow streaming

**Deliverable**: Spark Connect-compatible server

### Phase 5: Abstraction Layer (Optional, Weeks 13-16)
- Add Calcite for multi-engine support
- Support PostgreSQL, ClickHouse
- Substrait integration
- Cross-engine optimizations

**Deliverable**: Multi-engine capable system

## Key Advantages of This Approach

1. **Simplicity**: Direct DuckDB targeting is far simpler than Calcite
2. **Performance**: No Calcite overhead, zero-copy Arrow paths
3. **Hardware-Optimized**: Intel AVX-512 and ARM NEON SIMD support
4. **Future-Ready**: Clean architecture supports Spark Connect later
5. **Elegant**: Minimal abstraction layers, clear separation of concerns
6. **Pragmatic**: Start simple, add complexity only when needed

## Migration Path for Existing Spark Code

```scala
// Existing Spark code
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.parquet("data.parquet")
df.filter($"age" > 25).groupBy($"country").count()

// DuckDB-backed version (same API!)
val spark = DuckDBSparkSession.builder().getOrCreate() // Drop-in replacement
val df = spark.read.parquet("data.parquet")
df.filter($"age" > 25).groupBy($"country").count()
// ^ Same code, 5-10x faster execution
```

## Conclusion

This architecture prioritizes:
1. **Performance**: DuckDB-native, zero-copy, SIMD-optimized
2. **Simplicity**: Direct SQL generation, no Calcite complexity initially
3. **Elegance**: Clean layers, minimal abstraction
4. **Future-Ready**: Architecture supports Spark Connect without major refactoring

Start with embedded DuckDB mode, deliver value quickly, then add Spark Connect when needed. Add Calcite only if multi-engine support becomes a requirement.
