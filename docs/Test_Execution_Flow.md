# Test Execution Flow Visualization

## High-Level Test Execution Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DEVELOPER / CI PIPELINE                      │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         ./test.sh [command]                         │
│                     (Unified Test Runner)                           │
└─────────────┬───────────────┬──────────────┬───────────────────────┘
              │               │              │
    ┌─────────┴────┐   ┌──────┴──────┐   ┌──┴─────────┐
    │ ./test.sh    │   │ ./test.sh   │   │ ./test.sh  │
    │ unit         │   │ integration │   │ benchmark  │
    └─────────┬────┘   └──────┬──────┘   └──┬─────────┘
              │               │              │
              ▼               ▼              ▼
```

## Layer 1: BDD Unit Test Execution Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  1. GIVEN: Test Data Setup                                      │
│     ┌────────────────────────────────────────┐                  │
│     │ TestDataBuilder                        │                  │
│     │ • Create DataFrame with schema         │                  │
│     │ • Add test data rows                   │                  │
│     │ • Include edge cases                   │                  │
│     └────────────────────────────────────────┘                  │
│                        │                                         │
│                        ▼                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           Parallel Execution                            │    │
│  │  ┌─────────────────────┬─────────────────────┐         │    │
│  │  │ Spark Local Mode    │ DuckDB Embedded     │         │    │
│  │  │ (Reference Oracle)  │ (Implementation)    │         │    │
│  │  └─────────────────────┴─────────────────────┘         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                        │                                         │
│                        ▼                                         │
│  2. WHEN: Execute Operation                                     │
│     ┌────────────────────────────────────────┐                  │
│     │ Same operation on both engines:        │                  │
│     │ • filter("age > 25")                   │                  │
│     │ • select("name", "age")                │                  │
│     │ • groupBy("category").agg(...)         │                  │
│     └────────────────────────────────────────┘                  │
│                        │                                         │
│                        ▼                                         │
│  3. THEN: Validate Results                                      │
│     ┌────────────────────────────────────────┐                  │
│     │ DifferentialAssertion                  │                  │
│     │ • assertSchemaEquals()                 │                  │
│     │ • assertDataEquals()                   │                  │
│     │ • assertRowCount()                     │                  │
│     │ • assertNumericPrecision()             │                  │
│     └────────────────────────────────────────┘                  │
└──────────────────────────────────────────────────────────────────┘
```

## Layer 2: Integration Test Execution Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  Multi-Step Transformation Chain                                │
│                                                                  │
│  Step 1: Load Data                                              │
│  ┌──────────────────────────────────────────────────┐           │
│  │ spark.read().parquet("sales.parquet")            │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Step 2: Filter                                                 │
│  ┌──────────────────────────────────────────────────┐           │
│  │ .filter("date >= '2024-01-01'")                  │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Step 3: Join                                                   │
│  ┌──────────────────────────────────────────────────┐           │
│  │ .join(products, "product_id")                    │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Step 4: Aggregate                                              │
│  ┌──────────────────────────────────────────────────┐           │
│  │ .groupBy("category").agg(sum("amount"))          │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Step 5: Window Function                                        │
│  ┌──────────────────────────────────────────────────┐           │
│  │ .withColumn("rank", rank().over(...))            │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Step 6: Validate                                               │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Compare final result: Spark vs DuckDB            │           │
│  │ • Schema match                                   │           │
│  │ • Data match (all rows)                          │           │
│  │ • Performance: DuckDB should be 5x faster        │           │
│  └──────────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────────┘
```

## Layer 3: Performance Benchmark Execution Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  TPC-H Query Benchmark (e.g., Q1)                               │
│                                                                  │
│  Phase 1: Warmup (3 iterations)                                 │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Execute query 3 times to warm JVM/cache          │           │
│  │ Discard results                                  │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Phase 2: Measurement (10 iterations)                           │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Parallel Execution:                              │           │
│  │ ┌─────────────────────┬─────────────────────┐    │           │
│  │ │ Spark Execution     │ DuckDB Execution    │    │           │
│  │ │ • Record time       │ • Record time       │    │           │
│  │ │ • Record memory     │ • Record memory     │    │           │
│  │ │ • Collect result    │ • Collect result    │    │           │
│  │ └─────────────────────┴─────────────────────┘    │           │
│  │                                                   │           │
│  │ Per iteration:                                   │           │
│  │ • Start timer                                    │           │
│  │ • Execute query                                  │           │
│  │ • Collect result                                 │           │
│  │ • Stop timer                                     │           │
│  │ • Record metrics                                 │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Phase 3: Analysis                                              │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Statistical Analysis:                            │           │
│  │ • Calculate median time                          │           │
│  │ • Calculate mean time                            │           │
│  │ • Calculate std deviation                        │           │
│  │ • Compare results (validate correctness)         │           │
│  │ • Calculate speedup factor                       │           │
│  │                                                  │           │
│  │ Assertions:                                      │           │
│  │ ✓ Results match between engines                 │           │
│  │ ✓ DuckDB median time < Spark median / 5         │           │
│  │ ✓ DuckDB peak memory < Spark peak / 6           │           │
│  └──────────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────────┘
```

## Test Data Generation Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  Test Data Lifecycle                                            │
│                                                                  │
│  Stage 1: Generation (one-time setup)                           │
│  ┌──────────────────────────────────────────────────┐           │
│  │ TPC-H Data Generator (dbgen)                     │           │
│  │ • Scale Factor: 0.01                             │           │
│  │ • Output: tpch_data/*.parquet                    │           │
│  │ • Size: ~10MB per table                          │           │
│  │                                                  │           │
│  │ TPC-DS Data Generator (dsdgen)                   │           │
│  │ • Scale Factor: 0.01                             │           │
│  │ • Output: tpcds_data/*.parquet                   │           │
│  │ • Size: ~50MB total                              │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Stage 2: Loading (per test)                                    │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Spark: spark.read().parquet("tpch_data/...")     │           │
│  │ DuckDB: read_parquet('tpch_data/...')            │           │
│  │                                                  │           │
│  │ Lazy loading (no data movement until query)     │           │
│  └────────────────────┬─────────────────────────────┘           │
│                       ▼                                          │
│  Stage 3: Query Execution                                       │
│  ┌──────────────────────────────────────────────────┐           │
│  │ Execute TPC-H/TPC-DS queries                     │           │
│  │ • Both engines read same data                    │           │
│  │ • Compare execution time                         │           │
│  │ • Validate result correctness                    │           │
│  └──────────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────────┘
```

## Differential Testing Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                   Differential Testing Core                     │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ @Test                                                     │  │
│  │ public void testFilterOperation() {                       │  │
│  │                                                           │  │
│  │   // GIVEN: Same test data in both engines               │  │
│  │   DataFrame testData = TestData.createSample();          │  │
│  │   DataFrame sparkDF = sparkSession.createDataFrame(...); │  │
│  │   DataFrame duckDF = duckDBSession.createDataFrame(...); │  │
│  │                                                           │  │
│  │   // WHEN: Execute same operation                        │  │
│  │   Dataset<Row> sparkResult = sparkDF                     │  │
│  │       .filter("age > 25")                                │  │
│  │       .select("name", "age")                             │  │
│  │       .collect();                                        │  │
│  │                                                           │  │
│  │   Dataset<Row> duckDBResult = duckDF                     │  │
│  │       .filter("age > 25")                                │  │
│  │       .select("name", "age")                             │  │
│  │       .collect();                                        │  │
│  │                                                           │  │
│  │   // THEN: Results must match                            │  │
│  │   DifferentialAssertion.assertResultsMatch(              │  │
│  │       sparkResult,                                       │  │
│  │       duckDBResult,                                      │  │
│  │       tolerance = 1e-10  // For floating-point           │  │
│  │   );                                                     │  │
│  │ }                                                         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Comparison Algorithm:                                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. Compare schemas                                        │  │
│  │    • Field names must match                               │  │
│  │    • Data types must be compatible                        │  │
│  │    • Nullability must match                               │  │
│  │                                                           │  │
│  │ 2. Compare row counts                                     │  │
│  │    • Must be identical                                    │  │
│  │                                                           │  │
│  │ 3. Compare data (row by row)                              │  │
│  │    • Sort both results by all columns (deterministic)     │  │
│  │    • For each row:                                        │  │
│  │      - For numeric: abs(spark - duckdb) < tolerance       │  │
│  │      - For string: exact match                            │  │
│  │      - For null: both null or both not null               │  │
│  │                                                           │  │
│  │ 4. Report differences                                     │  │
│  │    • Show first N differing rows                          │  │
│  │    • Highlight which columns differ                       │  │
│  │    • Compute diff statistics                              │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## CI/CD Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  GitHub Actions Workflow                                        │
│                                                                 │
│  Trigger: Push or Pull Request                                 │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────┐                   │
│  │ Job 1: Unit Tests (parallel)            │                   │
│  │ • Run on ubuntu-latest                  │                   │
│  │ • JDK 17                                │                   │
│  │ • Execute: ./test.sh unit --ci          │                   │
│  │ • Duration: ~2 minutes                  │                   │
│  │ • Upload coverage to Codecov            │                   │
│  └─────────────────────────────────────────┘                   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────┐                   │
│  │ Job 2: Integration Tests (parallel)     │                   │
│  │ • Run on ubuntu-latest                  │                   │
│  │ • JDK 17                                │                   │
│  │ • Execute: ./test.sh integration --ci   │                   │
│  │ • Duration: ~2 minutes                  │                   │
│  └─────────────────────────────────────────┘                   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────┐                   │
│  │ Job 3: Benchmarks (on PR only)          │                   │
│  │ • Run on ubuntu-latest (8 cores)        │                   │
│  │ • JDK 17                                │                   │
│  │ • Execute: ./test.sh benchmark          │                   │
│  │ • Duration: ~5 minutes                  │                   │
│  │ • Upload benchmark results as artifact  │                   │
│  │ • Comment performance comparison on PR  │                   │
│  └─────────────────────────────────────────┘                   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────┐                   │
│  │ Job 4: Report Generation                │                   │
│  │ • Aggregate test results                │                   │
│  │ • Generate HTML report                  │                   │
│  │ • Publish to GitHub Pages               │                   │
│  └─────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

## Summary: Test Execution Time Budget

```
┌──────────────────────────────────────────────────────────────┐
│  Test Execution Time Breakdown (Target: <5 minutes)         │
│                                                              │
│  Layer 1: BDD Unit Tests             <2 min   (300+ tests)  │
│  ├─ Type Mapping Tests               20 sec   (50 tests)    │
│  ├─ Expression Translation Tests     40 sec   (100 tests)   │
│  ├─ Function Mapping Tests           50 sec   (200 tests)   │
│  └─ SQL Generation Tests             10 sec   (50 tests)    │
│                                                              │
│  Layer 2: Integration Tests          <2 min   (100+ tests)  │
│  ├─ End-to-End Pipelines             60 sec   (30 tests)    │
│  ├─ Format Readers                   40 sec   (40 tests)    │
│  └─ Complex Patterns                 20 sec   (30 tests)    │
│                                                              │
│  Layer 3: Performance Benchmarks     <1 min   (22+ tests)   │
│  ├─ TPC-H Queries                    40 sec   (22 queries)  │
│  ├─ Memory Profiling                 15 sec   (5 tests)     │
│  └─ Report Generation                5 sec                  │
│                                                              │
│  Total:                              <5 min   (500+ tests)  │
│                                                              │
│  Optimization Techniques:                                    │
│  • Parallel test execution (JUnit 5 @Parallel)              │
│  • In-memory test data (no I/O)                             │
│  • Shared SparkSession/DuckDBSession (@BeforeAll)           │
│  • Fast assertions (avoid expensive operations)             │
│  • Benchmark warmup only once per suite                     │
└──────────────────────────────────────────────────────────────┘
```

## Key Success Factors

1. **Parallelization**: Tests run in parallel using JUnit 5 parallel execution
2. **Shared Sessions**: One SparkSession and DuckDBSession per test class
3. **Fast Assertions**: Optimized comparison algorithms
4. **Minimal I/O**: In-memory test data for unit/integration tests
5. **Smart Benchmarking**: Warmup once, measure multiple times
6. **Fail Fast**: CI mode exits on first failure for quick feedback
