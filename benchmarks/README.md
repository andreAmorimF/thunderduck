# TPC-H Benchmark Framework

This module provides a JMH-based benchmark framework for measuring query optimizer performance using the TPC-H decision support benchmark.

## Overview

The TPC-H benchmark consists of 22 business-oriented queries that exercise different aspects of SQL optimization:

- **Simple aggregations** (Q1, Q6)
- **Complex joins** (Q2, Q3, Q5, Q7, Q8, Q9, Q10)
- **Subqueries** (Q2, Q4, Q17, Q20, Q21, Q22)
- **Window functions** (Q12, Q13, Q15, Q16, Q18, Q19)
- **Set operations** (Q11, Q14)

## Current Implementation Status

### ✅ Fully Implemented
- **Query 1 (Pricing Summary)**: Complete implementation with filter, aggregate, and sort
- **Query 3 (Shipping Priority)**: Multi-table joins with aggregation
- **Query 6 (Forecasting Revenue)**: Simple scan and filter aggregation

### 🔨 Structural Stubs
- **Query 2**: Minimum cost supplier (simplified to table scan)
- **Queries 4-22**: Basic stubs for framework completeness

## Usage

### Prerequisites

Add JMH dependencies to `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>org.openjdk.jmh</groupId>
        <artifactId>jmh-core</artifactId>
        <version>1.37</version>
    </dependency>
    <dependency>
        <groupId>org.openjdk.jmh</groupId>
        <artifactId>jmh-generator-annprocess</artifactId>
        <version>1.37</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

### Build Benchmarks

```bash
# Build the benchmark JAR
mvn clean package -pl benchmarks

# Or use Maven shade plugin to create uber-jar
mvn clean package -pl benchmarks -Pjmh
```

### Run Benchmarks

```bash
# Run all TPC-H benchmarks
java -jar benchmarks/target/benchmarks.jar TPCHBenchmark

# Run specific query (e.g., Q1)
java -jar benchmarks/target/benchmarks.jar "TPCHBenchmark.benchmarkQ1.*"

# Run with specific scale factor
java -jar benchmarks/target/benchmarks.jar -p scaleFactor=1

# Run with optimization enabled/disabled
java -jar benchmarks/target/benchmarks.jar -p enableOptimization=true

# Run with specific parameters
java -jar benchmarks/target/benchmarks.jar \
    -p scaleFactor=0.01,1 \
    -p enableOptimization=true,false \
    "TPCHBenchmark.benchmarkQ1.*"
```

### Manual Testing

For quick testing without JMH:

```bash
cd benchmarks
mvn compile exec:java -Dexec.mainClass="com.catalyst2sql.tpch.TPCHBenchmark"
```

## Scale Factors

The benchmark supports multiple scale factors for different testing scenarios:

| Scale Factor | Dataset Size | Use Case |
|--------------|--------------|----------|
| 0.01 | 10MB | Development, unit tests |
| 1 | 1GB | Continuous integration |
| 10 | 10GB | Nightly benchmarks |
| 100 | 100GB | Weekly benchmarks |

## Benchmark Metrics

The framework measures:

1. **SQL Generation Time**: Time to convert logical plan to DuckDB SQL
2. **Optimization Time**: Time to apply optimization rules (filter pushdown, column pruning, etc.)
3. **End-to-End Time**: Complete pipeline from logical plan to optimized SQL
4. **Memory Overhead**: Memory used during optimization

## Performance Targets

Based on TPC-H SF=10:

- **SQL Generation**: < 100ms for complex queries
- **Optimization**: 10-30% improvement over unoptimized plans
- **Memory Overhead**: < 50MB for optimization metadata

## Query Implementation Details

### Q1: Pricing Summary Report

**Complexity**: Simple
**Operations**: TableScan → Filter → Aggregate → Sort

```sql
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

**Tests**: Basic scan, filter, aggregate, and sort operations

### Q3: Shipping Priority

**Complexity**: Moderate
**Operations**: 3-way Join → Filter → Aggregate → Sort → Limit

```sql
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

**Tests**: Multi-table joins, aggregation, top-N

### Q6: Forecasting Revenue Change

**Complexity**: Simple
**Operations**: TableScan → Filter → Aggregate

```sql
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
```

**Tests**: Simple scan and filter aggregation, good for baseline performance

## Data Generation

TPC-H data can be generated using `tpchgen-rs` (20x faster than classic dbgen):

```bash
# Install tpchgen-rs
cargo install tpchgen-cli

# Generate data at different scale factors
tpchgen-cli -s 0.01 --format=parquet --output=data/tpch_sf001  # 10MB
tpchgen-cli -s 1 --format=parquet --output=data/tpch_sf1       # 1GB
tpchgen-cli -s 10 --format=parquet --output=data/tpch_sf10     # 10GB
```

## Output Format

JMH produces detailed benchmark results including:

```
Benchmark                                 (scaleFactor)  (enableOptimization)  Mode  Cnt   Score   Error  Units
TPCHBenchmark.benchmarkQ1_SQLGeneration            0.01                  true  avgt   10   2.345 ± 0.123  ms/op
TPCHBenchmark.benchmarkQ1_Optimization             0.01                  true  avgt   10   1.234 ± 0.089  ms/op
TPCHBenchmark.benchmarkQ1_EndToEnd                 0.01                  true  avgt   10   3.567 ± 0.201  ms/op
```

## Future Work

- Complete implementation of all 22 TPC-H queries
- Add query execution benchmarks (in addition to SQL generation)
- Integrate with actual TPC-H data and DuckDB execution
- Add memory profiling and GC analysis
- Create automated regression detection
- Compare with Spark query optimization

## References

- [TPC-H Benchmark Specification](http://www.tpc.org/tpch/)
- [JMH Documentation](https://openjdk.org/projects/code-tools/jmh/)
- [DuckDB Documentation](https://duckdb.org/)
