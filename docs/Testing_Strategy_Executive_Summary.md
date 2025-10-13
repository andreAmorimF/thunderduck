# Testing Strategy - Executive Summary

## Overview

This document summarizes the comprehensive 4-layer testing strategy for validating the Spark DataFrame to DuckDB translation layer.

## Architecture

```
Layer 4: Comparative Analysis (DuckDB vs Spark Cluster)
    ↓
Layer 3: Performance Benchmarks (TPC-H, TPC-DS @ SF=0.01)
    ↓
Layer 2: Integration Tests (Transformation chains, Format readers)
    ↓
Layer 1: BDD Unit Tests (Spark local mode as reference oracle)
```

## Key Design Decisions

### 1. Differential Testing Approach
**Use Spark 3.5 local mode as the reference oracle**
- Every test executes the same operation on both Spark and DuckDB
- Compare: schemas, data (row-by-row), behavior (error handling)
- Ensures numerical consistency and semantic correctness

### 2. BDD Test Structure (Given-When-Then)
```java
// GIVEN: Test data setup
given().sparkDataFrame().withColumns(...).withData(...)

// WHEN: DataFrame operation execution
when().filter("age > 25").execute()

// THEN: Result validation against Spark
then().assertSchemaEquals().assertDataEquals()
```

### 3. Test Pyramid
- **Layer 1 (300+ tests)**: Fast unit tests (<2 min)
- **Layer 2 (100+ tests)**: Integration tests (<2 min)
- **Layer 3 (22+ queries)**: Performance benchmarks (<1 min)
- **Layer 4 (Comparative)**: Cost/performance analysis

Total: **500+ tests in <5 minutes**

## Testing Layers Detail

### Layer 1: BDD Unit Tests
**Purpose**: Validate individual components with Spark as oracle

**Test Categories**:
1. **Type Mapping** (50+ tests)
   - Numeric types (byte, short, int, long, float, double, decimal)
   - Complex types (array, map, struct)
   - Timestamp precision (microseconds)

2. **Expression Translation** (100+ tests)
   - Arithmetic (integer division, overflow, modulo)
   - Comparison (null handling, three-valued logic)
   - Logical (short-circuit evaluation)

3. **Function Mapping** (200+ tests)
   - String functions (upper, lower, substring, regex)
   - Date functions (year, month, date_add)
   - Aggregates (sum, avg, count, stddev)
   - Window functions (row_number, rank, lag, lead)

4. **SQL Generation** (50+ tests)
   - Simple selects
   - Complex queries with subqueries
   - Join syntax

### Layer 2: Integration Tests
**Purpose**: Test transformation chains and format readers

**Test Areas**:
- End-to-end ETL pipelines (multi-step transformations)
- Self-join patterns
- Parquet: Nested schema handling
- Delta Lake: Time travel
- Iceberg: Partition pruning

### Layer 3: Performance Benchmarks
**Purpose**: Validate speedup targets

**TPC-H (SF=0.01, 22 queries)**:
- Q1 (Scan + Agg): Target 5x faster
- Q3 (Join + Agg): Target 5x faster
- Q6 (Selective): Target 8x faster
- Q13 (Complex): Target 5x faster

**TPC-DS (SF=0.01, selected queries)**:
- More complex patterns than TPC-H
- Target: 5-10x speedup

**Memory Profiling**:
- 1GB scan: <500MB memory (8x less than Spark)
- 10GB join: <2GB memory (6-8x less than Spark)

### Layer 4: Comparative Analysis
**Purpose**: Compare embedded DuckDB vs external Spark cluster

**Analyses**:
1. Cost-effectiveness (target >60% savings)
2. Performance crossover (~200GB dataset size)
3. Workload sizing recommendations

## Command-Line Interface

```bash
# Run all tests (5 minutes)
./test.sh all

# Run by layer
./test.sh unit              # Layer 1
./test.sh integration       # Layer 2
./test.sh benchmark --suite=tpch  # Layer 3
./test.sh compare           # Layer 4

# Run specific categories
./test.sh unit --category=type-mapping
./test.sh integration --format=parquet
./test.sh benchmark --suite=tpch --query=1,3,6

# Generate reports
./test.sh all --coverage
./test.sh report --format=html
```

## Success Criteria

### Correctness
- ✓ Differential test pass rate: **>95%**
- ✓ Numerical consistency: **100%** (within tolerance)
- ✓ Type mapping accuracy: **100%**
- ✓ Function coverage: **>90%** of common functions

### Performance
- ✓ TPC-H speedup: **5-10x** vs Spark
- ✓ TPC-DS speedup: **5-10x** vs Spark
- ✓ Memory efficiency: **6-8x** less than Spark
- ✓ Overhead vs native DuckDB: **10-20%**

### Maintainability
- ✓ Test execution time: **<5 minutes** full suite
- ✓ Code coverage: **>80%**
- ✓ Test flakiness rate: **<1%**

## Implementation Roadmap

| Week | Phase | Deliverables |
|------|-------|--------------|
| 1 | Foundation | BDD framework, 150+ unit tests |
| 2 | Coverage | 250+ function tests, integration framework |
| 3 | Performance | TPC-H/TPC-DS benchmarks, memory profiling |
| 4 | Analysis | Comparative analysis, comprehensive reports |

**Total: 4 weeks to complete testing infrastructure**

## Infrastructure Requirements

### Development Environment
- CPU: 4+ cores
- RAM: 16GB minimum
- Storage: 50GB for test data

### CI/CD Environment
- CPU: 8+ cores
- RAM: 32GB minimum
- Storage: 100GB

### Software Dependencies
- **Testing**: JUnit 5.10.0, Cucumber 7.14.0, AssertJ 3.24.2
- **Benchmarking**: JMH 1.37
- **Reference Oracle**: Apache Spark 3.5.3 (test scope)
- **Coverage**: JaCoCo

## Key Insights

1. **Differential Testing is Critical**: Using Spark as the reference oracle ensures correctness without re-implementing complex validation logic.

2. **Fast Feedback Loop**: <5 minute test execution enables rapid iteration during development.

3. **Progressive Validation**: Test pyramid structure (fast unit → slower integration → focused performance) optimizes for speed and coverage.

4. **Industry-Standard Benchmarks**: TPC-H and TPC-DS provide objective, comparable performance metrics.

5. **One-Liner Commands**: Simple CLI interface (`./test.sh unit`) lowers barrier to running tests.

## Next Steps

1. **Handoff to CODER agent**: Implement BDD test framework (Week 1)
2. **Handoff to COORDINATOR agent**: Set up CI/CD pipeline
3. **Handoff to PERFORMANCE agent**: Generate TPC-H/TPC-DS test data

## References

- **Full Strategy Document**: `/workspaces/catalyst2sql/docs/Testing_Strategy.md`
- **Architecture Design**: `/workspaces/catalyst2sql/docs/Analysis_and_Design.md`
- **Memory Store**: `/workspaces/catalyst2sql/.hive-mind/workers/analyst/`
