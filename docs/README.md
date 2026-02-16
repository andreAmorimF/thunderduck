# Thunderduck Documentation

This directory contains documentation for the Thunderduck project.

---

## Core Documentation

### Architecture & Design
- **[PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)** - Multi-architecture support (x86_64, ARM64)
- **[OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)** - Why we use DuckDB's optimizer
- **[SUPPORTED_OPERATIONS.md](SUPPORTED_OPERATIONS.md)** - Quick reference of supported Spark operations
- **[SPARK_CONNECT_GAP_ANALYSIS.md](SPARK_CONNECT_GAP_ANALYSIS.md)** - Comprehensive Spark Connect protocol coverage analysis
- **[SQL_PARSER_RESEARCH.md](research/SQL_PARSER_RESEARCH.md)** - Research on SQL parser options for SparkSQL support
- **[TPC_H_BENCHMARK.md](TPC_H_BENCHMARK.md)** - TPC-H data generation and benchmark guide
- **[FUNCTION_COVERAGE.md](FUNCTION_COVERAGE.md)** - Spark SQL function coverage gap analysis (~260 functions mapped)

### Architecture Documents (architect/)
- **[SPARK_COMPAT_EXTENSION.md](architect/SPARK_COMPAT_EXTENSION.md)** - Spark compatibility extension (strict/relaxed modes)
- **[DIFFERENTIAL_TESTING_ARCHITECTURE.md](architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)** - Differential testing framework
- **[SESSION_MANAGEMENT_ARCHITECTURE.md](architect/SESSION_MANAGEMENT_ARCHITECTURE.md)** - Session management design
- **[ARROW_STREAMING_ARCHITECTURE.md](architect/ARROW_STREAMING_ARCHITECTURE.md)** - Arrow data streaming
- **[SPARK_CONNECT_QUICK_REFERENCE.md](architect/SPARK_CONNECT_QUICK_REFERENCE.md)** - Quick reference guide
- **[SPARK_CONNECT_PROTOCOL_COMPLIANCE.md](architect/SPARK_CONNECT_PROTOCOL_COMPLIANCE.md)** - Protocol compliance
- **[CATALOG_OPERATIONS.md](architect/CATALOG_OPERATIONS.md)** - Catalog operations implementation
- **[TYPE_MAPPING.md](architect/TYPE_MAPPING.md)** - DuckDB to Spark type mapping
- **[SPARKSQL_PARSER_DESIGN.md](architect/SPARKSQL_PARSER_DESIGN.md)** - SparkSQL ANTLR4 parser design (implemented)

### Research (research/)
Historical technical investigations: TPC-DS discovery, GROUPING() function analysis, Q36 DuckDB limitation.

### Pending Design (pending_design/)
Future feature specifications: Delta Lake integration, AWS credential chain, S3 throughput optimization.

### Development Journal (dev_journal/)
Milestone completion reports documenting the project's development progress.
Reports are prefixed with M[X]_ indicating chronological order (M1 through M71+).

Key milestones:
- **M1-M16**: Core infrastructure, TPC-H/TPC-DS, Spark Connect server
- **M17-M38**: Session management, DataFrame operations, window functions
- **M39-M44**: Differential testing framework, catalog operations
- **M45-M49**: Statistics, lambda functions, complex types, type literals
- **M50-M64**: Direct alias optimization, type inference, joins, sorting
- **M68-M71**: Decimal precision, datetime fixes, performance instrumentation
- **M72-M79**: SparkSQL parser, schema-aware dispatch, extension functions
- **M80-M83**: Strict mode convergence (744/2), test suite optimization
- **M84**: Function coverage expansion (81 new function mappings, 76 new tests)
- **M85**: Bug fixes, 17 skipped tests resolved, rewriteSQL() elimination, from_json schema support (830/0/3)

---

## Quick Links

**Getting Started**:
- Start with [../README.md](../README.md) - Project overview
- Then read [SUPPORTED_OPERATIONS.md](SUPPORTED_OPERATIONS.md) - What Spark operations are supported
- For testing: [architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md](architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)

**For Developers**:
- Compatibility modes: [architect/SPARK_COMPAT_EXTENSION.md](architect/SPARK_COMPAT_EXTENSION.md)
- Gap analysis: [SPARK_CONNECT_GAP_ANALYSIS.md](SPARK_CONNECT_GAP_ANALYSIS.md)
- Optimization: [OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)

**For Operations**:
- Platform support: [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)
- Session management: [architect/SESSION_MANAGEMENT_ARCHITECTURE.md](architect/SESSION_MANAGEMENT_ARCHITECTURE.md)

---

## Appendix: Accepted Divergences from Spark in Strict Mode

Thunderduck aims for exact Spark parity in strict mode. However, a small number of behavioral differences have been intentionally accepted as documented trade-offs, where matching Spark exactly is either infeasible or would require disproportionate effort for marginal benefit.

### Functions Using Different Algorithms

These functions exist in both Spark and DuckDB but use fundamentally different algorithms, making exact value matching impossible without reimplementing the algorithm from scratch in the DuckDB extension.

| Function | Spark Formula | DuckDB Formula | Difference and Rationale |
|----------|--------------|----------------|--------------------------|
| `percentile_approx` | G-K algorithm with accuracy param | T-Digest (different algorithm) | Not feasible to match exactly -- different algorithms produce inherently different approximations. Could implement G-K in the extension, but this is extremely high complexity for marginal benefit. |
| `skewness` (relaxed mode only) | Population: `mu_3 / mu_2^(3/2)` | Sample: `sqrt(n*(n-1))/(n-2) * population` | In relaxed mode, uses DuckDB's built-in sample skewness. Difference is ~1.5% for n=100, ~6% for n=10. Strict mode matches exactly via `spark_skewness()` extension function. |

### Functions with Output Format Differences

These functions produce semantically equivalent results but in a different textual representation that cannot be reconciled without a full reimplementation.

| Function | Spark Output | DuckDB Output | Difference and Rationale |
|----------|-------------|---------------|--------------------------|
| `schema_of_json` | DDL format: `STRUCT<a: BIGINT>` | JSON format: `{"a":"UBIGINT"}` | DuckDB's `json_structure` returns JSON schema representation. Producing Spark's DDL format would require building a full JSON-to-DDL converter with Spark type name mapping -- high complexity for a rarely used introspection function. |

### Intentionally Out of Scope

Entire function families that are not applicable to Thunderduck's single-node DuckDB architecture:

| Category | Approximate Count | Rationale |
|----------|-------------------|-----------|
| Sketch functions | ~25 | Distributed approximation algorithms (HyperLogLog, CountMinSketch, etc.) designed for distributed compute. DuckDB has native exact equivalents for most use cases. |
| Streaming functions | ~10 | Spark Structured Streaming concepts (window, watermark) that don't apply to batch-on-DuckDB. |
| Distributed-only | ~15 | Functions like `spark_partition_id`, `input_file_name` that are inherently tied to distributed execution. |
| Variant functions | ~10 | Spark 4.x semi-structured type -- niche feature not yet widely adopted. |
| XML functions | ~12 | Niche format with minimal overlap with DuckDB's analytical workloads. |
| Avro/Protobuf | ~5 | Serialization format functions not relevant to single-node analytics. |

---

**Last Updated**: 2026-02-16
