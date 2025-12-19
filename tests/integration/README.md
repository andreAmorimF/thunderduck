# Thunderduck Integration Tests

Pytest-based integration tests for Thunderduck Spark Connect server using PySpark 4.0.1 client.

## Overview

This test suite validates the Spark Connect server implementation by running queries against both Thunderduck and Apache Spark 4.0.1, comparing results for correctness.

## Directory Structure

```
tests/integration/
├── conftest.py                         # Pytest fixtures
├── README.md                           # This file
│
├── differential/                       # Differential tests (vs Apache Spark 4.0.1)
│   ├── test_differential_v2.py         # TPC-H SQL + DataFrame (34 tests)
│   ├── test_tpcds_differential.py      # TPC-DS SQL + DataFrame (126 tests)
│   ├── test_dataframe_functions.py     # Function parity (57 tests)
│   ├── test_multidim_aggregations.py   # pivot, cube, rollup (21 tests)
│   ├── test_window_functions.py        # Window functions (35 tests)
│   ├── test_dataframe_ops_differential.py  # DataFrame operations (25 tests)
│   ├── test_lambda_differential.py     # Lambda/HOF functions (18 tests)
│   ├── test_using_joins_differential.py    # USING join syntax (11 tests)
│   ├── test_statistics_differential.py # cov, corr, describe, summary (16 tests)
│   ├── test_complex_types_differential.py  # struct, array, map access (15 tests)
│   ├── test_type_literals_differential.py  # Type literals, intervals (32 tests)
│   ├── test_to_schema_differential.py  # df.to(schema) support (12 tests)
│   └── test_tpcds_dataframe_differential.py  # TPC-DS DataFrame API (34 tests)
│
├── test_*.py                           # Feature tests (Thunderduck only)
│   ├── test_catalog_operations.py      # Catalog/database operations
│   ├── test_empty_dataframe.py         # Empty DataFrame handling
│   ├── test_simple_sql.py              # Basic SQL connectivity
│   └── test_temp_views.py              # Temporary view management
│
├── tpch_sf001/                         # TPC-H SF0.01 data (parquet)
├── tpcds_dataframe/                    # TPC-DS DataFrame API implementations
├── sql/                                # SQL query files
└── utils/                              # Test utilities
```

## Quick Start

### Run Differential Tests (Recommended)

Compare Thunderduck against Apache Spark 4.0.1:

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run all differential tests
./tests/scripts/run-differential-tests-v2.sh

# Run specific test groups
./tests/scripts/run-differential-tests-v2.sh tpch         # TPC-H (34 tests)
./tests/scripts/run-differential-tests-v2.sh tpcds        # TPC-DS (126 tests)
./tests/scripts/run-differential-tests-v2.sh functions    # Functions (57 tests)
./tests/scripts/run-differential-tests-v2.sh aggregations # Aggregations (21 tests)
./tests/scripts/run-differential-tests-v2.sh window       # Window (35 tests)
./tests/scripts/run-differential-tests-v2.sh operations   # DataFrame ops (25 tests)
./tests/scripts/run-differential-tests-v2.sh lambda       # Lambda/HOF (18 tests)
./tests/scripts/run-differential-tests-v2.sh joins        # USING joins (11 tests)
./tests/scripts/run-differential-tests-v2.sh statistics   # Statistics (16 tests)
./tests/scripts/run-differential-tests-v2.sh types        # Complex types (47 tests)
./tests/scripts/run-differential-tests-v2.sh schema       # ToSchema (12 tests)
./tests/scripts/run-differential-tests-v2.sh dataframe    # TPC-DS DataFrame API (34 tests)
```

### Run Feature Tests

Test specific Thunderduck features:

```bash
cd tests/integration

# All feature tests
python3 -m pytest test_*.py -v

# Specific test file
python3 -m pytest test_empty_dataframe.py -v
```

## Test Categories

### Differential Tests (`differential/`)

These tests run the same query on both Thunderduck and Apache Spark 4.0.1, comparing results row-by-row.

| Suite | Tests | Description |
|-------|-------|-------------|
| TPC-H | 34 | Q1-Q22 SQL + DataFrame API |
| TPC-DS | 126 | 102 SQL + 24 DataFrame |
| Functions | 57 | Array, Map, String, Math |
| Aggregations | 21 | pivot, unpivot, cube, rollup |
| Window | 35 | rank, lag/lead, frames |
| Operations | 25 | drop, withColumn, union, sample |
| Lambda | 18 | transform, filter, aggregate |
| Joins | 11 | USING join syntax |
| Statistics | 16 | cov, corr, describe, summary |
| Complex Types | 15 | struct.field, arr[i], map[key] |
| Type Literals | 32 | timestamps, intervals, arrays, maps |
| ToSchema | 12 | df.to(schema) column reorder/cast |
| DataFrame API | 34 | TPC-DS queries via DataFrame API |
| **Total** | **436** | |

### Feature Tests (root `test_*.py`)

Standalone tests for specific Thunderduck features (not compared to Spark).

## Fixtures

Key fixtures in `conftest.py`:

- `spark` / `spark_thunderduck` - Thunderduck session (port 15002)
- `spark_reference` - Apache Spark session (port 15003)
- `tpch_data_dir` - Path to TPC-H parquet files

## Troubleshooting

### Server Won't Start

```bash
pkill -9 -f thunderduck-connect-server
mvn clean package -DskipTests
```

### Check Server Logs

```bash
tail -f /tmp/thunderduck-server.log
```

---

**Last Updated**: 2025-12-19
