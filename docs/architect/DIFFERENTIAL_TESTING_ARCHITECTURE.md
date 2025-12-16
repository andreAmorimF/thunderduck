# Differential Testing Architecture

**Last Updated:** 2025-12-16
**Status:** Production Ready

## Overview

The differential testing framework compares Thunderduck against Apache Spark 4.0.1 to ensure exact compatibility. Both systems run via Spark Connect protocol for fair comparison.

**Test Coverage:**
- **TPC-H**: 23 differential tests (Q1-Q22 + sanity test) - ALL PASSING
- **TPC-DS**: 102 differential tests (94 standard queries + 8 variants) - ALL PASSING
- **Total**: 125+ differential tests validating Spark parity

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Test Suite (Pytest)                        │
│                  test_differential_v2.py                      │
└───────────────────┬────────────────┬─────────────────────────┘
                    │                │
        ┌───────────▼──────┐  ┌──────▼──────────┐
        │  Spark Reference │  │  Thunderduck    │
        │     Fixture      │  │     Fixture     │
        └───────────┬──────┘  └──────┬──────────┘
                    │                │
     PySpark        │                │        PySpark
     Connect        │                │        Connect
     Client         │                │        Client
                    │                │
        ┌───────────▼──────┐  ┌──────▼──────────┐
        │  Spark Connect   │  │  Thunderduck    │
        │  4.0.1 Server    │  │  Connect Server │
        │  (Native)        │  │  (Java)         │
        │  :15003          │  │  :15002         │
        └──────────────────┘  └──────────────────┘
                │                      │
        ┌───────▼──────┐      ┌───────▼──────┐
        │ Apache Spark │      │   DuckDB     │
        │ Local Engine │      │   Runtime    │
        └──────────────┘      └──────────────┘
```

## Components

### 1. Server Management (`DualServerManager`)

Orchestrates both servers with automatic lifecycle management:

```python
class DualServerManager:
    def __init__(self, thunderduck_port=15002, spark_reference_port=15003):
        ...

    def start_both(self, timeout=120) -> Tuple[bool, bool]:
        """Start both servers, returns (spark_ok, thunderduck_ok)"""

    def stop_both(self):
        """Stop both servers gracefully"""
```

**Location:** `tests/integration/utils/dual_server_manager.py`

### 2. DataFrame Diff Utility (`dataframe_diff.py`)

Detailed row-by-row comparison with:
- Schema validation (names, types, nullability)
- Numeric tolerance (configurable epsilon)
- Detailed diff output showing exact differences

**Location:** `tests/integration/utils/dataframe_diff.py`

### 3. Test Fixtures (`conftest.py`)

Session-scoped fixtures for performance:

```python
@pytest.fixture(scope="session")
def dual_server_manager():
    """Starts both servers, kills on teardown"""

@pytest.fixture(scope="session")
def spark_reference(dual_server_manager):
    """PySpark session connected to Spark 4.0.1"""

@pytest.fixture(scope="session")
def spark_thunderduck(dual_server_manager):
    """PySpark session connected to Thunderduck"""
```

Includes signal handlers for proper cleanup on Ctrl+C.

### 4. Setup Script (`setup-differential-testing.sh`)

One-time setup that:
1. Checks Java/Python prerequisites
2. Downloads and installs Apache Spark 4.0.1
3. Creates Python virtual environment
4. Installs all dependencies
5. Builds Thunderduck

### 5. Run Script (`run-differential-tests-v2.sh`)

Test runner that:
1. Activates virtual environment
2. Kills existing server processes
3. Runs pytest
4. Cleans up on exit (trap handlers)

## Key Design Decisions

### 1. Spark Connect on Both Sides

**Decision:** Both Thunderduck and Spark reference run as Spark Connect servers.

**Rationale:** Ensures both systems are tested via the same protocol, making comparisons fair and reducing protocol-level differences.

### 2. Native Spark Installation (vs Containers)

**Decision:** Use native Spark installation instead of Docker/Podman containers.

**Rationale:**
- Simpler setup and debugging
- Works in all environments (including containers)
- Faster startup time
- Easier to customize configuration

### 3. Session-Scoped Fixtures

**Decision:** Start servers once per test session, not per test.

**Rationale:** Starting servers for each test would add 10+ seconds of overhead per test. Session-scoped fixtures make the full TPC-H suite run in ~20 seconds.

### 4. Port Separation

**Decision:** Thunderduck on 15002, Spark on 15003.

**Rationale:** Allows both servers to run simultaneously without conflicts. Tests can connect to both and compare results.

### 5. Isolated Python Environment

**Decision:** Use a dedicated virtual environment in `tests/integration/.venv/`.

**Rationale:**
- Avoids conflicts with system Python packages
- Ensures reproducible dependency versions
- Isolates PySpark 4.0.1 from other projects

## File Structure

```
tests/
├── scripts/
│   ├── setup-differential-testing.sh     # One-time setup
│   ├── run-differential-tests-v2.sh      # Run tests
│   ├── start-spark-4.0.1-reference.sh    # Start Spark
│   └── stop-spark-4.0.1-reference.sh     # Stop Spark
│
├── integration/
│   ├── .venv/                            # Python virtual environment
│   ├── .env                              # Environment config
│   ├── conftest.py                       # Pytest fixtures
│   ├── test_differential_v2.py           # TPC-H differential tests (23 tests)
│   ├── test_tpcds_differential.py        # TPC-DS differential tests (102 tests)
│   └── utils/
│       ├── dual_server_manager.py        # Server orchestration
│       ├── dataframe_diff.py             # Comparison utility
│       ├── server_manager.py             # Single server manager
│       └── result_validator.py           # Legacy validator

benchmarks/
├── tpch_queries/                         # TPC-H SQL queries (Q1-Q22)
└── tpcds_queries/                        # TPC-DS SQL queries (Q1-Q99 + variants)
```

## Usage

### Quick Start

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run all tests
./tests/scripts/run-differential-tests-v2.sh
```

### Running Specific Tests

```bash
# Activate venv first
source tests/integration/.venv/bin/activate
cd tests/integration

# Run all TPC-H tests (~20 seconds)
python -m pytest test_differential_v2.py -v

# Run all TPC-DS tests (~5 minutes)
python -m pytest test_tpcds_differential.py -k "Batch" -v

# Run specific TPC-H query
python -m pytest test_differential_v2.py::TestTPCH_Q1_Differential -v -s

# Run specific TPC-DS batch
python -m pytest test_tpcds_differential.py::TestTPCDS_Batch1 -v

# Run sanity tests
python -m pytest test_differential_v2.py::TestDifferential_Sanity -v -s
python -m pytest test_tpcds_differential.py::TestTPCDS_Sanity -v -s
```

## Test Output

### Successful Test
```
================================================================================
Comparing: TPC-H Q1
================================================================================
✓ Schemas match
  Reference rows: 4
  Test rows:      4
✓ Row counts match
✓ All 4 rows match

Performance:
  Spark Reference: 2.156s
  Thunderduck:     0.234s
  Speedup:         9.21x

================================================================================
✓ TPC-H Q1 PASSED
================================================================================
```

### Failed Test with Diff
```
================================================================================
DIFF: 1 mismatched rows
================================================================================

--- Row 0 ---
Reference row:
  l_returnflag: N
  sum_charge: 10385578.38

Test row:
  l_returnflag: N
❌ sum_charge: 10385578.37

Differences:
  sum_charge:
    Reference: 10385578.38 (float)
    Test:      10385578.37 (float)
    Diff:      0.0100000000
```

## Server Lifecycle

1. **Before tests start:** `kill_all_servers()` ensures clean slate
2. **Fixture setup:** Starts both servers via `DualServerManager`
3. **Tests run:** Use `spark_reference` and `spark_thunderduck` fixtures
4. **Fixture teardown:** `stop_both()` stops servers gracefully
5. **Signal handlers:** Ctrl+C triggers cleanup via registered signal handlers
6. **Fallback:** `atexit` handler ensures cleanup even on crashes

## Prerequisites

### Required Software
- Python 3.8+ with venv module
- Java 17+
- Maven (for building Thunderduck)
- curl (for downloading Spark)

### Required Data
- TPC-H SF0.01 data in `data/tpch_sf001/`
- TPC-DS SF1 data in `data/tpcds_sf1/`

## Troubleshooting

### Port already in use
```bash
pkill -9 -f "org.apache.spark.sql.connect.service.SparkConnectServer"
pkill -9 -f thunderduck-connect-server
```

### Virtual environment not found
```bash
./tests/scripts/setup-differential-testing.sh
```

### Import errors
```bash
source tests/integration/.venv/bin/activate
python -c "import pyspark; print(pyspark.__version__)"
```

## Future Work

1. ~~Add TPC-DS differential tests~~ ✅ DONE (102 tests passing)
2. Add performance benchmarking and reporting
3. Integrate into CI/CD pipeline
4. Add more detailed performance metrics collection

## TPC-DS Coverage Notes

TPC-DS differential tests cover 94/99 standard queries:
- **Excluded:** Q36 (uses `GROUPING()` in window `PARTITION BY`, unsupported by DuckDB)
- **Variants included:** Q14a/b, Q23a/b, Q24a/b, Q39a/b
- **Total coverage:** 98/99 unique query patterns (99%)

---

**See Also:**
- [Session Management Architecture](SESSION_MANAGEMENT_ARCHITECTURE.md)
- [Arrow Streaming Architecture](ARROW_STREAMING_ARCHITECTURE.md)
