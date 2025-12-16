# M39: Differential Testing Framework V2

**Date:** 2025-12-16
**Status:** Complete

## Summary

Implemented a comprehensive differential testing framework (V2) that compares Thunderduck against Apache Spark 4.0.1 using Spark Connect protocol on both sides. All 23 TPC-H tests pass, validating Spark parity.

## What Was Built

### Infrastructure

1. **Automated Setup Script** (`tests/scripts/setup-differential-testing.sh`)
   - Downloads and installs Apache Spark 4.0.1 to `~/spark/spark-4.0.1`
   - Creates Python virtual environment at `tests/integration/.venv/`
   - Installs all Python dependencies (pytest, pyspark==4.0.1, pandas, pyarrow, grpcio, etc.)
   - Builds Thunderduck
   - Saves configuration to `tests/integration/.env`

2. **Test Runner Script** (`tests/scripts/run-differential-tests-v2.sh`)
   - Activates virtual environment
   - Kills any existing server processes before tests
   - Sets up cleanup traps for EXIT, INT, TERM signals
   - Runs pytest with proper environment

3. **Spark Reference Server Scripts**
   - `start-spark-4.0.1-reference.sh` - Start Spark on port 15003
   - `stop-spark-4.0.1-reference.sh` - Stop Spark server

### Python Components

1. **DualServerManager** (`tests/integration/utils/dual_server_manager.py`)
   - Orchestrates both servers (Spark 4.0.1 + Thunderduck)
   - Automatic startup/shutdown with health checks
   - Port management (15002 for Thunderduck, 15003 for Spark)

2. **DataFrame Diff Utility** (`tests/integration/utils/dataframe_diff.py`)
   - Row-by-row comparison with detailed output
   - Schema validation (names, types, nullability)
   - Numeric tolerance support (epsilon)

3. **Updated conftest.py**
   - Signal handlers for SIGINT/SIGTERM cleanup
   - atexit handler as fallback
   - Session-scoped fixtures for performance
   - Kills existing servers before starting fresh

### Test Suite

`test_differential_v2.py` with:
- TPC-H Q1-Q22 differential tests
- Sanity test for basic validation
- Performance comparison metrics (speedup)

## Test Results

**ALL 23 TESTS PASSED** in 20.63 seconds

- TPC-H Q1-Q22: All passed
- Sanity test: Passed
- Performance: Thunderduck consistently 5-10x faster than Spark

Example from Q1:
```
Performance:
  Spark Reference: 0.076s
  Thunderduck:     0.013s
  Speedup:         5.99x
```

## Key Design Decisions

1. **Native Spark (vs Containers)**: Due to Podman limitations in the development environment, we use a native Spark installation. This is actually simpler and works in all environments.

2. **Session-Scoped Fixtures**: Servers start once per test session (~10 seconds) rather than per test, enabling the full suite to complete in ~20 seconds.

3. **Isolated Python Environment**: Dedicated venv avoids conflicts with system packages and ensures reproducible dependencies.

4. **Signal Handlers**: Proper cleanup on Ctrl+C prevents orphaned server processes.

## Files Created/Modified

### New Files
```
tests/scripts/
├── setup-differential-testing.sh
├── start-spark-4.0.1-reference.sh
└── stop-spark-4.0.1-reference.sh

tests/integration/utils/
├── dual_server_manager.py
└── dataframe_diff.py

docs/architect/
└── DIFFERENTIAL_TESTING_ARCHITECTURE.md
```

### Modified Files
```
tests/scripts/run-differential-tests-v2.sh  # Rewritten for venv support
tests/integration/conftest.py               # Added signal handlers, cleanup
README.md                                   # Added differential testing section
```

### Removed Files (Obsolete)
```
# Workspace root
DIFFERENTIAL_TESTING_V2_SUMMARY.md             # Moved to docs/architect
QUICKSTART_DIFFERENTIAL_V2.md                  # Consolidated into README

# Architecture docs
docs/architect/TAIL_CHAINING_DESIGN.md         # Obsolete per M33 refactor
docs/architect/DIFFERENTIAL_TESTING_GUIDE.md   # Referenced Spark 3.5.3
docs/architect/TESTING_STRATEGY.md             # 1200+ line theoretical doc, referenced Spark 3.5
docs/architect/SESSION_MANAGEMENT_REDESIGN.md  # Rewritten as SESSION_MANAGEMENT_ARCHITECTURE.md

# Container infrastructure (switched to native Spark)
tests/docker/                                  # Entire directory removed
tests/scripts/build-spark-connect-container.sh
tests/scripts/start-spark-connect-container.sh
tests/scripts/stop-spark-connect-container.sh

# Old Spark 3.5.x scripts
tests/scripts/start-spark-connect.sh           # Referenced Spark 3.5.3
tests/scripts/stop-spark-connect.sh
tests/scripts/run-differential-tests.sh        # Superseded by V2
tests/scripts/run-tpc-spark-connect-tests.sh
```

### Updated Files
```
README.md                                      # Added differential testing section, fixed doc links
tests/integration/README.md                    # Updated for PySpark 4.0.1, new test structure
docs/architect/PROTOBUF_AND_ARROW_CONFIGURATION.md  # Updated for Spark 4.0.x
```

## Usage

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run all differential tests
./tests/scripts/run-differential-tests-v2.sh

# Or manually
source tests/integration/.venv/bin/activate
cd tests/integration
python -m pytest test_differential_v2.py -v
```

## Lessons Learned

1. **Containers aren't always better**: Native installations can be simpler and more reliable in some environments.

2. **Signal handling is critical**: Tests that start external processes must handle interrupts properly to avoid orphaned processes.

3. **Virtual environments**: Essential for reproducible Python dependencies, especially with version-specific packages like pyspark==4.0.1.

4. **Session-scoped fixtures**: Key for test performance when setup is expensive.

## Next Steps

1. Add TPC-DS differential tests
2. Integrate into CI/CD pipeline
3. Add performance benchmarking reports
4. Consider adding data type validation beyond row comparison

---

**Related:**
- M33: Tail Operation Streaming Fix
- M38: Window Function SQL Generation Fix
- [Differential Testing Architecture](../architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)
