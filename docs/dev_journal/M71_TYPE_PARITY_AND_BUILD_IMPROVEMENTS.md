# M71: Type Parity, Build Improvements, and Test Fixes

**Date:** 2026-01-06
**Status:** Complete

## Summary

This milestone focused on DuckDB-Spark type compatibility (particularly HUGEINT handling), macOS build support, test suite improvements, and removal of SparkSQL dependencies from E2E tests. Key contributions from community member André Fonseca.

## Work Completed

### 1. DuckDB HUGEINT Type Handling

**Commit:** `9d8219a`

DuckDB's `SUM()` function returns `HUGEINT` (128-bit integer) to prevent overflow, but Spark Connect protocol only supports up to 64-bit integers. This required explicit casting.

**Changes:**
- Fixed `sum_distinct` translator to wrap with `CAST(... AS BIGINT)`
- Updated `SchemaInferrer` comments to explain Spark Connect limitation
- Created `docs/architect/TYPE_MAPPING.md` documenting integer type mapping and overflow behavior

**Files Modified:**
| File | Change |
|------|--------|
| `core/.../functions/FunctionRegistry.java` | Add CAST wrapper to sum_distinct |
| `core/.../schema/SchemaInferrer.java` | Update HUGEINT comment |
| `docs/architect/TYPE_MAPPING.md` | **NEW** - Type mapping documentation |

### 2. Integer Overflow Differential Tests

**Commit:** `4979708`

Added comprehensive differential tests verifying Thunderduck matches Spark 4.0 overflow semantics:

- Both engines throw exceptions on SUM overflow (ANSI mode default)
- Return types match: `SUM→LongType`, `COUNT→LongType`, `AVG→DoubleType`
- Boundary values produce identical results
- Arithmetic expressions behave consistently

**Key Finding:** Spark throws `ArithmeticException`, Thunderduck throws `SparkConnectGrpcException` with DuckDB conversion error - both prevent silent overflow.

**File Added:** `tests/src/test/python/differential/test_overflow_differential.py` (495 lines)

### 3. macOS Build Support

**Commit:** `02441e1`

Added Maven profile for macOS builds where Gatekeeper blocks protoc binaries from Maven Central.

**Usage:**
```bash
mvn clean package -DskipTests -Puse-system-protoc
```

**Prerequisites:**
```bash
brew install protobuf grpc
```

**Files Modified:**
| File | Change |
|------|--------|
| `connect-server/pom.xml` | Add `use-system-protoc` profile |
| `README.md` | Add macOS Build Requirements section |

### 4. Test Suite Case-Sensitivity Fixes

**Commits:** `22ba1d1` (André Fonseca), `c0c680e`

Fixed 25+ failing unit tests due to SQL function name casing differences:

- Use `containsIgnoringCase()` for SQL function comparisons
- Update assertions for CAST-wrapped SUM functions
- Update DuckDB date function syntax assertions
- Remove tests checking for specific function name casing

**PR:** #7 (andreAmorimF/fix-tests-case-check)

### 5. Remove SparkSQL Dependencies from E2E Tests

**Commit:** `cfc1c0f` (André Fonseca)

Removed SparkSQL dependencies from DataFrame API tests to improve test isolation.

**PR:** #8 (andreAmorimF/removing-spark-sql-test)

**Files Modified:**
| File | Change |
|------|--------|
| `tests/.../e2e/SparkConnectE2ETest.java` | Skip SparkSQL-dependent test |
| `tests/.../python/thunderduck_e2e/test_dataframes.py` | Remove SparkSQL usage |
| `tests/.../python/thunderduck_e2e/test_runner.py` | Update test runner |

### 6. Test Fix for sum_distinct CAST Wrapper

**Commit:** `6602cc1`

Updated `testSumDistinctFunctionTranslation` assertion to expect `CAST(SUM(DISTINCT amount) AS BIGINT)` matching the implementation from commit `9d8219a`.

## Key Insights

1. **Spark Connect Protocol Limitation:** 128-bit integers (HUGEINT) are not supported in the protocol, requiring explicit BIGINT casts for aggregate functions that could produce large results.

2. **Type Parity is Critical:** Per CLAUDE.md, Thunderduck must match Spark's return types exactly - not just produce equivalent values. The HUGEINT→BIGINT cast ensures client code expecting BIGINT doesn't fail.

3. **Overflow Behavior Parity:** Both Spark 4.0 (ANSI mode) and DuckDB throw on integer overflow rather than wrapping silently. This is correct behavior for data integrity.

4. **Multi-module Maven Builds:** When tests depend on core module changes, `mvn clean install -DskipTests` is required before running tests to ensure the tests module picks up the latest core module from the local Maven repository.

## Community Contributions

- **André Fonseca** (@andreAmorimF): PRs #7 and #8 improving test reliability and removing SparkSQL dependencies

## Commits (chronological)

- `02441e1` - Add macOS build support with use-system-protoc profile
- `22ba1d1` - Fixing tests failing due to the uppercase string comparison (André Fonseca)
- `c0c680e` - Fix 25 failing unit tests with case-insensitive assertions
- `cfc1c0f` - Removing spark sql dependencies on dataframe api tests (André Fonseca)
- `9d8219a` - Document DuckDB HUGEINT handling and fix sum_distinct CAST
- `4979708` - Add differential tests for integer overflow behavior
- `6602cc1` - Fix sum_distinct test to expect CAST wrapper for Spark BIGINT parity
