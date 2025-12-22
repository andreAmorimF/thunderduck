# M54: Date/Time Differential Tests

**Date**: 2025-12-22
**Status**: Complete

## Summary

Added comprehensive differential tests for date/time functions, comparing Thunderduck against Apache Spark 4.0.1. Fixed multiple issues in Arrow type handling and date function translations to achieve Spark parity.

## Test Results

- **18 tests passing** (90%)
- **2 tests skipped** (10%) - pending complex implementations
- Test file: `tests/integration/differential/test_datetime_functions_differential.py`
- Test group: `datetime` in run-differential-tests-v2.sh

## Key Fixes

### 1. Arrow DATE/TIMESTAMP Type Handling

Fixed `SQLGenerator.getArrowValue()` to handle all Arrow date/time vector types:

| Vector Type | Fix Applied |
|-------------|-------------|
| `DateDayVector` | Already handled |
| `DateMilliVector` | Added - dates as milliseconds |
| `TimeStampMicroTZVector` | Added - timestamps with timezone (microseconds) |
| `TimeStampMilliTZVector` | Added - timestamps with timezone (milliseconds) |
| `TimeStampNanoTZVector` | Added - timestamps with timezone (nanoseconds) |
| `TimeStampSecTZVector` | Added - timestamps with timezone (seconds) |
| `TimeStampMicroVector` | Fixed - use Instant instead of java.sql.Timestamp |
| `TimeStampMilliVector` | Added |
| `TimeStampNanoVector` | Added |
| `TimeStampSecVector` | Added |

Also added `formatSQLValue()` handlers for `java.time.Instant` and `java.time.LocalDateTime`.

### 2. Date Function Translations

Updated `FunctionRegistry.java` with custom translators:

| Spark Function | Issue | DuckDB Translation |
|----------------|-------|-------------------|
| `dayofweek` | DuckDB: 0-6, Spark: 1-7 | `dayofweek(x) + 1` |
| `datediff` | Different arg order | `datediff('day', end, start)` |
| `date_sub` | No direct equivalent | `CAST((date - INTERVAL 'n days') AS DATE)` |
| `add_months` | No direct equivalent | `CAST((date + INTERVAL 'n months') AS DATE)` |
| `date_format` | Different format syntax | Format string conversion (yyyy→%Y, MM→%m, etc.) |
| `to_date` | Needs format parsing | `strptime()` + CAST for format-aware parsing |
| `to_timestamp` | Needs format parsing | `strptime()` + CAST for format-aware parsing |
| `unix_timestamp` | Different behavior | `epoch()` for timestamps, `strptime()` for strings |
| `from_unixtime` | Different return type | `strftime(to_timestamp(epoch), format)` |
| `date_trunc` | DuckDB returns DATE | `CAST(date_trunc(...) AS TIMESTAMP)` |

### 3. Type Inference

Added return type inference in `ExpressionConverter.inferFunctionReturnType()`:

| Function | Return Type |
|----------|-------------|
| `months_between` | DoubleType |
| `to_date`, `last_day`, `next_day` | DateType |
| `to_timestamp`, `date_trunc` | TimestampType |
| `unix_timestamp` | LongType |
| `from_unixtime` | StringType |
| `date_add`, `date_sub`, `add_months` | DateType |
| `datediff` | IntegerType |

### 4. Function Metadata

Added `FunctionMetadata` entries for proper schema resolution.

## Test Coverage

### Passing Tests (18)

**Date Extraction (8 tests)**:
- `year`, `month`, `day`, `hour`, `minute`, `second`
- `dayofweek` (with +1 offset fix)
- `dayofyear`, `quarter`, `weekofyear`

**Date Arithmetic (4 tests)**:
- `date_add`, `date_sub`, `datediff`, `add_months`

**Date Formatting (4 tests)**:
- `date_format`, `to_date`, `to_timestamp`, `unix_timestamp`/`from_unixtime`

**Date Truncation (2 tests)**:
- `date_trunc`, `last_day`

### Skipped Tests (2)

| Test | Reason |
|------|--------|
| `months_between` | Requires complex fractional month calculation matching Spark's exact algorithm |
| `next_day` | DuckDB doesn't have native `next_day` function - requires custom implementation |

## Files Modified

| File | Changes |
|------|---------|
| `core/src/main/java/com/thunderduck/generator/SQLGenerator.java` | Arrow vector type handling, formatSQLValue for Instant |
| `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java` | Custom translators for date functions, format string conversion |
| `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java` | Return type inference for date functions |
| `tests/integration/differential/test_datetime_functions_differential.py` | New test file with 20 tests |
| `tests/scripts/run-differential-tests-v2.sh` | Added `datetime` test group |

## Running the Tests

```bash
# Run datetime tests only
./tests/scripts/run-differential-tests-v2.sh datetime

# Or directly with pytest
cd tests/integration
python3 -m pytest differential/test_datetime_functions_differential.py -v
```

## Future Work

1. **months_between**: Implement Spark's exact fractional month algorithm
2. **next_day**: Implement custom translator using DuckDB date arithmetic
3. **current_date/current_timestamp**: Add tests (deterministic comparison needed)

## Key Learnings

1. **Arrow vector types vary**: Spark Connect can send timestamps in multiple formats (micro, milli, nano, with/without timezone). Must handle all variants.

2. **Format string conversion**: Spark uses Java SimpleDateFormat (`yyyy-MM-dd`), DuckDB uses C strftime (`%Y-%m-%d`). Must convert between them.

3. **Type coercion matters**: DuckDB's `date_trunc` returns DATE when truncating to 'day', but Spark always returns TIMESTAMP. Explicit CAST needed.

4. **Day-of-week conventions differ**: DuckDB uses 0=Sunday, Spark uses 1=Sunday. Need +1 offset.
