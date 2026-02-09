# TPC-DS Differential Test Report — Strict Mode

**Date**: 2026-02-09
**Thunderduck Commit**: `a34be39`
**Mode**: `THUNDERDUCK_COMPAT_MODE=strict` (DuckDB extension loaded)

## Summary

| Suite | Total | Passed | Failed | Pass Rate |
|-------|-------|--------|--------|-----------|
| SQL Differential | 227 | 143 | 84 | 63% |
| DataFrame Differential | 33 | 26 | 7 | 79% |
| **Combined** | **260** | **169** | **91** | **65%** |

Note: SQL test failures include duplicates — each query runs in both the parameterized class and a batch class. There are **39 unique failing SQL queries**.

## SQL Differential Tests (`test_tpcds_differential.py`)

### Category 1: `spark_decimal_div requires DECIMAL arguments` (11 queries)

The extension function only handles DECIMAL inputs but receives INTEGER or DOUBLE from division expressions in raw SQL.

| Query | Status |
|-------|--------|
| Q4 | FAILED |
| Q7 | FAILED |
| Q11 | FAILED |
| Q23a | FAILED |
| Q23b | FAILED |
| Q39a | FAILED |
| Q39b | FAILED |
| Q58 | FAILED |
| Q66 | FAILED |
| Q74 | FAILED |
| Q85 | FAILED |

**Root cause**: The `/` operator override in the extension only accepts DECIMAL arguments. When raw SQL has `integer / integer`, DuckDB routes to the extension but it throws. The extension needs INTEGER/BIGINT/DOUBLE overloads, or the operator override should be more selective.

### Category 2: `spark_avg DECIMAL overload requires DECIMAL argument` (4 queries)

| Query | Status |
|-------|--------|
| Q22 | FAILED |
| Q26 | FAILED |
| Q27 | FAILED |
| Q54 | FAILED |

**Root cause**: `preprocessSQL()` rewrites all `AVG(` to `spark_avg(`, including AVG over non-DECIMAL types (INTEGER, DOUBLE). The extension only has a DECIMAL overload.

### Category 3: DuckDB INTERNAL Error — cast expression type mismatch (8 queries)

| Query | Status |
|-------|--------|
| Q12 | FAILED |
| Q13 | FAILED |
| Q17 | FAILED |
| Q35 | FAILED |
| Q75 | FAILED |
| Q76 | FAILED |
| Q78 | FAILED |
| Q83 | FAILED |

**Root cause**: DuckDB internal assertion failure when the extension's type casting conflicts with the query's expression types.

### Category 4: Decimal Precision/Scale Mismatch (6 queries)

| Query | Column | Reference | Thunderduck |
|-------|--------|-----------|-------------|
| Q2 | (details not captured) | — | — |
| Q12 | `revenueratio` | DecimalType(38,17) | DecimalType(38,20) |
| Q14b | `sumsales` | DecimalType(38,2) | DecimalType(36,2) |
| Q20 | `revenueratio` | DecimalType(38,17) | DecimalType(38,20) |
| Q64 | `((promotions/total)*100)` | DecimalType(38,19) | DecimalType(38,20) |
| Q70 | `sumsales` | DecimalType(38,2) | DecimalType(36,2) |
| Q98 | `sumsales` | DecimalType(38,2) | DecimalType(36,2) |

**Root cause**: DuckDB computes intermediate DECIMAL precision differently than Spark. The `fixDecimalPrecisionForComplexAggregates()` method doesn't cover all TPC-DS patterns.

### Category 5: Type Mismatch (3 queries)

| Query | Column | Reference | Thunderduck |
|-------|--------|-----------|-------------|
| Q49 | `rnk` | IntegerType | LongType |
| Q70 | `rk` | IntegerType | LongType |
| Q88 | `lochierarchy` | ByteType | LongType |

**Root cause**: DuckDB window functions (`RANK()`, `ROW_NUMBER()`) return BIGINT; Spark returns INTEGER. `GROUPING()` returns BIGINT in DuckDB vs TINYINT in Spark.

### Category 6: Column Name Mismatch (1 query)

| Query | Reference | Thunderduck |
|-------|-----------|-------------|
| Q67 | `syear`, `cnt` | `syear_1`, `cnt_1` |

**Root cause**: Thunderduck deduplicates column names with `_1` suffix in UNION/JOIN results; Spark preserves original names.

### Category 7: Nullable Mismatch (4 queries)

| Query | Issue |
|-------|-------|
| Q44 | Multiple columns: Ref=non-nullable, Test=nullable |
| Q84 | Multiple columns: Ref=non-nullable, Test=nullable |
| Q86 | `customername`: nullable mismatch |
| Q88 | `lochierarchy`: nullable mismatch |

**Root cause**: DuckDB returns all columns as nullable; Spark infers non-nullable for certain expressions. The `fixCountNullable()` method doesn't cover all patterns.

### Category 8: Other (3 queries)

| Query | Error |
|-------|-------|
| Q28 | AssertionError (details not captured) |
| Q90 | Spark DIVIDE_BY_ZERO (Spark bug, not Thunderduck) — **now skipped** |
| Q93 | AssertionError (details not captured) |

---

## DataFrame Differential Tests (`test_tpcds_dataframe_differential.py`)

### Results: 26/33 passed

| Query | Status | Error |
|-------|--------|-------|
| Q1 | PASSED | |
| Q2 | PASSED | |
| Q3 | PASSED | |
| Q4 | PASSED | |
| Q6 | PASSED | |
| Q7 | FAILED | `spark_avg DECIMAL overload requires DECIMAL argument` |
| Q10 | PASSED | |
| Q11 | PASSED | |
| Q13 | FAILED | `spark_avg DECIMAL overload requires DECIMAL argument` |
| Q15 | PASSED | |
| Q16 | PASSED | |
| Q17 | FAILED | `spark_avg DECIMAL overload requires DECIMAL argument` |
| Q19 | PASSED | |
| Q25 | PASSED | |
| Q26 | FAILED | `spark_avg DECIMAL overload requires DECIMAL argument` |
| Q29 | PASSED | |
| Q32 | FAILED | `TypeError: float() argument must be a string or a real number, not 'NoneType'` — **now fixed** |
| Q37 | PASSED | |
| Q40 | PASSED | |
| Q42 | PASSED | |
| Q43 | PASSED | |
| Q46 | PASSED | |
| Q48 | PASSED | |
| Q52 | PASSED | |
| Q55 | PASSED | |
| Q59 | PASSED | |
| Q65 | PASSED | |
| Q68 | PASSED | |
| Q73 | PASSED | |
| Q79 | PASSED | |
| Q82 | PASSED | |
| Q85 | FAILED | `spark_avg DECIMAL overload requires DECIMAL argument` |
| Q92 | FAILED | `TypeError: float() argument must be a string or a real number, not 'NoneType'` — **now fixed** |

---

## Action Items

1. **Extension: Add non-DECIMAL overloads** to `spark_decimal_div`, `spark_sum`, `spark_avg` — INTEGER, BIGINT, DOUBLE inputs (fixes ~15 SQL + 5 DataFrame queries)
2. **Extension: Fix internal cast assertion** — debug the type mismatch in DuckDB's expression casting (fixes 8 queries)
3. **Decimal precision**: Extend `fixDecimalPrecisionForComplexAggregates()` for TPC-DS patterns (fixes ~6 queries)
4. **Type mapping**: RANK/ROW_NUMBER → CAST to INTEGER; GROUPING → CAST to TINYINT (fixes 3 queries)
5. **Column dedup**: Match Spark's column naming in UNION/JOIN (fixes Q67)
6. **Nullable inference**: Extend nullable fixup for more expression patterns (fixes 4 queries)
7. ~~**Q90**: Skip (Spark bug)~~ Done
8. ~~**Q32/Q92**: Fix float(None) test code~~ Done
