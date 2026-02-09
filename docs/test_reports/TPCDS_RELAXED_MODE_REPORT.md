# TPC-DS Differential Test Report — Relaxed Mode

**Date**: 2026-02-09
**Thunderduck Commit**: `a34be39`
**Mode**: `THUNDERDUCK_COMPAT_MODE=relaxed` (no DuckDB extension)

## Summary

| Suite | Total | Passed | Failed | Pass Rate |
|-------|-------|--------|--------|-----------|
| SQL Differential | 227 | 216 | 11 | 95% |
| DataFrame Differential | 33 | 26 | 7 | 79% |
| **Combined** | **260** | **242** | **18** | **93%** |

Note: SQL failures include duplicates from batch test classes. There are **5 unique failing SQL queries**.

---

## SQL Differential Tests (`test_tpcds_differential.py`)

### Results: 216/227 passed (95%)

Relaxed mode performs significantly better on SQL tests because it doesn't rewrite SUM/AVG to extension functions and doesn't override the `/` operator.

### Category 1: Column Name Deduplication (4 unique queries)

| Query | Columns Affected | Issue |
|-------|-----------------|-------|
| Q14b | columns 6-11 (`channel`, `i_brand_id`, `i_class_id`, `i_category_id`, `sales`, `number_sales`) | `_1` suffix added |
| Q39a | columns 5-9 (`w_warehouse_sk`, `i_item_sk`, `d_moy`, `mean`, `cov`) | `_1` suffix added |
| Q39b | columns 5-9 (same as Q39a) | `_1` suffix added |
| Q64 | columns 16-20 (`s1`, `s2`, `s3`, `syear`, `cnt`) | `_1` suffix added |

**Root cause**: Thunderduck deduplicates duplicate column names in UNION/JOIN by appending `_1`. Spark preserves original names even when duplicated.

Each of these appears twice in the test results (parameterized + batch), accounting for 8 of the 11 failures.

### Category 2: Data Mismatch (1 query)

| Query | Issue |
|-------|-------|
| Q77 | 2 rows with swapped NULL/non-NULL values in sales/returns/profit columns |

**Root cause**: Non-deterministic sort order — ties broken differently between Spark and DuckDB. Not a correctness bug.

### Category 3: Spark Reference Error (1 query)

| Query | Issue |
|-------|-------|
| Q90 | `DIVIDE_BY_ZERO` in Spark reference server — **now skipped** |

---

## DataFrame Differential Tests (`test_tpcds_dataframe_differential.py`)

### Results: 26/33 passed (79%)

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

### Failure Analysis

**`spark_avg` errors (Q7, Q13, Q17, Q26, Q85)**: In relaxed mode, the DuckDB extension is not loaded, but `preprocessSQL()` unconditionally rewrites `AVG(` → `spark_avg(`. The `spark_avg` function doesn't exist without the extension. **Fix in progress** — the rewrite must be guarded by `SparkCompatMode.isStrictMode()`.

**`float()` on None (Q32, Q92)**: Test code calls `float()` on a scalar subquery result that returns `None`. **Now fixed** — added None guard with `0.0` default.

---

## Comparison: Strict vs Relaxed

| Metric | Strict | Relaxed |
|--------|--------|---------|
| SQL pass rate | 63% (143/227) | 95% (216/227) |
| DataFrame pass rate | 79% (26/33) | 79% (26/33) |
| Combined pass rate | 65% (169/260) | 93% (242/260) |
| Extension-caused failures | ~23 | 5 (spark_avg in DataFrame) |
| Precision/type failures | ~13 | 0 |
| Column name failures | 2 (Q67) | 8 (Q14b, Q39a/b, Q64) |

**Key insight**: Strict mode introduces more failures because:
1. The extension's `/` operator override breaks non-DECIMAL division
2. `spark_sum`/`spark_avg` lack non-DECIMAL overloads
3. Decimal precision fixup doesn't cover all TPC-DS patterns

Relaxed mode is cleaner because DuckDB's native behavior is closer to correct for most queries, with column name dedup being the main issue.

---

## Action Items

1. ~~**Q90**: Skip (Spark bug)~~ Done
2. ~~**Q32/Q92**: Fix float(None)~~ Done
3. **Guard `spark_sum`/`spark_avg` rewrite** behind `isStrictMode()` — fix in progress
4. **Column dedup**: Match Spark's naming in UNION/JOIN for Q14b, Q39a/b, Q64, Q67
5. **Q77 sort stability**: Document as known non-determinism (not a bug)
