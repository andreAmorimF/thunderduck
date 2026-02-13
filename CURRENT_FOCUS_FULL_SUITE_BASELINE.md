# Full Differential Test Suite Baseline

**Date**: 2026-02-13 (updated)

## Relaxed Mode

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **746 passed, 0 failed, 2 skipped** (748 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL + DataFrame passing
- **Lambda HOFs**: 27/27 (100%) — transform, filter, exists, forall, aggregate
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)
- **0 regressions**: T2+T3 fixes resolved prior relaxed regressions without introducing new ones

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → 737/0/2 → 746/0/2 → 739/7/2 → **746/0/2**

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **712 passed, 36 failed, 0 skipped** (748 total)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → 658/81 → 665/83 → 684/64 → 685/63 → **712/36**

### What Changed (685/63 → 712/36)

#### T2 fix: Decimal literal parsing + CTE schema propagation

| Component | Change |
|-----------|--------|
| `Literal.java` | Added `BigDecimal` factory method with Spark `DecimalType.fromBigDecimal` precision/scale rules; added `DecimalType` case in `toSQL()` |
| `SparkSQLAstBuilder.java` | Parse `DecimalLiteralContext` as `Literal(BigDecimal, DecimalType)` instead of `RawSQLExpression`; CTE schema registry for forward-propagation during parsing |

**Impact**: Fixed ~20 TPC-DS SQL failures caused by decimal literals being treated as unresolved types, and CTE references losing schema information.

#### T3 fix: CAST wrapping for DECIMAL type in WithColumns path

| Component | Change |
|-----------|--------|
| `SQLGenerator.java` | `generateExpressionWithCast()` adds CAST to DECIMAL when TypeInferenceEngine resolves DECIMAL but expression-level `dataType()` returns non-DECIMAL (e.g., DOUBLE from unresolved columns) |

**Impact**: Fixed ~7 failures where DataFrame division/arithmetic expressions returned DOUBLE instead of DECIMAL in strict mode. Also fixed 3 TPC-DS DataFrame tests (Q12, Q20, Q98) and resolved relaxed mode regressions (extension functions no longer emitted in relaxed mode).

---

### Root Cause Clustering (36 failures)

| # | Root Cause | Count | Fix Strategy |
|---|-----------|-------|-------------|
| **N2** | Nullable over-broadening | **~12** | struct/map field access, grouping functions, VALUES, lambda |
| **D1** | TPC-DS gRPC / query errors | **~8** | Q4, Q9, Q11, Q17, Q61, Q66, Q74 gRPC errors; Q39a/Q39b decimal-div |
| **S1** | StringType fallback (MAP/STRUCT) | **~7** | SchemaInferrer MAP/STRUCT parsing, function return types |
| **T2** | Decimal precision residual | **~5** | Remaining precision edge cases in TPC-DS |
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |
| **T1** | TPC-H SQL residual | **2** | Q13, Q14 assertion failures |

### Failures by Test File

| File | Failures | Primary Root Cause |
|------|----------|-------------------|
| `test_tpcds_differential.py` (SQL) | 13 | D1 + T2 (Q2, Q4, Q9, Q11, Q17, Q27, Q28, Q59, Q61, Q66, Q70, Q74, Q86) |
| `test_complex_types_differential.py` | 7 | N2 (nullable) + S1 (map/struct type) |
| `test_dataframe_functions.py` | 4 | S1 (flatten, map_keys, map_values, map_from_arrays) |
| `test_multidim_aggregations.py` | 4 | N2 (grouping nullable) |
| `test_differential_v2.py` (TPC-H SQL) | 2 | T1 (Q13, Q14) |
| `test_tpcds_differential.py` (Q39a, Q39b) | 2 | D1 (decimal-div type inference) |
| `test_overflow_differential.py` | 2 | X2 |
| `test_lambda_differential.py` | 1 | N2 (sql_transform_with_table nullable) |
| `test_simple_sql.py` | 1 | N2 (VALUES clause) |
| **TOTAL** | **36** | |

Zero-failure test files (all passing):
`test_tpch_differential.py`, `test_tpcds_dataframe_differential.py`, `test_to_schema_differential.py`, `test_window_functions.py`, `test_joins_differential.py`, `test_datetime_functions_differential.py`, `test_array_functions_differential.py`, `test_sql_expressions_differential.py`, `test_string_functions_differential.py`, `test_math_functions_differential.py`, `test_column_operations_differential.py`, `test_null_handling_differential.py`, `test_subquery_differential.py`, `test_ddl_parser_differential.py`, `test_dataframe_ops_differential.py`, `test_conditional_differential.py`, `test_type_literals_differential.py`, `test_type_casting_differential.py`, `test_using_joins_differential.py`

---

### Prioritized Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | N2: Nullable residual | ~12 | Hard | SchemaInferrer MAP/STRUCT containsNull, grouping/grouping_id, VALUES |
| **P2** | D1: TPC-DS gRPC / decimal-div | ~8 | Medium | Debug Q4/Q11/Q66/Q74 gRPC errors; Q39a/Q39b decimal-div fix |
| **P3** | S1: StringType residual | ~7 | Medium | SchemaInferrer MAP/STRUCT type parsing, function return types (flatten, map_*) |
| **P4** | T2: Decimal precision residual | ~5 | Medium | Remaining precision edge cases in complex TPC-DS queries |
| **P5** | X2: Overflow | 2 | Low | Add overflow detection to spark_sum extension |
| **P6** | T1: TPC-H Q13/Q14 | 2 | Low | Investigate assertion failures |

---

### Architecture Goal: Zero-Copy Strict Mode

**Two invariants that must hold simultaneously:**

```
Apache Spark 4.1 types (authoritative truth)
  <- must match -> DuckDB output (shaped by SQL generation + extension functions)
  <- must match -> inferSchema() (type inference in logical plan)
```

1. **Spark is the authority.** The target types, precision, scale, and nullability are defined by what Apache Spark 4.1 returns. We don't approximate -- we match exactly.

2. **DuckDB output must match Spark at the engine level.** This is achieved through SQL generation (CASTs, `AS` aliases, function rewrites) and DuckDB extension functions (`spark_avg`, `spark_sum`, `spark_decimal_div`). No post-hoc Arrow rewriting.

3. **`inferSchema()` must match DuckDB output.** The logical plan's type inference must return exactly the same types that the generated SQL will produce when executed by DuckDB.

**`SchemaCorrectedBatchIterator` has been removed.** DuckDB Arrow batches flow through with no schema patching. All type correctness is achieved at SQL generation time. Zero Arrow vector copying. Zero runtime type conversion.

---

## Performance Optimization: `__int128` Accumulators (2026-02-12)

Changed `SparkSumDecimalState` and `SparkAvgDecimalState` accumulators from `hugeint_t` to `__int128` in `spark_aggregates.hpp`.

| Operation | Before | After |
|-----------|--------|-------|
| SUM/AVG per-row accumulation | `hugeint_t::operator+=` (non-inline library call) | `__int128 +=` (inline ADD/ADC, 2 instructions) |
| SUM/AVG ConstantOperation | `Hugeint::Convert` + `hugeint_t::operator*` (2 non-inline calls) | `__int128 *` (inline MUL, 1 instruction) |
| SUM/AVG Combine | `hugeint_t::operator+=` (non-inline) | `__int128 +=` (inline) |
| SUM Finalize | `HugeintToInt128` conversion + write | Direct write from `__int128` state |
| AVG Finalize | `HugeintToInt128` conversion + division + write | Division + write (skip conversion) |

Input arrives as `hugeint_t` (DuckDB's type), converted once per row via `HugeintToInt128()` (already inline in `wide_integer.hpp`). All arithmetic stays in `__int128` until finalize, where `WriteAggResult` converts back to the target physical type.
