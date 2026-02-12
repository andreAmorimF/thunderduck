# Full Differential Test Suite Baseline

**Date**: 2026-02-12 (updated)

## Relaxed Mode (Complete)

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **737 passed, 0 failed, 2 skipped** (739 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL queries passing
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → **737/0/2** (unchanged)

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **658 passed, 81 failed, 0 skipped** (839 collected, 1 deselected, 838 ran)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → **658/81**

### What Changed (638/88 → 658/81): Remove SchemaCorrectedBatchIterator

| Change | What it fixed | Tests fixed | Regressions |
|--------|--------------|-------------|-------------|
| Remove `SchemaCorrectedBatchIterator` + unused `SparkCompatMode` import | Eliminated nullable over-broadening from incorrect schema patching; DuckDB Arrow batches now flow through as-is | **7** (4 lambda, 3 multidim) | 0 |
| DDL parser tests no longer excluded | DDL parser gRPC timeout resolved; 13 tests now run and pass | **13** (all pass) | 0 |

**Net movement**: 88 → 81 failures (-7), 638 → 658 passed (+20, includes 13 formerly-excluded DDL tests now passing)

### Delta: What flipped (88 → 81)

**7 tests fixed** (FAIL → PASS):

| Test File | Old | New | Fix |
|-----------|-----|-----|-----|
| `test_lambda_differential.py` | 18 | 14 | -4: nullable patching removal |
| `test_multidim_aggregations.py` | 7 | 4 | -3: nullable patching removal |

**13 tests newly running** (excluded → PASS):

| Test File | Old | New | Reason |
|-----------|-----|-----|--------|
| `test_ddl_parser_differential.py` | excluded (gRPC timeout) | 0 failures | DDL parser timeout resolved |

**0 regressions** (no PASS → FAIL).

---

### Root Cause Clustering (81 failures)

| # | Root Cause | Count | Fix Strategy |
|---|-----------|-------|-------------|
| **N2** | Nullable over-broadening | **~23** | Refine: don't blanket-nullable all sources; propagate per-column |
| **S1** | StringType fallback (missing type inference) | **~15** | Implement type inference for unhandled expression types |
| **T2** | Decimal precision/scale mismatch | **~15** | SUM precision formula, scale propagation |
| **T3** | DOUBLE ↔ DECIMAL confusion | **~10** | AVG/division return type mapping |
| **D1** | spark_decimal_div BinaryExpression path | **4** | Re-land integer-to-DECIMAL promotion fix |
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |
| **R1** | Misc (toSchema, stddev, VALUES types) | **~3** | Individual fixes |

---

### N2: Nullable Over-broadening (~23 tests) — reduced from ~30

The `withAllNullable()` change forces ALL parquet source columns to `nullable=true`. Removing `SchemaCorrectedBatchIterator` helped (DuckDB's native all-nullable-true is now the baseline), but `inferSchema()` still reports incorrect nullable flags for AnalyzePlan responses.

| Sub-group | Tests | Pattern |
|-----------|-------|---------|
| **Lambda functions** | 14 | Lambda results become nullable because source arrays are nullable |
| **Complex types** | ~5 | Struct field access, map access — nullable propagates through extraction |
| **TPC-DS SQL** | ~4 | Nullable mismatches stacking with existing decimal issues |

### S1: StringType Fallback (~15 tests) — persistent

Type inference returns `StringType` instead of the correct type for several expression patterns.

| Function/Pattern | Expected Type | Tests |
|-----------------|--------------|-------|
| `split()` | `ArrayType(StringType)` | 1 |
| `flatten()` | `ArrayType(IntegerType)` | 1 |
| `map_keys/values/entries` | Correct map element types | 3 |
| `map_from_arrays` | `MapType(...)` | 1 |
| Struct field access (strict) | Struct field type | 3 |
| Map key access (strict) | Value type | 2 |
| `UpdateFields` (`with_field`) | `StructType(...)` | 2 |
| `grouping()` | `ByteType` | 3 |
| `toSchema` coercion | Target schema type | 1 |

### T2: Decimal Precision/Scale (~15 tests) — persistent

`DecimalType(37,2)` vs `DecimalType(38,2)` — SUM precision off by 1. Also wrong scale in division results (e.g., `DecimalType(38,12)` vs `DecimalType(38,6)`).

Affected TPC-DS: Q22, Q93, Q14b, Q67, Q86, and others with aggregate DECIMAL columns.

### T3: DOUBLE ↔ DECIMAL Confusion (~10 tests) — persistent

| Direction | Example | Affected Tests |
|-----------|---------|---------------|
| Returns DOUBLE, Spark expects DECIMAL | Q64, Q66, Q80 (`sales`/`returns`/`profit` columns) | ~6 |
| Returns DECIMAL, Spark expects DOUBLE | Q17 (`avg_yearly`), Q24a/Q24b | ~4 |

Root cause: AVG over DECIMAL returns DECIMAL in DuckDB but DOUBLE in Spark. Division type inference doesn't match Spark's widening rules.

### D1: spark_decimal_div BinaryExpression Path (4 tests) — persistent, fix reverted

Q23a/Q23b fail through `BinaryExpression.generateStrictModeDivision()` — a second code path that emits `spark_decimal_div`. The fix for integer-to-DECIMAL promotion was merged then reverted (`64f1826`).

Additionally Q39a/Q39b fail with raw SQL StringType issues in strict mode.

### X2: Overflow Behavior (2 tests) — persistent

DuckDB's `spark_sum` extension doesn't throw on integer overflow (promotes to HUGEINT internally). Spark throws `ArithmeticException`. Tests: `test_sum_overflow_both_throw_exception`, `test_sum_negative_overflow_both_throw_exception`.

---

### Failures by Test File

| File | Failures | Delta (from 88) | Primary Root Cause |
|------|----------|------------------|--------------------|
| `test_tpcds_differential.py` (SQL) | 39 | 0 | T2 + T3 + D1 |
| `test_lambda_differential.py` | 14 | -4 | N2 |
| `test_complex_types_differential.py` | 10 | 0 | N2 + S1 |
| `test_dataframe_functions.py` | 6 | 0 | S1 (split, flatten, map functions) |
| `test_multidim_aggregations.py` | 4 | -3 | N2 + S1 (grouping) |
| `test_differential_v2.py` (TPC-H SQL) | 3 | 0 | T2 + T3 |
| `test_overflow_differential.py` | 2 | 0 | X2 |
| `test_simple_sql.py` | 1 | 0 | N2 |
| `test_type_casting_differential.py` | 1 | 0 | T2 |
| `test_to_schema_differential.py` | 1 | 0 | S1 |
| `test_ddl_parser_differential.py` | 0 | — | Was excluded, now passing |
| **TOTAL** | **81** | **-7** | |

Zero-failure test files (all passing):
`test_tpcds_dataframe_differential.py`, `test_tpch_differential.py`, `test_window_functions_differential.py`, `test_joins_differential.py`, `test_datetime_functions_differential.py`, `test_array_functions_differential.py`, `test_sql_expressions_differential.py`, `test_string_functions_differential.py`, `test_math_functions_differential.py`, `test_column_operations_differential.py`, `test_null_handling_differential.py`, `test_subquery_differential.py`, `test_ddl_parser_differential.py`, `test_dataframe_ops_differential.py`

---

### Prioritized Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | N2: Nullable over-broadening | ~23 | Medium | Refine per-expression nullable narrowing in `inferSchema()` |
| **P2** | S1: StringType fallback | ~15 | Medium | Add type inference for split, flatten, map ops, struct access, grouping |
| **P3** | T2: Decimal precision | ~15 | Low | Fix SUM precision formula (38 not 37), scale propagation |
| **P4** | T3: DOUBLE ↔ DECIMAL | ~10 | Medium | AVG-over-DECIMAL → DOUBLE, division widening rules |
| **P5** | D1: decimal-div BinaryExpr | 4 | Low | Re-land reverted fix (with extension rebuild) |
| **P6** | X2: Overflow | 2 | Low | Add overflow detection to spark_sum extension |

**Note on N2**: With `SchemaCorrectedBatchIterator` removed, nullable mismatches now only surface through `AnalyzePlan` schema responses (not Arrow data). The N2 cluster shrank from ~30 to ~23 because DuckDB's native all-nullable-true is accepted by clients. Remaining failures are where `inferSchema()` reports wrong nullable for schema introspection.

---

### Architecture Goal: Zero-Copy Strict Mode

**Two invariants that must hold simultaneously:**

```
Apache Spark 4.1 types (authoritative truth)
  ← must match → DuckDB output (shaped by SQL generation + extension functions)
  ← must match → inferSchema() (type inference in logical plan)
```

1. **Spark is the authority.** The target types, precision, scale, and nullability are defined by what Apache Spark 4.1 returns. We don't approximate — we match exactly.

2. **DuckDB output must match Spark at the engine level.** This is achieved through SQL generation (CASTs, `AS` aliases, function rewrites) and DuckDB extension functions (`spark_avg`, `spark_sum`, `spark_decimal_div`). No post-hoc Arrow rewriting — types must be correct in the Arrow data DuckDB produces.

3. **`inferSchema()` must match DuckDB output.** The logical plan's type inference must return exactly the same types that the generated SQL will produce when executed by DuckDB. If these disagree, `AnalyzePlan` responses (used by PySpark for `df.schema`) will report different types than the actual Arrow data.

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
