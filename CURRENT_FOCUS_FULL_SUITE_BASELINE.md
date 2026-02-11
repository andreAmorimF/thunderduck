# Full Differential Test Suite Baseline

**Date**: 2026-02-11

## Relaxed Mode (Complete)

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **737 passed, 0 failed, 2 skipped** (739 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL queries passing
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → **737/0/2**

---

## Strict Mode Analysis

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **541 passed, 198 failed** (739 total)

### Root Cause Clustering

| # | Root Cause | Tests | Fix Area |
|---|-----------|-------|----------|
| **C1** | Extension functions missing type overloads | **45** | DuckDB C++ extension |
| **C2** | `*Type → StringType` — schema correction not applied | **~70** | `SchemaCorrectedBatchIterator` / schema inference |
| **C3** | `DecimalType → DoubleType` — AVG/division using DOUBLE not DECIMAL | **~28** | SQL generation: strict mode should use extension functions |
| **C4** | `DecimalType` precision off by 1 | **~8** | `TypeInferenceEngine` precision calculation |
| **C5** | `ArrayType → StringType/ArrayType(StringType)` | **~5** | Complex type schema inference |
| **C6** | Extension internal crash (`Failed to cast expression`) | **2** | DuckDB extension bug (Q11, Q74) |
| **C7** | Minor type coercion (`LongType↔IntegerType`, `Double→Int`) | **~6** | Type inference edge cases |
| **C8** | Nullable flag mismatches | **~34** | Nullable inference |

---

### C1: Extension Function Missing Overloads (45 tests)

The DuckDB extension only implements **DECIMAL** overloads. When input is DOUBLE, INTEGER, or BIGINT, functions fail:

| Function | Error | Tests |
|----------|-------|-------|
| `spark_avg(DECIMAL)` only | "requires DECIMAL argument" when called with non-DECIMAL | 27 |
| `spark_avg(DOUBLE)` | "No function matches" — no DOUBLE overload exists | 4 |
| `spark_sum(DOUBLE)` | "No function matches" — no DOUBLE overload exists | 4 |
| `spark_decimal_div` | "requires DECIMAL arguments" when inputs are non-DECIMAL | 8 |
| Extension crash | "Failed to cast expression to type" (DuckDB internal) | 2 |

**Fix**: Add DOUBLE, INTEGER, BIGINT overloads to the C++ extension functions, or guard the SQL generator to only emit `spark_avg`/`spark_sum`/`spark_decimal_div` when inputs are confirmed DECIMAL.

### C2: Everything Returns as StringType (~70 tests)

The dominant single issue. Thunderduck returns `StringType` for columns that should be typed:

| Expected Type | Occurrences |
|--------------|-------------|
| `LongType → StringType` | 22 |
| `IntegerType → StringType` | 21 |
| `DecimalType(various) → StringType` | 14 |
| `ByteType → StringType` | 6 |
| `DoubleType → StringType` | 3 |
| `DateType → StringType` | 1 |

This suggests `SchemaCorrectedBatchIterator` or schema inference isn't properly mapping DuckDB result types to Spark types in strict mode.

### C3: DECIMAL → DOUBLE (~28 tests)

Relaxed mode casts `AVG()` results to `DOUBLE` for simplicity. In strict mode, these should use the extension's `spark_avg`/`spark_decimal_div` to preserve DECIMAL precision. The SQL generator isn't switching to extension functions when strict mode is active.

| Pattern | Occurrences |
|---------|-------------|
| `DecimalType(37,20) → DoubleType` | 11 |
| `DecimalType(27,2) → DoubleType` | 10 |
| `DecimalType(28,2) → DoubleType` | 2 |
| `DecimalType(33,2) → DoubleType` | 1 |
| `DecimalType(32,2) → DoubleType` | 1 |
| `DecimalType(30,6) → DoubleType` | 1 |

### C4: DecimalType Precision Off by 1 (~8 tests)

| Pattern | Occurrences |
|---------|-------------|
| `DecimalType(38,2) → DecimalType(37,2)` | 4 |
| `DecimalType(23,2) → DecimalType(22,2)` | 2 |
| `DecimalType(38,19) → DecimalType(38,20)` | 1 |

### C5–C8: Smaller Issues (~47 tests combined)

- **C5**: Array types returned as `StringType` or `ArrayType(StringType)` instead of correct element types (5 tests)
- **C6**: DuckDB extension internal crashes on Q11 and Q74 — "Failed to cast expression to type" (2 tests)
- **C7**: Minor type coercion issues: `LongType↔IntegerType`, `DoubleType→IntegerType` (6 tests)
- **C8**: Nullable flag mismatches — `nullable=False` expected but `nullable=True` returned (34 tests, cross-cutting)

---

### Failures by Test File

| File | Failures |
|------|----------|
| `test_tpcds_differential.py` | 60 |
| `test_type_literals_differential.py` | 29 |
| `test_lambda_differential.py` | 18 |
| `test_multidim_aggregations.py` | 13 |
| `test_complex_types_differential.py` | 12 |
| `test_dataframe_functions.py` | 10 |
| `test_window_functions.py` | 10 |
| `test_aggregation_functions_differential.py` | 8 |
| `test_differential_v2.py` (TPC-H SQL) | 8 |
| `test_tpcds_dataframe_differential.py` | 5 |
| `test_array_functions_differential.py` | 4 |
| `test_overflow_differential.py` | 4 |
| `test_sql_expressions_differential.py` | 4 |
| `test_temp_views.py` | 4 |
| Others (6 files) | 9 |

---

### Architecture Goal: Zero-Copy Strict Mode

**Three invariants that must hold simultaneously:**

```
Apache Spark 4.1 types (authoritative truth)
  ← must match → DuckDB output (shaped by SQL generation + extension functions)
  ← must match → inferSchema() (type inference in logical plan)
```

1. **Spark is the authority.** The target types, precision, scale, and nullability are defined by what Apache Spark 4.1 returns. We don't approximate — we match exactly.

2. **DuckDB output must match Spark at the engine level.** This is achieved through a combination of SQL generation (CASTs, `AS` aliases, function rewrites) and DuckDB extension functions (`spark_avg`, `spark_sum`, `spark_decimal_div`). The extension functions exist to produce Spark-correct types where vanilla DuckDB diverges. No post-hoc Arrow vector rewriting — types must be correct in the Arrow data DuckDB produces.

3. **`inferSchema()` must match DuckDB output.** The logical plan's type inference must return exactly the same types that the generated SQL will produce when executed by DuckDB. If these disagree, `AnalyzePlan` responses (used by PySpark for `df.schema`) will report different types than the actual Arrow data.

**`SchemaCorrectedBatchIterator` should not be needed in strict mode.** If it's still required, it means either the SQL generation isn't producing Spark-correct types at the DuckDB level, or `inferSchema()` doesn't match what DuckDB returns. Both are bugs to fix, not to paper over with post-hoc correction.

**No performance compromise.** All type correctness is achieved at query planning / SQL generation time. Zero Arrow vector copying. Zero runtime type conversion. The extension functions run inside DuckDB's execution engine at native speed.

### Key Finding: Relaxed Mode Hid Pre-Existing Type Issues

The test framework (`dataframe_diff.py:202-212`) **skips type and nullable comparison entirely in relaxed mode**:

```python
# In relaxed mode, skip type checking entirely
if not self.relaxed and ref_type != test_type:
    mismatches.append(...)
```

`_is_relaxed_mode()` returns `True` for both `auto` and `relaxed` modes. This means the ~70 "StringType" failures are NOT caused by strict mode introducing wrong types — they are pre-existing `inferSchema()` gaps that relaxed mode never checked. The types have always been wrong for these expressions; strict mode merely reveals it.

### Priority Fix Order

1. **C2 (StringType)** — ~70 tests. Pre-existing `inferSchema()` gaps: `UnresolvedColumn.dataType()` returns StringType placeholder, complex expressions (lambda, struct, collect_list/set) don't resolve output types. Fix: complete type inference for all expression/plan node types.
2. **C1 (Extension overloads)** — 45 tests. Extension only has DECIMAL overloads for `spark_avg`/`spark_sum`/`spark_decimal_div`. Fix: add DOUBLE/INTEGER/BIGINT overloads in C++ extension, OR guard SQL gen to only emit extension functions for confirmed DECIMAL inputs.
3. **C3 (DECIMAL→DOUBLE)** — ~28 tests. SQL generator emits `CAST(AVG(...) AS DOUBLE)` in relaxed mode but doesn't switch to extension functions in strict mode. Fix: mode-conditional SQL generation for AVG/division.
4. **C4 (Precision)** — ~8 tests. `TypeInferenceEngine` precision calculation off by 1. Fix: match Spark 4.1 precision rules exactly.
5. **C5–C8** — smaller batches, incremental fixes.
