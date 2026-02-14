# M82: Final Strict Mode Convergence — Decimal Dispatch, N2/T1/G1/D1 Fixes, Extension Optimization, Refactoring

**Date**: 2026-02-14
**Scope**: Schema-aware SQL-path decimal dispatch, final type inference fixes (N2, T1, G1, D1), C++ extension performance optimization, TypeInferenceEngine refactoring, terminal baseline
**Result**: Strict mode 712/36 -> **744/2/2** (terminal baseline), relaxed mode **746/0/2** (unchanged)

## 1. Schema-Aware SQL-Path Decimal Dispatch (strict 712/36 -> 727/21)

**Commit**: `977a1a1`

**Problem**: The raw SQL (ANTLR) path could not resolve column types before SQL generation. Without knowing whether a column is DECIMAL, the generator could not dispatch to `spark_decimal_div()` for division or wrap SUM/AVG with precision-correct CASTs. This caused 15 TPC-DS failures where DECIMAL columns appeared in division or aggregation expressions.

**Fix**: Added `transformExpressionForStrictMode()` to `SQLGenerator` -- a recursive expression walker that:

1. Resolves column types from the child plan's `inferSchema()` result
2. Rewrites DIVIDE expressions to `spark_decimal_div()` when both operands are DECIMAL
3. Wraps SUM/AVG on DECIMAL columns with `CAST(sum/avg(...) AS DECIMAL(p,s))`

**spark_sum/spark_avg replacement**: Replaced the extension's custom `spark_sum`/`spark_avg` aggregate functions with `CAST(sum/avg(...) AS DECIMAL(p,s))`. DuckDB's native `sum()` preserves DECIMAL precision; the CAST adjusts precision/scale to match Spark's formula. This works around a DuckDB `CompressedMaterialization::CompressAggregate` optimizer crash when custom aggregates appear inside UNION ALL CTEs (Q4, Q11, Q66, Q74).

Additional fixes in this commit:
- `round`/`bround` DECIMAL type preservation in type inference
- MapType STRUCT_FIELD fallback for field access
- `toSparkSQL()` method on `BinaryExpression` for column naming without extension function leakage

**TPC-DS fixes**: Q2, Q4, Q9, Q11, Q14, Q28, Q59, Q61, Q66, Q74.

**Files**: `core/.../generator/SQLGenerator.java`, `core/.../expression/BinaryExpression.java`, `core/.../logical/Project.java`, `core/.../types/TypeInferenceEngine.java`

## 2. N2 Fix: Nullable Over-Broadening (strict 727 -> ~735)

**Commit**: `7e3dfdd` (part of combined commit)

**Problem**: Multiple code paths produced `nullable=true` for fields that Spark marks as non-nullable, causing schema mismatches in strict mode.

**Seven sub-fixes**:

| Component | Fix |
|-----------|-----|
| `ExtractValueExpression` | STRUCT_FIELD nullable = `base.nullable \|\| field.nullable` (was always `true`) |
| Qualified column resolution | Traverse struct hierarchy via `resolveType()` + `resolveNullable()` |
| Composite aggregate | Nullable via `resolveNullable()` instead of hardcoded `true` |
| Lambda `transform` | `containsNull` derived from body nullable; `LambdaVariableExpression` schema lookup |
| VALUES clause | Schema inference from literal values in `SparkSQLAstBuilder` |
| View schema cache | Threading from `Session` to `SparkSQLParser` for correct nullable flags |
| `AliasedRelation` | Column alias application in `inferSchema()` |

**Files**: `core/.../types/TypeInferenceEngine.java`, `core/.../parser/SparkSQLAstBuilder.java`, `core/.../SparkConnectServiceImpl.java`

## 3. T1 Fix: stddev/variance Type Mismatch (strict ~735 -> ~738)

**Commit**: `7e3dfdd` (part of combined commit)

**Problem**: Statistical functions (`stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`) were not routed through type inference, falling through to default types. Spark always returns `DoubleType` for these functions.

**Fix**: Added routing in `resolveType()` and explicit return types in `resolveAggregateReturnType()` for all statistical functions. Result: always `DoubleType`, always `nullable=true`.

**Impact**: Fixed Q17 and `test_multiple_aggregations_same_column`.

**Files**: `core/.../types/TypeInferenceEngine.java`

## 4. G1 Fix: grouping/grouping_id Type Inference (strict ~738 -> 743)

**Commit**: `7e3dfdd` (part of combined commit)

**Problem**: `grouping()` and `grouping_id()` had no type inference entries. DuckDB returns these as INTEGER/BIGINT, but Spark uses ByteType for `grouping()` and LongType for `grouping_id()`.

**Fix**:
- `TypeInferenceEngine`: `grouping()` returns `ByteType`, `grouping_id()` returns `LongType`
- `SQLGenerator`: Strict mode wraps with `CAST(... AS TINYINT)` / `CAST(... AS BIGINT)`
- `SparkConnectServiceImpl`: Added `ByteType` and `ShortType` proto conversion

**Impact**: Fixed TPC-DS Q27, Q70, Q86.

**Files**: `core/.../types/TypeInferenceEngine.java`, `core/.../generator/SQLGenerator.java`, `core/.../SparkConnectServiceImpl.java`

## 5. D1 Fix: map_keys containsNull Mismatch (strict 743/3 -> 744/2)

**Commit**: `5808f3d`

**Problem**: `map_keys()` return type used `containsNull=false` (logically correct -- map keys cannot be null). However, Spark conservatively marks `map_keys()` output elements as `nullable=true` (`containsNull=true`).

**Fix**: Changed `map_keys` return type from `ArrayType(keyType, false)` to `ArrayType(keyType, true)` at both inference sites (lines ~1024 and ~1213 in `TypeInferenceEngine`).

**Impact**: Fixed `test_map_keys` in strict mode. Only 2 X2 overflow tests remain.

**Files**: `core/.../types/TypeInferenceEngine.java`

## 6. C++ Extension Performance Optimization

**Commit**: `0ba99b4`

Updated the DuckDB extension submodule with three performance optimizations:

### Knuth's Algorithm D for Division
Replaced the generic division implementation with Knuth's Algorithm D (multi-precision division), which is the standard algorithm for dividing large integers. More efficient than the previous iterative approach for the DECIMAL division use case.

### Flat-Vector Fast Path
Added a fast path for flat (non-dictionary, non-constant) vectors in the extension's aggregate functions. When the input vector is flat (the common case), skip the unified vector format indirection and read values directly from the data pointer. Eliminates per-row `FlatVector::GetData` overhead.

### Bind Deduplication
Deduplicated bind function logic across `spark_sum`, `spark_avg`, and `spark_decimal_div` by extracting shared bind helpers. Reduces code duplication and ensures consistent type handling across all extension functions.

**Files**: `thunderduck-duckdb-extension/` (submodule update)

## 7. TypeInferenceEngine Refactoring

**Commit**: `c768ddd`

**Problem**: `TypeInferenceEngine` had accumulated significant code duplication through rapid feature additions in M80-M81. Two parallel `ArrayType`/`MapType` inference blocks had ~100 lines of duplicate logic. String-based function matching used 11 `String.matches()` regex calls. Four identical decimal promotion blocks existed for DIVIDE/MULTIPLY/ADD/MODULO.

**Fix** (net +310/-235 lines, improved readability):

| Refactoring | Detail |
|-------------|--------|
| 10 function-dispatch helpers | `resolveCollectListType`, `resolveArrayConstructorType`, `resolveMapKeysValuesType`, etc. -- eliminates duplicate code between ArrayType/MapType branches |
| `Set<String>` lookups | Replaced 11 `String.matches()` regex calls with `static final Set<String>` constants (`STRING_RETURNING_FUNCTIONS`, `DATE_RETURNING_FUNCTIONS`, etc.) for O(1) lookup |
| `tryDecimalPromotion()` | Extracted with `BiFunction` parameter, replacing 4 identical decimal promotion blocks |
| `normalizeDistinctSuffix()` | Extracted, replacing 3 occurrences of `_DISTINCT` suffix handling |

All 51/51 TPC-H strict mode tests pass after refactoring. Zero behavior changes.

**Files**: `core/.../types/TypeInferenceEngine.java`

## 8. X2 Overflow: Won't-Fix Decision (strict 744/2/2 terminal)

**Commit**: `eb0e3c3`

The 2 remaining strict mode failures are in `test_overflow_differential.py`. DuckDB silently promotes to a wider type on integer overflow (e.g., `BIGINT` -> `HUGEINT`), while Spark throws `ArithmeticException`. DuckDB's behavior is mathematically correct and preserves data.

**Decision**: Marked as won't-fix. **744/2/2 is the terminal strict mode baseline.** Reproducing Spark's overflow exceptions would require degrading correctness.

## Final Baseline

### Strict Mode: 744 passed, 2 failed, 2 skipped (748 total)

| Category | Result |
|----------|--------|
| TPC-H | 51/51 (100%) |
| TPC-DS | 99/99 (100%) |
| Lambda HOFs | 27/27 (100%) |
| All other test files | 100% except 2 overflow tests |
| 2 failed | X2 overflow (won't-fix) |
| 2 skipped | Negative array index (relaxed-only) |

### Relaxed Mode: 746 passed, 0 failed, 2 skipped (748 total)

Zero failures. Zero regressions throughout M80-M82.

## Strict Mode Journey (Full Progression)

```
541/198  (M80 start, extension guards not yet applied)
  -> 572  (+31, extension function guards)
  -> 623  (+51, type inference expansion + __int128)
  -> 636  (+13, SchemaCorrectedBatchIterator removal)
  -> 665  (+29, nullable deepening + function type fixes)
  -> 684  (+19, view schema caching)
  -> 685  (+1, S1 complex type inference)
  -> 712  (+27, T2 decimal precision + T3 confusion fix)
  -> 727  (+15, schema-aware decimal dispatch)
  -> 743  (+16, N2 nullable + T1 stddev + G1 grouping)
  -> 744  (+1, D1 map_keys containsNull)
  -> 744/2/2 (X2 overflow marked won't-fix — terminal)
```

**Total improvement**: +203 tests fixed (541 -> 744) in 3 days of work across M80-M82.
