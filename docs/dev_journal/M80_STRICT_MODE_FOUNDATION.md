# M80: Strict Mode Foundation — Type Inference, Zero-Copy Architecture, Extension Performance

**Date**: 2026-02-11 to 2026-02-12
**Scope**: Establish strict mode type parity infrastructure: expand type inference, remove schema patching, guard extension functions, optimize C++ accumulators
**Result**: Strict mode 541/198 -> 636/103 (+95 tests), relaxed mode 737/0/2 (zero regressions)

## 1. Extension Function Guards (strict 541 -> 572, +31 tests)

**Commit**: `c165acf`

**Problem**: Strict mode unconditionally emitted `spark_sum`, `spark_avg`, and `spark_decimal_div` for all input types. These extension functions only have DECIMAL overloads, so DOUBLE/INTEGER/BIGINT inputs caused "No function matches" errors.

**Fix**: Added type guards at all 7 dispatch points in `SQLGenerator` and `WindowFunction`. Non-DECIMAL inputs now fall back to vanilla DuckDB functions.

**Files**: `core/.../generator/SQLGenerator.java`

## 2. Type Inference Expansion for Strict Mode (strict 572 -> 623)

**Commit**: `da12193`

**Problem**: `plan.inferSchema()` returned incomplete types for many expression types, causing incorrect schema responses. Complex types, collection constructors, lambda array functions, and many function categories had no type resolution.

**Fix**: Expanded `TypeInferenceEngine` with type resolution for:
- Complex types: `ExtractValue`, `FieldAccess`, `Cast`, `RawSQL`
- Collection constructors: `array()`, `map()`, `struct()`, `named_struct()`
- Collection aggregates: `collect_list`/`collect_set` (with `containsNull=false` matching Spark)
- Lambda array functions: `transform`, `filter`, `reduce`
- Function return types: string, datetime, math, boolean categories
- Nullable resolution for all new expression types

Also added `RawSQLExpression` typed constructor for expressions with known types at parse time (e.g., INTERVAL literals).

**Files**: `core/.../types/TypeInferenceEngine.java`, `core/.../converter/RelationConverter.java`, `core/.../expression/RawSQLExpression.java`

## 3. `__int128` Accumulator Optimization (strict 623 -> 623, performance only)

**Commit**: `e528ca6`

**Problem**: DuckDB extension's `SparkSumDecimalState` and `SparkAvgDecimalState` used `hugeint_t` for accumulators, which requires non-inline library calls for arithmetic operations.

**Fix**: Switched accumulators from `hugeint_t` to native `__int128`. Impact on hot-loop operations:

| Operation | Before (`hugeint_t`) | After (`__int128`) |
|-----------|---------------------|-------------------|
| Per-row accumulation | `operator+=` (non-inline library call) | `+=` (inline ADD/ADC, 2 instructions) |
| ConstantOperation | `Convert` + `operator*` (2 non-inline calls) | `*` (inline MUL, 1 instruction) |
| Combine | `operator+=` (non-inline) | `+=` (inline) |
| Finalize | `HugeintToInt128` conversion + write | Direct write from state |

Input arrives as `hugeint_t`, converted once per row via `HugeintToInt128()` (already inline). All arithmetic stays in `__int128` until finalize.

**Files**: `thunderduck-duckdb-extension/.../spark_aggregates.hpp`

## 4. SchemaCorrectedBatchIterator Removal (strict 623 -> 636)

**Commit**: `e52cf93`

**Problem**: `SchemaCorrectedBatchIterator` wrapped every Arrow batch to patch nullable flags, adding per-batch overhead and violating the zero-copy architecture goal.

**Fix**: Removed the iterator entirely. DuckDB's native all-nullable-true Arrow output is accepted by Spark clients directly. This completes the zero-copy strict mode architecture: DuckDB Arrow batches flow through with no schema patching, no type conversion, no vector copying.

**Impact**: Strict mode improved by 7 tests (lambda -4, multidim_aggregations -3). Zero relaxed mode regressions.

**Files**: `core/.../SparkConnectServiceImpl.java`

## 5. Nullable Inference Deepening

**Commits**: `527e9aa`, `d283905`, `4c0514b`

Three rounds of nullable inference fixes:

1. **Aggregates and outer joins** (`527e9aa`): SUM/AVG/MIN/MAX/FIRST/LAST now always return `nullable=true` per Spark semantics. `Join.inferSchema()` marks non-preserved side columns as nullable for LEFT/RIGHT/FULL OUTER joins.

2. **Source columns and math functions** (`d283905`): Force all Parquet source columns to `nullable=true` (Spark treats file-based columns as always nullable). Mark math functions (ceil, floor, round, log, exp, sqrt, pow, trig) as always nullable. Fix `nvl2(a,b,c)` nullable = `b.nullable AND c.nullable`.

3. **Expression-level refinement** (`4c0514b`): Schema-aware nullable resolution for `IS NULL`/`IS NOT NULL` (always non-nullable), struct field access (`struct.nullable || field.nullable`), `CASE WHEN` branches, higher-order functions, array/map operations, complex type constructors.

**Files**: `core/.../types/TypeInferenceEngine.java`, `core/.../converter/RelationConverter.java`

## 6. Type Inference Fixes for Function Return Types

**Commits**: `387bc02`, `9713ece`, `803779c`, `c52cd00`

| Fix | Detail |
|-----|--------|
| String length functions | Return `IntegerType` (not `LongType`); added `CAST(... AS INTEGER)` in SQL since DuckDB returns BIGINT |
| `array_position` | Return `LongType` (not `IntegerType`); added `CAST(... AS BIGINT)` |
| Multi-arg type-preserving functions | `coalesce`, `nvl`, `greatest`, `least`, etc. now promote types across arguments using Spark's ANSI coercion rules |
| `selectExpr`/SQL expressions | Fixed `RawSQLExpression` returning `UnresolvedType` falling through to `StringType`; added `resolveUnresolvedSchemaFields()` that queries DuckDB with `LIMIT 0` for unresolved fields |
| AVG scale formula | Capped at 18 (`min(min(s+4,18), newPrecision)`) to match C++ extension |
| `spark_decimal_div` guard | Mixed-type divisions (DECIMAL / INTEGER) fall through to vanilla DuckDB |

**Impact**: Fixed 12+ tests across `test_sql_expressions`, `test_array_functions`, `test_tpcds` (Q2, Q39a, Q39b).

## 7. Division Operator Cleanup

**Commit**: `480448d`

**Problem**: The DuckDB extension's `/` operator override for DECIMAL types caused TPC-DS strict mode failures. The ANTLR SparkSQL parser already generates explicit `spark_decimal_div()` calls, making the override redundant and harmful.

**Fix**:
- Removed the `/` operator override from the DuckDB extension
- Extracted `BinaryExpression.generateStrictDivisionSQL()` as shared public static method
- Updated `SQLGenerator.qualifyCondition()` to use the shared method

**Files**: `core/.../expression/BinaryExpression.java`, `core/.../generator/SQLGenerator.java`, `thunderduck-duckdb-extension/`

## Architecture Impact

This milestone established the three pillars of strict mode:

1. **Zero-copy Arrow streaming**: `SchemaCorrectedBatchIterator` removed. DuckDB output flows directly to Spark clients.
2. **Type-correct SQL generation**: Extension functions guarded to DECIMAL-only. Division dispatch uses AST types, not regex.
3. **Complete type inference**: `TypeInferenceEngine` covers all expression types, function categories, and nullable rules.

## Baseline Progression

| Milestone | Strict | Relaxed |
|-----------|--------|---------|
| Start (M79) | 541/198 | 737/0/2 |
| After guards | 572/167 | 737/0/2 |
| After type inference | 623/116 | 737/0/2 |
| After SchemaCorrected removal | 636/103 | 737/0/2 |
| **End (M80)** | **636/103** | **737/0/2** |
