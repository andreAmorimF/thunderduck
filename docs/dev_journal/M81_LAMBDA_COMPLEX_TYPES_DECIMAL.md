# M81: SQL-Path Lambda Expressions, Complex Type Inference (S1), and Decimal Precision (T2/T3)

**Date**: 2026-02-12 to 2026-02-13
**Scope**: Lambda expression support for raw SQL, view schema caching, S1 complex type return type inference, T2 decimal precision fix, T3 DOUBLE/DECIMAL confusion fix
**Result**: Strict mode 636/103 -> 712/36 (+76 tests), relaxed mode 739/7/2 -> 746/0/2 (perfected)

## 1. Lambda Expression Support for SparkSQL Raw Queries

**Commit**: `d5734b4`

**Problem**: The SparkSQL ANTLR parser did not handle lambda expressions (e.g., `transform(array, x -> x + 1)`). Lambda syntax fell back to `RawSQLExpression`, losing type information and preventing correct Spark-to-DuckDB translation for higher-order functions.

**Fix**: `SparkSQLAstBuilder` now parses lambda expressions into proper `LambdaExpression` AST nodes. Lambda variable scope tracking ensures references in lambda bodies resolve to `LambdaVariableExpression` (unquoted) rather than `UnresolvedColumn`.

`FunctionRegistry` gains HOF (higher-order function) emulation translators for `exists`, `forall`, and `aggregate`, so the SQL path matches the DataFrame path's behavior.

**Files**: `core/.../parser/SparkSQLAstBuilder.java`, `core/.../functions/FunctionRegistry.java`

## 2. View Schema Caching (N2 partial fix, strict 636 -> 684)

**Commits**: `eaba6cc`, `1148ebc`

**Problem**: When `spark.table("view_name")` resolves a temp view's schema, DuckDB's `DESCRIBE` always reports all columns as `nullable=true`. This caused nullable over-broadening (N2) where views created from non-nullable sources lost their nullable precision.

Two separate caching gaps:

1. **DataFrame API path** (`eaba6cc`): `df.createOrReplaceTempView()` now caches the `LogicalPlan`'s `inferSchema()` result at registration time. `inferTableSchema()` checks the cache before falling back to DuckDB `DESCRIBE`.

2. **SQL path** (`1148ebc`): `CREATE TEMP VIEW` via SQL went through `SparkSQLAstBuilder` -> `RawDDLStatement` -> direct DDL execution, bypassing `Session.registerTempView()` entirely. Fix threads the view name and inner query `LogicalPlan` through `RawDDLStatement`, then caches `inferSchema()` after DDL execution.

**Impact**: Strict 665/83 -> 684/64 (-19 failures). Lambda -12->1, complex_types -10->0, dataframe_functions -4->0.

**Files**: `core/.../Session.java`, `core/.../logical/RawDDLStatement.java`, `core/.../parser/SparkSQLAstBuilder.java`

## 3. Nullable Inference Refinement

**Commits**: `fc2beb2`, `4c0514b`

Extended nullable inference to cover additional cases:

- `SchemaInferrer`: Parse MAP and STRUCT types (no more StringType fallback). Change LIST `containsNull` default to `false` (matches Spark).
- `TypeInferenceEngine`: Add `grouping`/`grouping_id` type + nullable inference. Add `UpdateFieldsExpression` type resolution. Add `split`, `map_entries` return type inference. Mark `aggregate`/`reduce` HOFs as always nullable (Spark semantics).
- `ExpressionConverter`: Fix hardcoded `StringType` in aggregate `list_prepend`/`list_reduce`.

**Files**: `core/.../types/TypeInferenceEngine.java`, `core/.../types/SchemaInferrer.java`, `core/.../converter/ExpressionConverter.java`

## 4. Decimal Precision/Scale Formulas

**Commit**: `5009ea5`

**Problem**: Decimal precision and scale formulas diverged from Spark's `DecimalPrecision` rules, causing type mismatches in arithmetic expressions involving DECIMAL types.

**Fix**: Rewrote decimal promotion to match Spark exactly:

| Operation | Formula |
|-----------|---------|
| Addition/Subtraction | `adjustPrecisionScale()` with +1 carry digit |
| Multiplication | `adjustPrecisionScale()` (replaces naive `min()` cap) |
| Division | Shared `adjustPrecisionScale()` (refactored from standalone) |
| Modulo | `min(intDigits)` not `max` (Spark's remainder formula) |
| LongType promotion | `DECIMAL(20,0)` not `DECIMAL(19,0)` |
| Integer-to-DECIMAL | Promotion when other operand is DECIMAL in DIVIDE |

Extracted `adjustPrecisionScale()` as shared utility for precision overflow handling across all four arithmetic operations.

**Files**: `core/.../types/TypeInferenceEngine.java`

## 5. Complex Type Return Type Inference (S1)

**Commit**: `05dd8a9`

**Problem**: `TypeInferenceEngine` returned `StringType` for map/array functions when the declared return type contained `UnresolvedType` elements. Functions like `map_keys`, `map_values`, `flatten`, `split`, `map_entries`, `map_from_arrays`, `map`, and `create_map` all fell through to `resolveNestedType()` which defaulted to `StringType`.

**Fix**: Added explicit return type handlers for each function in both the `ArrayType` and `MapType` branches of `TypeInferenceEngine`:

| Function | Return Type |
|----------|-------------|
| `map_keys(Map<K,V>)` | `Array<K>` |
| `map_values(Map<K,V>)` | `Array<V>` |
| `flatten(Array<Array<T>>)` | `Array<T>` |
| `split(String, regex)` | `Array<String>` |
| `map_entries(Map<K,V>)` | `Array<Struct<key:K, value:V>>` |
| `map_from_arrays(Array<K>, Array<V>)` | `Map<K,V>` |
| `map`/`create_map(k1,v1,k2,v2,...)` | `Map<K,V>` |

**Files**: `core/.../types/TypeInferenceEngine.java`

## 6. ToSchema Schema Preservation and Lambda Body Type Inference

**Commit**: `ed926ce`

**Problem**: `convertToSchema()` did not build a target `StructType`, losing schema information. `SparkSQLAstBuilder` did not handle ARRAY/LIST types from DuckDB. Lambda body type inference had no mechanism to resolve variable types.

**Fix**:
- `RelationConverter.convertToSchema()` builds target `StructType` and passes to `SQLRelation`
- `SparkSQLAstBuilder.mapDuckDBTypeToThunderduck()` handles ARRAY/LIST via `SchemaInferrer`
- `TypeInferenceEngine` resolves `LambdaVariableExpression` types from schema; `transform` handler evaluates lambda body type by augmenting schema with lambda parameter bindings

**Files**: `core/.../converter/RelationConverter.java`, `core/.../parser/SparkSQLAstBuilder.java`, `core/.../types/TypeInferenceEngine.java`

## 7. Decimal Precision (T2) and DOUBLE/DECIMAL Confusion (T3)

**Commit**: `e7ecd86`

**T2 — Decimal literal precision**: `DecimalLiteralContext` was parsed as `StringType`. Now parsed as `Literal(BigDecimal, DecimalType)` with Spark precision/scale rules. Added CTE schema registry for forward-propagation during SQL parsing.

**T3 — DOUBLE/DECIMAL confusion**: `generateExpressionWithCast()` in `SQLGenerator` now adds CAST wrapping when `TypeInferenceEngine` resolves DECIMAL but expression-level `dataType()` returns non-DECIMAL (WithColumns path).

**Impact**: Strict 685/63 -> 712/36 (+27 tests). Relaxed 739/7/2 -> 746/0/2 (perfected: zero failures, zero regressions).

**Files**: `core/.../parser/SparkSQLAstBuilder.java`, `core/.../generator/SQLGenerator.java`

## Baseline Progression

| Step | Strict | Relaxed |
|------|--------|---------|
| Start (M80 end) | 636/103 | 737/0/2 |
| After lambda + view caching | 684/64 | 739/7/2 |
| After S1 + baselines | 685/63 | 739/7/2 |
| After T2 + T3 | 712/36 | 746/0/2 |
| **End (M81)** | **712/36** | **746/0/2** |

## Key Design Decisions

1. **View schema caching over DuckDB DESCRIBE**: DuckDB always reports `nullable=true` for all columns. Caching the logical plan's schema at view creation preserves the correct nullable flags without querying DuckDB at all.

2. **`spark_sum`/`spark_avg` replacement with CAST**: During T2/T3 work, discovered that `spark_sum` inside UNION ALL CTEs triggers a DuckDB `CompressedMaterialization` optimizer crash. Replaced with `CAST(sum/avg(...) AS DECIMAL(p,s))` which achieves the same precision control without custom aggregates.

3. **Lambda variable scope tracking**: Instead of post-hoc lambda variable detection, the parser maintains a scope stack. Variables introduced by lambda parameters are tracked during parsing and resolved immediately in the AST builder.
