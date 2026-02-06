# Root Cause Investigation Findings

**Date:** 2026-02-06 | **Spark:** 4.1.1 | **Thunderduck:** 0.1.0-SNAPSHOT

Each section summarizes a parallel investigation into one of the 7 open issues.

---

## 1. RC1 — COUNT Nullable (Q1, Q4, Q13, Q16, Q21, Q22, Sanity)

### Key Finding

The type-inference logic is **already correct**:
- `TypeInferenceEngine.resolveAggregateNullable()` (lines 942-943) returns `false` for COUNT/COUNT_DISTINCT.
- `Aggregate.inferSchema()` (lines 259-260) calls it and builds correct `StructField(nullable=false)`.
- `SchemaCorrectedBatchIterator.buildCorrectedSchema()` (lines 138-159) applies the logical schema's nullable flags to the Arrow output.

### Where It Breaks

The failing tests use **raw SQL** (`spark.sql(query)`), not the DataFrame API. For raw SQL:

1. There is **no logical plan** — the SQL string goes straight to DuckDB.
2. In `SparkConnectServiceImpl`, when `logicalSchema == null`, the code falls back to inferring the schema from DuckDB's Arrow output.
3. DuckDB marks **ALL columns as nullable=true**, so the fallback loses the correct nullable metadata.

### Fix Location

| File | What to change |
|------|---------------|
| `SparkConnectServiceImpl.java` | For SQL-string execution: after DuckDB returns results, detect COUNT columns and override nullable to `false` in the effective schema. Alternatively, build a lightweight logical plan even for raw SQL so that `inferSchema()` provides correct metadata. |

### Complexity: **Low-Medium**

The DataFrame API path already works. The fix needs to patch only the raw-SQL path.

---

## 2. RC2 — SUM Decimal Precision (Q1, Q18, Q22)

### Current State

The `p+10` precision rule is **correctly implemented** in 4 places:

| Location | Status |
|----------|--------|
| `TypeInferenceEngine.resolveAggregateReturnType()` (lines 867-881) | Correct |
| `Aggregate.toSQL()` (lines 171-188) — emits `CAST(SUM(col) AS DECIMAL(p+10,s))` | Correct |
| `SQLGenerator.visitAggregate()` (lines 1118-1131) — same CAST | Correct (duplicate) |
| `WindowFunction.dataType()` (lines 208-230) | Correct metadata, but `toSQL()` may not emit CAST |

### Why Queries Still Fail

Same root cause as RC1: the failing tests use **raw SQL**. When there is no logical plan:
- No `Aggregate.toSQL()` is invoked (the SQL string goes directly to DuckDB).
- DuckDB returns `DECIMAL(38,s)` for every SUM.
- The schema-correction layer has no logical schema to apply `DECIMAL(p+10,s)`.

### Fix Options

| Option | Approach | Pros | Cons |
|--------|----------|------|------|
| **A** | Build logical plan for raw SQL (parse + construct `Aggregate` nodes) | Fixes all type issues holistically | Large refactor |
| **B** | Post-process DuckDB schema: detect SUM columns, infer input type from parquet metadata, compute `p+10` | Targeted fix | Fragile; needs column-expression correlation |
| **C** | Accept DuckDB precision, override schema metadata in the correction layer | Minimal code | Requires mapping SQL columns to aggregate functions |

**Recommended**: Option C (schema correction) for raw SQL path, leveraging the already-correct logic for DataFrame path.

### Performance

`CAST` around `SUM` is **zero-cost** in DuckDB — it only narrows metadata, not values.

---

## 3. RC3 — AVG/Division Returns Double (Q1, Q8, Q14, Q17)

### Spark's Rules

- `AVG(DECIMAL(p,s))` → `DECIMAL(min(p+4, 38), min(s+4, precision))`
- `DECIMAL / DECIMAL` → `DECIMAL` (not DOUBLE), using `promoteDecimalDivision()`

### Current State

| Component | Status |
|-----------|--------|
| `TypeInferenceEngine.resolveAggregateReturnType("AVG", DecimalType)` | Correct (p+4, s+4) |
| `Aggregate.toSQL()` AVG CAST (lines 153-169) | Correct for DataFrame path |
| `SQLGenerator.visitAggregate()` AVG CAST (lines 1104-1116) | Correct (duplicate) |
| `TypeInferenceEngine.promoteDecimalDivision()` (lines 233-256) | Correct |
| `SQLGenerator.generateExpressionWithCast()` division (lines 974-977) | Only handles `WithColumns` context |
| `WindowFunction.dataType()` AVG (lines 231-241) | Returns DoubleType if argument type is unresolved |

### Gaps

1. **Raw SQL path** (same issue as RC1/RC2): no logical plan → no CAST applied → DuckDB returns DOUBLE.
2. **Division outside `WithColumns`**: `generateExpressionWithCast()` only wraps divisions in the `WithColumns` context, not in SELECT projections.
3. **Window function AVG**: `WindowFunction.toSQL()` doesn't emit CAST because it lacks schema access.

### Recommended Approach

For **raw SQL** (the TPC-H failures): same fix as RC1/RC2 — parse aggregate functions from the SQL to build schema metadata.

For **DataFrame API** gaps: extend `SQLGenerator` to apply division CAST in all expression contexts, not just `WithColumns`.

---

## 4. RC4 — EXTRACT Returns Long (Q7, Q8, Q9)

### Why DataFrame API Works

`FunctionRegistry.initializeDateFunctions()` (lines 483-492) wraps date functions with CAST:
```java
CUSTOM_TRANSLATORS.put("year", args -> "CAST(year(" + args[0] + ") AS INTEGER)");
CUSTOM_TRANSLATORS.put("month", args -> "CAST(month(" + args[0] + ") AS INTEGER)");
CUSTOM_TRANSLATORS.put("day", args -> "CAST(day(" + args[0] + ") AS INTEGER)");
```

`ExpressionConverter.handleDateExtractFunction()` (lines 614-623) explicitly sets `IntegerType.get()`.

### Why Raw SQL Fails

Raw SQL like `EXTRACT(YEAR FROM date_col)` goes through `RawSQLExpression.toSQL()`, which:
1. Calls `FunctionRegistry.rewriteSQL()` — but this only does name mapping, not CAST wrapping.
2. Has no semantic understanding of EXTRACT — treats it as an opaque string.
3. DuckDB's EXTRACT always returns **BIGINT**, not INTEGER.

### Recommended Fix

Add EXTRACT-to-CAST wrapping in `RawSQLExpression.toSQL()` or `FunctionRegistry.rewriteSQL()`:

```java
// Detect: EXTRACT(YEAR|MONTH|DAY|... FROM expr)
// Rewrite: CAST(EXTRACT(YEAR FROM expr) AS INTEGER)
```

A regex-based approach with parenthesis-depth tracking can handle this in ~50 lines.

### Performance

One regex pass at SQL generation time — negligible impact.

---

## 5. RC5 — SUM(CASE int) Returns Decimal (Q12)

### Root Cause

DuckDB treats numeric literals inside CASE expressions as **DECIMAL(38,0)**, not INTEGER. So:
- `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` → DuckDB returns `DECIMAL(38,0)`
- Spark expects `LongType` (BIGINT)

### Why the Existing CAST Doesn't Apply

`Aggregate.toSQL()` (line 172) gates on `childSchema != null`:
```java
if (childSchema != null && baseFuncName.equals("SUM") && aggExpr.argument() != null) {
    DataType argType = resolveExpressionType(aggExpr.argument(), childSchema);
    if (argType instanceof IntegerType || ...) {
        aggSQL = String.format("CAST(%s AS BIGINT)", aggSQL);
    }
}
```

For **raw SQL** (TPC-H Q12): no logical plan → `childSchema` is null → no CAST applied.

For **DataFrame API**: the CASE expression's `dataType()` may return an unresolved type, causing the `instanceof IntegerType` check to fail.

### Recommended Fix

1. **Raw SQL path**: same meta-fix as RC1-RC4.
2. **DataFrame API path**: add fallback in `Aggregate.toSQL()` — when `childSchema` is null, use `aggExpr.argument().dataType()` as fallback.
3. **Safety net**: `SchemaCorrectedBatchIterator` already has Decimal→BigInt conversion (lines 342-349) — verify it's invoked.

---

## 6. Non-Deterministic Test Ordering — RESOLVED

All 3 test ordering issues have been fixed:

| Test | Fix Applied | Status |
|------|------------|--------|
| `test_simple_groupby` | Added `order_independent=True` | PASSED |
| `test_simple_orderby` | Added secondary sort key `l_orderkey` | PASSED |
| `test_simple_join` | Added `.orderBy("o_orderkey", "l_quantity")` before `.limit(100)` | PASSED |

**Full test suite: 34/34 passing in relaxed mode.**

---

## 7. SUM Nullable Inference (Q12 DataFrame)

### Root Cause

`TypeInferenceEngine.resolveAggregateNullable()` lines 963-972:

```java
// Current (WRONG for Spark 4.1):
if (funcUpper.equals("SUM") || funcUpper.equals("AVG") || ...) {
    if (argument != null) {
        return resolveNullable(argument, schema);  // ← inherits argument's nullability
    }
}
```

For Q12 DataFrame: argument is `CASE WHEN ... THEN 1 ELSE 0` → nullable=False → SUM inherits False.

**Spark 4.1 corrected this**: SUM/AVG/MIN/MAX are **always nullable** (empty groups return NULL). Only COUNT is non-nullable.

### Fix

```java
// Correct:
if (funcUpper.equals("SUM") || funcUpper.equals("AVG") || ...) {
    return true;  // Always nullable — empty groups produce NULL
}
```

### No Conflict with RC1

- COUNT → `false` (handled at lines 942-944, separate code path)
- SUM/AVG/MIN/MAX → `true` (this fix, lines 963-972)

### Side Effect

**This also helps RC1**: currently the SQL path for COUNT shows nullable=True because the schema falls back to DuckDB's metadata. But if we fix the raw-SQL schema issue (RC1 main fix), COUNT will correctly show nullable=False. This fix ensures SUM shows nullable=True for the DataFrame API path.

### WindowFunction.nullable()

Already correct at lines 257-283: returns `true` for all non-COUNT, non-ranking window functions.

---

## Cross-Cutting Observation

**RC1 through RC5 for TPC-H SQL tests share the same root cause**: raw SQL execution has **no logical plan**, so the schema-correction layer falls back to DuckDB's own metadata — which has wrong types, wrong precision, and all-nullable.

### Unified Fix Strategy

| Approach | Impact | Effort |
|----------|--------|--------|
| **Parse raw SQL into logical plan** | Fixes everything holistically | High |
| **Post-hoc schema inference** for raw SQL (detect AGG functions, compute Spark-compatible types) | Fixes type/precision/nullable for known patterns | Medium |
| **Per-RC targeted fixes** (EXTRACT wrapping, CAST injection, nullable override) | Each RC independently fixable | Low per RC, medium total |

The recommended path is to combine:
1. **Targeted fixes** for the 3 non-SQL issues (tests ordering, SUM nullable, EXTRACT wrapping)
2. **Schema inference for raw SQL** that detects aggregate patterns and produces correct metadata

---

## Files Summary

| File | Issues | What to Change |
|------|--------|---------------|
| `TypeInferenceEngine.java:963-972` | RC7 (SUM nullable) | SUM/AVG/MIN/MAX → always nullable |
| `SparkConnectServiceImpl.java` | RC1-RC5 (raw SQL path) | Build schema metadata for raw SQL aggregates |
| `RawSQLExpression.java:76-82` | RC4 (EXTRACT) | Wrap EXTRACT with CAST(... AS INTEGER) |
| `Aggregate.java:171-188` | RC5 (SUM CASE) | Fallback type resolution when childSchema is null |
| `test_differential_v2.py:614-677` | Ordering tests | ~~Add deterministic ORDER BY to 3 tests~~ **DONE** |
| `WindowFunction.java:toSQL()` | RC2/RC3 (window SUM/AVG) | Add CAST for decimal window functions |
| `SQLGenerator.java:974-977` | RC3 (division CAST) | Extend division CAST beyond WithColumns context |
