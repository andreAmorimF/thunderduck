# Eliminating SQL String Manipulation — Architecture Plan

**Status**: Phases 1-4 complete. preprocessSQL deleted, no fallback. ~400 lines of schema-fixup code remain (active, not dead).
**Created**: 2026-02-09
**Last Updated**: 2026-02-10

## Architectural Principles

These are non-negotiable constraints that govern all design decisions:

1. **All SQL and expression snippets MUST be parsed into a typed AST.** No string manipulation on SQL text — ever.
2. **Zero pre/post-processing of SQL strings.** All transformations happen on the AST.
3. **SparkSQL data flow**: Spark SQL string → ANTLR parse tree → Thunderduck expression tree (DuckDB constructs + extensions) → generate SQL string for execution.
4. **DataFrame data flow**: Spark Connect protobuf → Thunderduck expression tree (DuckDB constructs + extensions) → generate SQL string for execution.
5. **Relaxed mode**: Find the best performance mapping to vanilla DuckDB constructs that produces a value-equivalent result (type equivalence is not required).
6. **Strict mode**: Match Apache Spark behavior exactly via (a) CASTs at top-level SELECT projection, or (b) DuckDB extension functions (e.g., `spark_decimal_div`). No casts on intermediate values if avoidable.
7. **Minimal result-set adjustments**: No or minimal type/nullability adjustments when retrieving streaming results in strict mode.
8. **Zero result copying**: In relaxed mode, no type matching required. In strict mode, 100% type matching is achieved at SQL generation time using extension functions. No Arrow vector copying or rewriting.

## Current State — What Violates These Principles

### Violation 1: `preprocessSQL()` — 14 regex passes over SQL strings

Both the SparkSQL path and the DataFrame path run generated SQL through `preprocessSQL()`, a ~170-line function that applies regex-based transformations to SQL text. This violates principles 1, 2, 3, and 4.

**Location**: `SparkConnectServiceImpl.java:836-1005`

| Line | What it does | Which principle it violates |
|------|-------------|---------------------------|
| 838 | Backtick → double-quote | 1 (should be handled at parse time) |
| 844-852 | `count(*)` → `count(*) AS "count(1)"` | 2 (should be in SQLGenerator) |
| 856 | Fix `returns` keyword as alias | 1 (parser should handle) |
| 859-860 | `+` → `\|\|` for string concat | 1 (BinaryExpression should emit correct operator) |
| 863 | Remove `at` before `cross join` | 1 (parser bug) |
| 871 | `year()` → `CAST(year() AS INTEGER)` | 1 (parser already handles this via EXTRACT mapping) |
| 881-882 | `CAST(x AS INTEGER)` → `CAST(TRUNC(x) AS INTEGER)` | 1 (CastExpression should handle) |
| 890-891 | `SUM(` → `spark_sum(`, `AVG(` → `spark_avg(` | 1, 6 (SQLGenerator already does this for DataFrame path) |
| 897 | `fixDivisionDecimalCasts()` — 130 lines of SQL parsing | 1, 2, 6 (should be in expression tree) |
| 902-903 | Force-uppercase DESC/ASC | 1 (Sort already emits uppercase) |
| 908-999 | ROLLUP NULLS FIRST — 90 lines of char scanning | 1, 2 (should be in SQLGenerator) |
| 1002-1243 | `translateSparkFunctions()` + 4 sub-methods | 1, 3 (parser should produce correct AST) |

### Violation 2: `SchemaCorrectedBatchIterator` — Arrow vector copying

After DuckDB returns results, `SchemaCorrectedBatchIterator` copies every Arrow batch to correct nullable flags, decimal precision, and column names. This violates principles 7 and 8.

**Location**: `core/.../runtime/SchemaCorrectedBatchIterator.java:49-333`

| What it does | Principle violated | Why it exists today |
|-------------|-------------------|---------------------|
| Corrects nullable flags on every batch | 7, 8 | DuckDB defaults all columns to nullable |
| Renames `spark_sum(...)` → `sum(...)` columns | 8 | Extension functions change column names |
| Promotes DECIMAL precision (e.g., 28,4 → 38,4) | 8 | DuckDB intermediate precision differs from Spark |
| `splitAndTransfer()` copies batch data | 8 | Creates new VectorSchemaRoot with corrected schema |

### Violation 3: Post-hoc schema detection via SQL string parsing

Multiple methods in `SparkConnectServiceImpl` parse the generated SQL string to detect what expressions are present (COUNT, SUM/AVG, division patterns), then patch the schema accordingly. This violates principles 1 and 2.

| Method | Lines | What it does |
|--------|-------|-------------|
| `fixCountNullable()` | 2960-3078 | Regex-detects COUNT columns, fixes nullable |
| `fixDecimalPrecisionForComplexAggregates()` | 2314-2433 | Regex-detects arithmetic around aggregates |
| `fixDecimalToDoubleCasts()` | 2833-2941 | Compares logical schema vs DuckDB output, injects CASTs into SQL |
| `fixDivisionDecimalCasts()` | 2621-2750 | Wraps division expressions in DECIMAL casts |
| `detectCountColumns()` | 2978-3020 | Parses SELECT list for COUNT patterns |
| `detectComplexAggregateColumns()` | 2367-2433 | Scans for SUM/AVG with arithmetic |
| `mergeNullableOnly()` | 2242-2275 | Merges nullable flags from logical schema |
| `normalizeAggregateColumnName()` | 2284-2287 | String-replace `spark_sum` → `sum` |

### Violation 4: `transformSparkSQL` regex fallback

When the SparkSQL parser fails, `transformSparkSQL()` falls back to `preprocessSQL()` instead of failing. This masks parser gaps and keeps the regex hacks alive.

**Location**: `SparkConnectServiceImpl.java:816-834`

## Target Architecture

### Data Flow

```
SparkSQL path:
  spark.sql("SELECT ...")
    → ANTLR parse tree (SparkSQLParser)
    → Thunderduck expression tree (typed AST with DuckDB constructs)
    → SQLGenerator.generate()
    → SQL string for DuckDB execution

DataFrame path:
  df.select(...) / df.filter(...) / etc.
    → Spark Connect protobuf (ExpressionConverter + RelationConverter)
    → Thunderduck expression tree (typed AST with DuckDB constructs)
    → SQLGenerator.generate()
    → SQL string for DuckDB execution
```

Both paths converge on the **Thunderduck expression tree** — the single source of truth. No transformation happens after `SQLGenerator.generate()` returns a string. The string goes directly to DuckDB.

### Strict Mode Strategy

Strict mode achieves exact Spark type matching at SQL generation time via two mechanisms:

1. **Extension functions**: `spark_sum`, `spark_avg`, `spark_decimal_div` — produce Spark-correct types and rounding natively inside DuckDB. The expression tree emits these function names instead of vanilla DuckDB equivalents. Column naming uses DuckDB's `AS` aliases to match Spark names (e.g., `spark_sum(x) AS "sum(x)"`), eliminating the need for post-hoc column renaming.

2. **Top-level CASTs**: When an expression's DuckDB return type differs from Spark's expected type, the SQLGenerator wraps the outermost SELECT projection in `CAST(expr AS target_type)`. No intermediate casts — only at the final projection boundary.

This means:
- DuckDB returns results with correct types, correct nullability, correct column names
- No `SchemaCorrectedBatchIterator` needed
- No post-hoc schema merging, no SQL string re-parsing
- Arrow batches flow directly from DuckDB → gRPC serialization

### Relaxed Mode Strategy

Relaxed mode uses vanilla DuckDB constructs with no extension functions and no type-matching CASTs. Values are equivalent but types may differ (e.g., DuckDB returns HUGEINT where Spark returns BIGINT). No result copying — the client receives DuckDB's native types.

## Inventory of Violations and Their Resolution

### Category 1: Transformations that belong in the SparkSQL Parser

These are needed only for the `spark.sql("...")` path. The parser should produce the correct AST so no string fixup is needed.

| ID | Current hack | Resolution |
|----|-------------|------------|
| P1 | Backtick → double-quote (line 838) | Parser uses ANTLR lexer; SQLQuoting handles identifier emission |
| P2 | `returns` keyword alias (line 856) | **Resolved** — `RETURNS` is already in ANTLR grammar's `nonReserved` rule (line 2192) and `ansiNonReserved` (line 1809); no code change needed |
| P3 | String concat `+` → `\|\|` (lines 859-860) | Parser should detect string context and emit `\|\|` operator in BinaryExpression |
| P4 | `at cross join` removal (line 863) | Fix parser's join handling for this edge case |
| P5 | `year()` → `CAST(year() AS INTEGER)` (line 871) | Already done — EXTRACT maps to FunctionCall via parser (commit 28c8618) |
| P6 | `translateNamedStruct()` (lines 1046-1100) | Parser should emit `StructLiteralExpression` (partially done) |
| P7 | `translateMapFunction()` (lines 1105-1168) | Parser should emit `MapLiteralExpression` or `FunctionCall` |
| P8 | `translateStructFunction()` (lines 1173-1201) | Parser should map `struct()` → `row()` via FunctionRegistry |
| P9 | `translateArrayFunction()` (lines 1206-1243) | Parser should map `array()` → `list_value()` via FunctionRegistry |
| P10 | DESC/ASC uppercase (lines 902-903) | Parser produces `Sort.SortOrder` with correct direction enum |

### Category 2: Transformations that belong in the SQLGenerator

These affect both paths. The expression tree is correct, but the SQL generator doesn't emit the right DuckDB SQL.

| ID | Current hack | Resolution |
|----|-------------|------------|
| G1 | `count(*)` auto-aliasing as `"count(1)"` (lines 844-852) | SQLGenerator should emit `count(*) AS "count(1)"` when generating unaliased count(*) in a SELECT |
| G2 | `CAST(TRUNC(x) AS INTEGER)` (lines 881-882) | CastExpression.toSQL() should emit `TRUNC` when target is INTEGER and source is floating-point/decimal |
| G3 | `SUM` → `spark_sum` / `AVG` → `spark_avg` regex (lines 890-891) | SQLGenerator.visitAggregate() already handles this for DataFrame path. Extend to cover all plan shapes (WithColumns, subqueries). Remove regex. |
| G4 | `fixDivisionDecimalCasts()` (line 897, ~130 lines) | In strict mode, SQLGenerator wraps division-of-aggregate expressions in `CAST(... AS DECIMAL(38,6))` at the top-level SELECT projection |
| G5 | ROLLUP NULLS FIRST (lines 908-999, ~90 lines) | SQLGenerator detects Sort above Aggregate-with-ROLLUP and emits NULLS FIRST on all ORDER BY columns |
| G6 | `spark_sum(...)` → `sum(...)` column renaming | SQLGenerator emits `spark_sum(x) AS "sum(x)"` in strict mode, so DuckDB returns the correct column name. Eliminates `normalizeAggregateColumnName()`. |

### Category 3: Schema/type matching that belongs at SQL generation time

These post-hoc schema fixes violate principles 6, 7, 8. In the target architecture, the SQLGenerator produces SQL that makes DuckDB return the exact types Spark expects.

| ID | Current hack | Resolution |
|----|-------------|------------|
| T1 | `fixCountNullable()` — regex-detect COUNT, fix nullable | **Relaxed mode**: not needed (principle 5). **Strict mode**: DuckDB's COUNT already returns non-nullable. If schema reporting is wrong, fix the schema inference query, not the result. The expression tree knows count() is non-nullable — use that directly. |
| T2 | `fixDecimalPrecisionForComplexAggregates()` — promote DECIMAL(28,4) → DECIMAL(38,4) | **Relaxed mode**: not needed. **Strict mode**: `spark_sum`/`spark_avg` extension functions already return Spark-correct precision. If intermediate expressions still have wrong precision, wrap the top-level SELECT projection in `CAST(expr AS DECIMAL(38,s))`. |
| T3 | `fixDecimalToDoubleCasts()` — inject CAST(... AS DOUBLE) into SQL | **Relaxed mode**: not needed. **Strict mode**: SQLGenerator should emit `CAST(expr AS DOUBLE)` at the top-level projection when the expression tree's `dataType()` says DOUBLE but the DuckDB expression would return DECIMAL. This check lives in the expression tree, not in SQL string parsing. |
| T4 | `mergeNullableOnly()` — merge logical schema nullable onto DuckDB schema | Expression tree's `nullable()` method already knows the correct answer. SQLGenerator should produce SQL whose result schema matches. For `analyzePlan`, use the expression tree's schema directly instead of executing SQL and patching. |

### Category 4: Result-set processing to eliminate

| ID | Current code | Resolution |
|----|-------------|------------|
| R1 | `SchemaCorrectedBatchIterator` — copies every Arrow batch | **Eliminate entirely.** With G6 (column naming), T1-T4 (type/nullable correctness at SQL generation time), DuckDB returns Arrow batches with correct schema. Pass them through directly. |
| R2 | `normalizeAggregateColumnName()` | Eliminated by G6 — aliases in generated SQL |
| R3 | `inferSchemaFromDuckDB()` for schema analysis | For DataFrame plans, use the expression tree's `inferSchema()` directly. Only fall back to DuckDB execution for raw SQL where no AST is available. |

## Execution Plan

### Phase 1: Stop calling `preprocessSQL` on the DataFrame path ✅

**Completed**: 2026-02-09 (commit d018b0a)

The DataFrame path no longer calls `preprocessSQL()`. All transformations happen on the AST.

- [x] G1: `count(*)` aliasing → SQLGenerator
- [x] G2: `CAST(TRUNC)` → CastExpression
- [x] G3: Verify `spark_sum`/`spark_avg` coverage in SQLGenerator for all plan shapes
- [x] G5: ROLLUP NULLS FIRST → SQLGenerator
- [x] Remove `preprocessSQL()` call on DataFrame path
- [x] Verify full differential test suite passes without it

### Phase 2: Eliminate result-set copying ✅

**Completed**: 2026-02-09 (commit f0a730f)

`SchemaCorrectedBatchIterator` is now zero-copy (nullable flags only). Extension functions emit `AS` aliases for correct column naming. Schema analysis uses `plan.inferSchema()` directly.

- [x] G6: Emit `AS "sum(...)"` aliases for extension functions in strict mode
- [x] T1: Trust expression tree for COUNT nullability; stop regex detection
- [x] T2: Ensure extension functions return correct DECIMAL precision
- [x] T3: Emit CAST(... AS DOUBLE) in SQLGenerator when expression tree says DOUBLE
- [x] T4: Use expression tree schema for `analyzePlan` instead of DuckDB execution + patching
- [x] R1: Reduce `SchemaCorrectedBatchIterator` to zero-copy (nullable flags only)

### Phase 3: Complete the SparkSQL parser + remove fallback ✅

**Completed**: 2026-02-10 (commits 589e314, 1eb6998)

Two rounds of work. First (589e314): SingleRowRelation SQL generation, CTE support via `WithCTE`, `transformSparkSQLWithPlan()` for schema inference. Second (1eb6998): removed `preprocessSQL()` fallback entirely, added case-insensitive ANTLR lexing via `UpperCaseCharStream`, fixed 14 parser/generator bugs to reach 51/51 TPC-H (747/815 full suite, no regressions).

- [x] P3: String concatenation `+` → `||` — `BinaryExpression.toSQL()` detects string context
- [x] P6: `NAMED_STRUCT` → `struct_pack` — FunctionRegistry CUSTOM_TRANSLATOR
- [x] P7: `MAP()` → DuckDB map — FunctionRegistry CUSTOM_TRANSLATOR
- [x] P8: `struct()` → `row()` — FunctionRegistry DIRECT_MAPPING
- [x] P9: `array()` → `list_value()` — FunctionRegistry DIRECT_MAPPING
- [x] SingleRowRelation: SQLGenerator no-op + skip FROM clause in visitProject
- [x] CTE support: WithCTE logical plan node + visitWithCTE in SQLGenerator
- [x] Raw SQL schema: `transformSparkSQLWithPlan()` returns LogicalPlan for schema inference
- [x] Case-insensitive ANTLR lexing: `UpperCaseCharStream` wrapper
- [x] CROSS JOIN null condition handling in parser
- [x] Remove `preprocessSQL()` fallback — `transformSparkSQL()` now delegates directly to parser
- [x] Fix double AS alias in join optimization paths (Q5, Q7, Q8, Q9, Q11, Q17)
- [x] Fix DATE literal misidentified as string concatenation (Q4, Q15, Q20)
- [x] Add SelectEntry/selectOrder to preserve interleaved column ordering (Q3, Q10)
- [x] Fix table alias scoping in EXISTS subqueries (Q21)
- [x] Fix aggregate function name casing for Spark compatibility (Q18)
- [x] Fix Project.inferSchema column name extraction (Q2, Q15, Q20)

**Resolved** (confirmed no code change needed):
- [x] P2: `returns` keyword — already handled by ANTLR grammar's `nonReserved` rule (line 2192 of SqlBaseParser.g4) and `ansiNonReserved` (line 1809). TPC-DS Q5 and Q80 (which use `returns` as a column alias) pass differential tests. Q77 fails due to P4 (CROSS JOIN bug), not P2.

**Deferred** (out of scope — edge cases):
- P4: `at cross join` (affects 1 query, needs grammar change). Note: TPC-DS Q77 failure is caused by this CROSS JOIN issue, not by the `returns` keyword (P2).
- G4: Division decimal casting (complex strict-mode arithmetic analysis)

### Phase 4: Delete dead code ✅

**Completed**: 2026-02-10 (commits 589e314, 1eb6998)

Two rounds. First (589e314): deleted 3 confirmed dead methods (~137 lines), updated stale comment. Second (1eb6998): deleted `preprocessSQL()`, `translateSparkFunctions()` + 4 sub-methods, `fixDivisionDecimalCasts()` + helpers (~600 additional lines).

- [x] Delete `mergeNullableOnly()` (zero callers, replaced by SchemaCorrectedBatchIterator)
- [x] Delete `fixDecimalToDoubleCasts()` (zero callers, moved to SQL generation in Phase 2)
- [x] Delete `applyDoubleCastsToSelectItems()` (only called by dead `fixDecimalToDoubleCasts`)
- [x] Update stale comment: groupingSets now populated on Aggregate nodes
- [x] Delete `preprocessSQL()` entirely
- [x] Delete `translateSparkFunctions()` and its 4 sub-methods (`translateNamedStruct`, `translateMapFunction`, `translateStructFunction`, `translateArrayFunction`)
- [x] Delete `fixDivisionDecimalCasts()` and `findMatchingParen()`

**Remaining schema-fixup code** (still actively called, not dead code):

These methods are called in the DuckDB schema-inference fallback path (when `plan.inferSchema()` fails or returns null). They apply regex-based schema corrections to DuckDB output. They violate principles 1-2 but cannot be deleted until `inferSchema()` covers all plan shapes.

- [ ] `fixCountNullable()`, `detectCountColumns()`, `collectCountAliases()` — fix nullable flags for COUNT columns
- [ ] `fixDecimalPrecisionForComplexAggregates()`, `detectComplexAggregateColumns()` — promote DECIMAL precision for SUM/AVG
- [ ] `findOutermostKeyword()`, `splitSelectList()` — SQL string parsing helpers used by above
- [ ] `SchemaCorrectedBatchIterator` — zero-copy nullable flag correction (Phase 2 reduced from full copy)

Estimated remaining deletion: **~400 lines** (blocked on `inferSchema()` completeness).

## Extension Function Coverage Gaps

The strict-mode strategy depends on DuckDB extension functions producing Spark-correct types. Current gaps:

| Function | Gap | Impact |
|----------|-----|--------|
| `spark_avg` | Only supports DECIMAL input. No BIGINT/INTEGER overload. | AVG on integer columns may return wrong type in strict mode |
| `spark_sum` | Covers DECIMAL + all integer types. No DOUBLE overload. | SUM on DOUBLE may return wrong type |
| Column naming | Extension functions produce column names like `spark_sum(x)` | Need `AS "sum(x)"` aliases in generated SQL (G6) |

These gaps should be addressed by either adding overloads to the extension or by adding top-level CASTs in the SQLGenerator for the missing input types.

## Bugs Caused by String Hacks (evidence of fragility)

1. **2026-02-09**: `count(*) AS "count(1)"` regex applied BEFORE `OVER` clause, producing `count(*) AS "count(1)" OVER (...)` — invalid SQL. Root cause: regex can't understand SQL semantics.
2. **Ongoing risk**: `SUM(`/`AVG(` regex (line 890-891) matches inside string literals, subqueries, and column names containing "sum" or "avg".
3. **Ongoing risk**: `CAST(TRUNC)` regex (line 881) can't distinguish intentional casts from those needing truncation semantics.
4. **Known bug**: `Aggregate.toSQL()` applies `spark_sum`/`spark_avg` replacement without strict mode guard.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-09 | Adopt 8 architectural principles | Eliminate entire class of string-manipulation bugs; achieve predictable type behavior |
| 2026-02-09 | Phase 1 = DataFrame path first | Highest impact (most queries), lowest risk (AST already available) |
| 2026-02-09 | Phase 2 = eliminate result copying before parser completion | Result copying affects performance on every query; parser gaps only affect raw SQL edge cases |
| 2026-02-09 | Extension functions + top-level CASTs for strict mode | Matches principle 6 — no intermediate casts; high-performance native DuckDB execution |
| 2026-02-09 | Relaxed mode = no CASTs, no extensions, value-equivalent only | Matches principle 5 — best performance mapping to vanilla DuckDB |

## Related Files

| File | Role | Lines affected |
|------|------|---------------|
| `connect-server/.../service/SparkConnectServiceImpl.java` | preprocessSQL, schema fixes, transformSparkSQL | ~800 lines to delete |
| `core/.../runtime/SchemaCorrectedBatchIterator.java` | Arrow batch copying | ~280 lines to delete/gut |
| `core/.../generator/SQLGenerator.java` | SQL generation — gains strict-mode CASTs, aliases, ROLLUP handling | Moderate additions |
| `core/.../expression/CastExpression.java` | CAST generation — gains TRUNC for INTEGER target | Small addition |
| `core/.../parser/SparkSQLAstBuilder.java` | SparkSQL parser — gains MAP, ARRAY, STRUCT, string concat | Medium additions |
| `core/.../functions/FunctionRegistry.java` | Function name mapping — gains struct, array, map entries | Small additions |
| `core/.../types/TypeInferenceEngine.java` | Type inference — may need DuckDB decimal precision modeling | Medium additions |
| `thunderduck-duckdb-extension/src/include/spark_aggregates.hpp` | Extension functions — may need new overloads | Small additions |
