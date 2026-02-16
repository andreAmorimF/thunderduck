# M85: Bug Fixes, Previously-Skipped Tests, and Architecture Cleanup

**Date**: 2026-02-16

## Summary

Resolved 17 previously-skipped differential tests, fixed 5 open issues, eliminated the `rewriteSQL()` preprocessing path, and implemented full `from_json` schema support. Test suite grew from 802/20/0 (passed/skipped/failed) to 830/3/0.

## Test Progress

| Metric | M84 | M85 | Delta |
|--------|-----|-----|-------|
| Differential tests total | 822 | 833 | +11 |
| Passed | 802 | 830 | +28 |
| Skipped | 20 | 3 | -17 |
| Failed | 0 | 0 | 0 |
| Test files | 37 | 41 | +4 |

**Remaining 3 skips**: `percentile_approx` (algorithmic difference), `schema_of_json` (relaxed mode format), `test_overlay_binary` (BLOB overlay).

## Changes by Theme

### 1. Extension Function: spark_skewness() (commit 441aa3c)

Implemented `spark_skewness()` as a DuckDB extension function for strict mode. Computes population skewness (`mu_3 / mu_2^(3/2)`) matching Spark exactly, vs DuckDB's built-in which uses sample bias correction.

### 2. Previously-Skipped Tests Fixed (commits 5eca8f2, 1160810, d01461d, 6137994)

14 tests that were previously skipped now pass:

| Test | Function | Fix |
|------|----------|-----|
| `test_overlay` | `overlay` | Custom translator: `LEFT() \|\| replacement \|\| SUBSTR()` |
| `test_octet_length` | `octet_length` | Custom translator: `strlen()` (byte length for VARCHAR) |
| `test_bit_get` | `bit_get` | Custom translator: `(value >> pos) & 1` (bitwise math) |
| `test_array_prepend` | `array_prepend` | Custom translator: `list_prepend()` with swapped args |
| `test_width_bucket` | `width_bucket` | CASE expression emulating histogram bucket assignment |
| `test_dayname` | `dayname` | `LEFT(dayname(...), 3)` truncates to abbreviation |
| `test_monthname` | `monthname` | `LEFT(monthname(...), 3)` truncates to abbreviation |
| `test_kurtosis` | `kurtosis` | Mapped to `kurtosis_pop` (population formula) |
| `test_kurtosis_grouped` | `kurtosis` | Same fix |
| `test_skewness` | `skewness` | `spark_skewness()` extension function (strict) |
| `test_percentile_*` (4 tests) | `percentile` | Mapped to `quantile_cont` (linear interpolation) |

### 3. Bug Fixes (commits 14db93b, 5c84132, d01461d)

**split(str, pattern, limit)** -- The 3rd argument (limit) was silently dropped. Now emulated via CASE + list slicing to match Spark's limit semantics: positive limit splits into at most N parts, negative limit uses all splits.

**UNION type checking** -- Previously only checked column count, not types. `Union.inferSchema()` now computes widened types via `TypeInferenceEngine.unifyTypes()`, and `SQLGenerator.visitUnion()` wraps sides with CASTs when types differ. Fixes silent type mismatches like UNION of INTEGER and DOUBLE columns.

**5 open issues** (commit d01461d):
- `soundex`: Custom translator implementing American Soundex (H/W transparent, vowels separate)
- `dropFields()`: Generates `struct_pack()` excluding dropped field with proper schema resolution
- `json_tuple`: Parser now expands `json_tuple(json, k1, k2) AS (a1, a2)` into N separate projections
- `schema_of_json`: Extension function `spark_schema_of_json()` for strict mode DDL output
- Negative array index: Documented as intentionally not addressed (only detectable for literals)

### 4. Architecture: Eliminate rewriteSQL() (commit 0c7ce88)

`ExpressionString` protobuf messages (from `selectExpr()`, `expr()`) previously bypassed the ANTLR parser and went directly to DuckDB as raw SQL via `rewriteSQL()`. This created a shadow code path that skipped FunctionRegistry translation entirely.

**Fix**: ExpressionString now routes through `SparkSQLParser` like all other SQL. The `rewriteSQL()` method and all its SQL string manipulation helpers were deleted. This means `selectExpr("from_json(...)")` now correctly goes through FunctionRegistry translation.

### 5. from_json Full Schema Support (current session, uncommitted)

`from_json(jsonStr, schema)` previously mapped to DuckDB's basic `json()`, ignoring the schema. Now uses `json_transform(jsonStr, duckdbSchemaJSON)` for proper typed parsing.

**Changes**:
- `SchemaParser.parseField()`: Now supports space-separated DDL format (`name STRING`) in addition to colon-separated (`name:string`)
- `SchemaParser.parseType()`: Fixed nested struct parsing (was throwing `UnsupportedOperationException`)
- `SchemaParser.toDuckDBJsonSchema()`: New method converts Spark schema string (DDL or JSON) to DuckDB's JSON schema format
- `FunctionRegistry`: `from_json` translator extracts schema literal, converts via `toDuckDBJsonSchema()`, emits `json_transform()`
- 5 new differential tests: DDL schema, missing keys, NULL input, array fields, DataFrame API

## Files Modified

**New files:**
- `docs/dev_journal/M85_COMPLETION_REPORT.md`

**Modified source files:**
- `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java` -- from_json translator, width_bucket, overlay, octet_length, bit_get, array_prepend, soundex, split limit, dayname, monthname custom translators
- `core/src/main/java/com/thunderduck/types/SchemaParser.java` -- Nested struct support, space-separated DDL, toDuckDBJsonSchema()
- `core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java` -- Type unification for UNION
- `core/src/main/java/com/thunderduck/logical/Union.java` -- Type-widened schema inference
- `core/src/main/java/com/thunderduck/generator/SQLGenerator.java` -- CAST generation for UNION type mismatches, rewriteSQL() removal
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java` -- ExpressionString routing through ANTLR parser

**Modified test files:**
- `tests/integration/differential/test_json_functions_differential.py` -- 5 new from_json tests

## Commits (9 committed + 1 uncommitted)

```
0c7ce88 Eliminate rewriteSQL() by routing ExpressionString through ANTLR parser
0887fb6 Minor cleanup: fix duplicate Javadoc, unused import, deduplicate bit_get lambda
d01461d Fix 5 open issues: soundex, dropFields, json_tuple, schema_of_json, negative array index
6137994 Update function coverage doc: mark width_bucket, UNION type checking, split limit, dayname/monthname as fixed
14db93b Fix split(str, pattern, limit): support 3rd argument instead of dropping it
5c84132 Fix UNION type checking: add type widening and CAST generation for mismatched column types
1160810 Fix width_bucket: emulate with CASE expression since DuckDB lacks this function
5eca8f2 Fix 4 skipped differential tests: overlay, octet_length, bit_get, array_prepend
441aa3c Implement spark_skewness() extension function for exact Spark skewness parity
(uncommitted) Implement full from_json with schema support via json_transform
```
