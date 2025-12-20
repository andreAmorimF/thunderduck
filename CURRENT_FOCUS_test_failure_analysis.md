# Current Focus: Test Failure Root Cause Analysis

## Objective

Analyze the 451 failed tests from `run-differential-tests-v2.sh` to:
1. Cluster failures by root cause
2. Identify the most impactful fixes
3. Address root causes systematically

## Current State (as of latest run)

- **Failed**: 451
- **Passed**: 121
- **Skipped**: 2
- **Pass Rate**: ~21%

## Root Cause Analysis (Refined)

### Issue #1: GroupBy/Aggregate Schema Loss (~238 tests)

**Symptoms**:
- Column names become `group_0`, `group_1`, `group_2` instead of actual names
- All types become `StringType()`
- Affects TPC-DS (205), TPC-H DataFrame tests (33)

**Example**:
```
Reference: l_orderkey (LongType), o_orderdate (DateType), revenue (DecimalType)
Test: group_0 (StringType), group_1 (StringType), revenue (StringType)
```

**Root Cause**: `Aggregate` or `Project` logical plan doesn't preserve column names/types through groupBy operations.

**Files to Fix**: `SQLGenerator.java` - aggregate SQL generation, `Project.java` - schema inference

---

### Issue #2: Window Functions Return LongType Instead of IntegerType (~35 tests)

**Symptoms**:
- `row_number`, `rank`, `dense_rank`, `ntile` return `LongType()` instead of `IntegerType()`
- Nullable mismatches (all columns marked nullable when they shouldn't be)

**Example**:
```
Column 'row_num': type mismatch - Reference=IntegerType(), Test=LongType()
Column 'department': nullable mismatch - Reference=False, Test=True
```

**Root Cause**:
1. DuckDB window functions return BIGINT, not INT
2. Nullable inference doesn't account for NOT NULL columns

**Files to Fix**:
- `ExpressionConverter.java` - window function return type inference
- Schema inference for nullable

---

### Issue #3: USING Join Column Deduplication (~10 tests)

**Symptoms**:
- Column count mismatch (Test has extra columns)
- USING join columns appear twice instead of once

**Example**:
```
Reference: 3 columns (id, name1, name2)
Test: 4 columns (includes duplicate id column)
```

**Root Cause**: USING join doesn't deduplicate the join column in the output.

**Files to Fix**: `Join.java` or SQL generation for USING joins

---

### Issue #4: Missing SQL Syntax Translation (~70 tests)

**Symptoms**:
- `Parser Error: syntax error at or near "10"` for `ARRAY(10, 20, 30)`
- `Scalar Function with name named_struct does not exist`

**Functions needing translation**:
| Spark SQL | DuckDB Equivalent |
|-----------|------------------|
| `ARRAY(1, 2, 3)` | `[1, 2, 3]` or `list_value(1, 2, 3)` |
| `MAP('a', 1)` | `MAP(['a'], [1])` |
| `NAMED_STRUCT('x', 1)` | `{'x': 1}` or `struct_pack(x := 1)` |

**Files to Fix**: `ExpressionConverter.java` - literal handling, `SQLGenerator.java` - SQL output

---

### Issue #5: Missing DuckDB Functions (~30 tests)

| Function | Count | DuckDB Alternative |
|----------|-------|-------------------|
| `named_struct` | 22 | struct literal syntax |
| `list_union` | 2 | Need custom implementation |
| `list_except` | 2 | Need custom implementation |
| `explode_outer` | 2 | Need LATERAL JOIN pattern |
| `array_join` | 2 | `list_string_agg` |
| `initcap` | 2 | Custom UDF or implementation |

---

### Issue #6: Type Width Mismatches (~60 tests)

| Pattern | Count | Fix |
|---------|-------|-----|
| IntegerType vs LongType | 52 | Ensure INT functions return INT |
| LongType vs DecimalType | 20 | Map decimal types correctly |
| ByteType vs LongType | 12 | Small int handling |

---

### Issue #7: Lambda Function Tests (~18 tests)

All lambda tests fail - likely `transform`, `filter`, `aggregate` function translation issues.

---

### Issue #8: Nullable Inference (~50+ tests)

Many tests fail only due to nullable mismatches. Thunderduck marks columns as `nullable=True` when Spark says `nullable=False`.

---

## Priority Fix Order (by Impact)

| Priority | Issue | Tests Fixed | Effort |
|----------|-------|-------------|--------|
| 1 | GroupBy Schema Loss | ~238 | Medium |
| 2 | SQL Syntax (ARRAY, MAP, STRUCT) | ~70 | Low |
| 3 | Window Function Types | ~35 | Low |
| 4 | Missing Functions | ~30 | Medium |
| 5 | Lambda Functions | ~18 | Medium |
| 6 | USING Join Dedup | ~10 | Low |
| 7 | Type Width | ~60 | Low |
| 8 | Nullable Inference | ~50+ | Medium |

## Next Steps

1. [ ] **Fix GroupBy schema loss** - highest impact
   - Debug why groupBy loses column names/types
   - Check `Aggregate.toSQL()` and `Project.inferSchema()`

2. [ ] **Add ARRAY/MAP/STRUCT literal translation**
   - Update `ExpressionConverter` for complex type literals

3. [ ] **Fix window function return types**
   - Cast DuckDB BIGINT results to INT for ranking functions

4. [ ] **Implement missing functions**
   - Add `list_union`, `list_except`, `explode_outer` translations

## Investigation Commands

```bash
# Run specific test with verbose output
python3 -m pytest differential/test_tpcds_differential.py::TestTPCDS_DataFrame_Differential::test_q3_dataframe -v -s

# Check generated SQL
grep "Generated SQL" /tmp/server.log

# Run window function tests only
python3 -m pytest differential/test_window_functions.py -v
```
