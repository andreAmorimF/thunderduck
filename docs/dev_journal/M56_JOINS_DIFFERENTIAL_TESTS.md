# M56: Joins Differential Tests and USING Join Bug Fix

**Date:** 2025-12-22
**Milestone:** M56
**Focus:** Comprehensive differential tests for join operations and USING join column deduplication fix

---

## Summary

Created differential tests for all join types and fixed a critical bug in USING join column deduplication. The USING join bug caused join columns to appear twice in the output schema, breaking compatibility with Spark.

## Achievements

### 1. Join Differential Tests (26 total)

**ON-clause joins (15 tests in test_joins_differential.py):**
- Inner, Left, Right, Full outer joins with ON condition
- Cross join (Cartesian product)
- Left semi join (rows with matches)
- Left anti join (rows without matches)
- Composite key joins (multiple columns)
- Inequality joins (non-equals conditions)
- OR condition joins
- Self joins
- Multi-table joins (3-way joins)
- Join with filter
- Join with aggregation

**USING joins (11 tests in test_using_joins_differential.py):**
- Single column USING join
- Multi-column USING join
- USING with LEFT, RIGHT, FULL outer joins
- USING join with select, filter, aggregation
- USING join with Parquet tables

### 2. USING Join Bug Fix

**Problem:** USING joins duplicated the join column in output:
- Spark: `df1.join(df2, "id")` → `(id, name, value)` - 3 columns
- Thunderduck: → `(id, name, id, value)` - 4 columns (BUG)

**Root Cause:** The USING column information was lost after being converted to an ON condition, so there was no way to deduplicate columns in schema inference or SQL generation.

**Solution:**

1. **Join.java** - Track USING columns:
   ```java
   private final List<String> usingColumns;

   // Constructor overload accepting USING columns
   public Join(LogicalPlan left, LogicalPlan right, JoinType joinType,
               Expression condition, List<String> usingColumns)

   // Updated inferSchema() - USING columns first, then non-USING left, then right
   ```

2. **SQLGenerator.java** - Generate explicit column list:
   ```java
   private String generateJoinSelectClause(Join plan, String leftAlias, String rightAlias) {
       // For USING joins: SELECT col1, col2, ... (explicit deduplication)
       // For non-USING: SELECT *
       // For RIGHT/FULL outer USING: COALESCE(left.col, right.col) for join columns
   }
   ```

3. **RelationConverter.java** - Pass USING columns to Join:
   ```java
   List<String> usingColumnNames = join.getUsingColumnsList();
   return new Join(left, right, joinType, condition, usingColumnNames);
   ```

4. **RelationConverter.java** - Fix schema propagation for withColumnRenamed:
   ```java
   // Compute output schema by applying renames to input schema
   StructType outputSchema = computeRenamedSchema(inputSchema, renameMap);
   return new SQLRelation(sql, outputSchema);
   ```

### 3. Column Ordering Fix

**Issue:** Spark puts USING columns first in the output schema, but Thunderduck was preserving original left-side order.

**Fix:** Updated both `Join.inferSchema()` and `generateJoinSelectClause()` to follow Spark's column ordering:
1. USING columns first (in USING clause order)
2. Non-USING columns from left side
3. Non-USING columns from right side

### 4. RIGHT/FULL Outer USING Join Fix

**Issue:** For RIGHT and FULL outer USING joins, the left side can be NULL, so taking the join column from left side returns NULL.

**Fix:** Use COALESCE for USING columns in RIGHT/FULL outer joins:
```sql
SELECT COALESCE(left.id, right.id) AS id, left.name, right.value ...
```

## Files Modified

| File | Changes |
|------|---------|
| `core/src/main/java/com/thunderduck/logical/Join.java` | Added usingColumns field, constructor, getter; updated inferSchema() |
| `core/src/main/java/com/thunderduck/generator/SQLGenerator.java` | Added generateJoinSelectClause() method |
| `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` | Pass USING columns to Join; fix schema propagation in withColumnRenamed |
| `tests/integration/differential/test_joins_differential.py` | NEW: 15 tests for ON-clause joins |

## Test Results

- **26/26 join tests pass** (15 ON-clause + 11 USING)
- All existing tests continue to pass
- Build successful

## Technical Details

### Schema Propagation Issue

The Parquet USING join test (`test_using_join_parquet_tables`) was failing because `withColumnRenamed()` creates an `SQLRelation` without schema information. When the join tried to get the right side's schema for column deduplication, it got an empty schema.

Fixed by computing and passing the renamed schema to `SQLRelation`:
```java
StructType inputSchema = input.schema();
if (inputSchema != null && !inputSchema.fields().isEmpty()) {
    List<StructField> outputFields = new ArrayList<>();
    for (StructField field : inputSchema.fields()) {
        String newName = renameMap.getOrDefault(field.name(), field.name());
        outputFields.add(new StructField(newName, field.dataType(), field.nullable()));
    }
    outputSchema = new StructType(outputFields);
}
return new SQLRelation(sql, outputSchema);
```

## Metrics

- **Tests added:** 15 (ON-clause joins)
- **Tests fixed:** 11 (USING joins, from 2/11 to 11/11 passing)
- **Total test count:** 363 → 389
- **Files modified:** 4
- **Bug fixed:** USING join column deduplication

---

**Status:** Complete
**Next:** Set operations differential tests (union, intersect, except)
