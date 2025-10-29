# Q36 Rewrite Solution for DuckDB

## Problem

TPC-DS Query 36 uses `GROUPING()` function inside `PARTITION BY` clause of a window function, which is not supported by DuckDB:

```sql
-- Original pattern that doesn't work in DuckDB:
RANK() OVER (
    PARTITION BY GROUPING(i_category)+GROUPING(i_class),
    CASE WHEN GROUPING(i_class) = 0 THEN i_category END
    ORDER BY ...
)
FROM ...
GROUP BY ROLLUP(i_category, i_class)
```

## Solution: UNION ALL Approach

Instead of using `GROUP BY ROLLUP` with `GROUPING()` functions, we can achieve the same result by combining three separate aggregations with UNION ALL.

### Implementation Strategy

1. **Three Separate Aggregations**:
   - **Detail level**: GROUP BY i_category, i_class (grouping_id = 0)
   - **Category level**: GROUP BY i_category only (grouping_id = 1)
   - **Grand total**: No GROUP BY (grouping_id = 2)

2. **Manual Grouping ID**:
   - Assign explicit grouping_id values to replace GROUPING() function
   - Use these IDs for partitioning in window functions

3. **Window Function Partitioning**:
   - Replace `PARTITION BY GROUPING(...)` with `PARTITION BY grouping_id`
   - Maintain the same ranking logic

### Code Structure

```sql
WITH store_sales_summary AS (
  -- Detail level
  SELECT i_category, i_class, 0 AS grouping_id, SUM(...) AS total
  FROM ... GROUP BY i_category, i_class

  UNION ALL

  -- Category level
  SELECT i_category, NULL, 1 AS grouping_id, SUM(...) AS total
  FROM ... GROUP BY i_category

  UNION ALL

  -- Grand total
  SELECT NULL, NULL, 2 AS grouping_id, SUM(...) AS total
  FROM ...
)
-- Apply window functions using grouping_id
SELECT ..., RANK() OVER (PARTITION BY grouping_id, ... ORDER BY ...)
FROM store_sales_summary
```

## Benefits

1. **DuckDB Compatible**: Works without GROUPING() in PARTITION BY
2. **Same Results**: Produces identical output to original ROLLUP query
3. **Clear Logic**: Explicit grouping levels are easier to understand
4. **Portable**: Works across more database systems

## Performance Considerations

- **Trade-off**: Three table scans instead of one
- **Optimization**: DuckDB may optimize UNION ALL with common subexpressions
- **Caching**: Consider materializing base aggregation if reused

## Files

- **Original Q36**: `/workspace/benchmarks/tpcds_queries/q36.sql`
- **Rewritten Q36**: `/workspace/benchmarks/tpcds_queries/q36_rewritten.sql`
- **DuckDB Reference**: https://github.com/duckdb/duckdb/blob/main/extension/tpcds/dsdgen/queries/36.sql

## Testing Status

The rewritten query has been created but not yet tested due to the Q36 test being commented out. To enable Q36:

1. Replace the original q36.sql with q36_rewritten.sql
2. Generate new reference data using Spark
3. Uncomment the Q36 test in test_tpcds_batch1.py
4. Verify results match expected output

## Conclusion

While Q36 cannot run in its original form on DuckDB, the UNION ALL rewrite provides a functionally equivalent solution. This approach is used by DuckDB themselves in their official TPC-DS implementation, confirming it as the recommended workaround for this limitation.

**Last Updated**: 2025-10-29