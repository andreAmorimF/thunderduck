# TPC-DS Q5 Issue Analysis

**Date**: 2025-10-27
**Query**: TPC-DS Q5 (Sales/Returns with ROLLUP)
**Status**: Issue identified - NULL ordering difference

---

## Issue

Q5 test fails with 378 value mismatches out of 500 comparisons (100 rows × 5 columns).

**Error Pattern**:
```
Row 0, 'channel': None vs catalog channel
Row 0, 'id': None vs catalog_pageAAA...
Row 0, 'sales': 112458734.7 vs 163753.93
```

---

## Root Cause: NULL Ordering Difference

Q5 uses `GROUP BY ROLLUP (channel, id)` which creates hierarchical subtotals with NULL values.

The query ends with:
```sql
ORDER BY channel, id
LIMIT 100
```

**Default NULL Ordering**:
- **Spark SQL**: NULLs FIRST (SQL standard)
- **DuckDB**: NULLs LAST

**Result**:
- Spark Row 0: (NULL, NULL, 112M) ← Grand total
- DuckDB Row 0: (catalog channel, NULL, 38M) ← Channel subtotal

**Same data, different sort order!**

---

## Proof

Test with DuckDB:
```python
ORDER BY channel, id
→ catalog, store, web, NULL  (NULLs LAST)

ORDER BY channel NULLS FIRST, id NULLS FIRST  
→ NULL, catalog, store, web  (NULLs FIRST)
```

---

## Solutions

### Option 1: Configure DuckDB (Best for compatibility)
Set DuckDB to use NULLS FIRST by default:
```sql
SET default_null_order = 'NULLS FIRST';
```

### Option 2: Modify Test (Quick fix)
Sort both result sets before comparing (order-independent comparison)

### Option 3: Rewrite Queries (Not recommended)
Add explicit NULLS FIRST to all ORDER BY clauses

---

## Recommendation

**Option 1**: Configure DuckDB to match Spark's NULL ordering.

This ensures maximum Spark compatibility for all queries, not just Q5.

**Implementation**: Add to DuckDB connection initialization:
```java
connection.execute("SET default_null_order = 'NULLS FIRST'");
```

---

## Impact

- This affects any query with ORDER BY and ROLLUP/CUBE
- Likely affects multiple TPC-DS queries
- Not a ROLLUP bug - ROLLUP works correctly
- Pure ordering/collation issue

---

**Status**: Root cause identified, solution clear
**Next**: Implement DuckDB NULL ordering configuration
