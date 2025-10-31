# TPC-DS DataFrame API Compatibility Report

## Executive Summary

Based on systematic analysis of all 99 TPC-DS queries, **34 queries (34.3%)** can be implemented using pure Spark DataFrame API without requiring SQL-specific features. The remaining **65 queries (65.7%)** require SQL features that are not available or are too complex to express in DataFrame API.

## Compatible Queries (34 queries)

These queries can be fully implemented using DataFrame API operations:

### Query List
```
[3, 7, 9, 12, 13, 15, 17, 19, 20, 25, 26, 29, 32, 37, 40, 41, 42, 43, 45, 48, 50, 52, 55, 62, 71, 72, 82, 84, 85, 91, 92, 96, 98, 99]
```

### Characteristics of Compatible Queries
- Simple joins and aggregations
- Basic filtering and grouping
- Standard window functions
- CASE statements (expressible with `when`)
- Simple UNION operations
- Date arithmetic operations

## Incompatible Queries (65 queries)

These queries require SQL features that cannot be easily expressed in DataFrame API:

### Query List
```
[1, 2, 4, 5, 6, 8, 10, 11, 14, 16, 18, 21, 22, 23, 24, 27, 28, 30, 31, 33, 34, 35, 36, 38, 39, 44, 46, 47, 49, 51, 53, 54, 56, 57, 58, 59, 60, 61, 63, 64, 65, 66, 67, 68, 69, 70, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 86, 87, 88, 89, 90, 93, 94, 95, 97]
```

### Incompatible Features by Category

#### 1. CTEs (Common Table Expressions) - 26 queries
**Queries**: [1, 2, 4, 5, 11, 30, 31, 33, 47, 51, 54, 56, 57, 58, 59, 60, 64, 74, 75, 77, 78, 80, 81, 83, 95, 97]

CTEs allow for hierarchical query construction and recursive patterns that have no direct DataFrame equivalent.

#### 2. Subqueries in FROM Clause - 36 queries
**Queries**: [2, 5, 6, 8, 21, 28, 33, 34, 38, 44, 46, 49, 51, 53, 54, 56, 59, 60, 61, 63, 65, 66, 67, 68, 70, 73, 75, 76, 77, 79, 80, 87, 88, 89, 90, 93]

Complex subqueries in the FROM clause require creating intermediate DataFrames that can be difficult to manage.

#### 3. ROLLUP Operations - 10 queries
**Queries**: [5, 18, 22, 27, 36, 67, 70, 77, 80, 86]

ROLLUP provides hierarchical aggregation not directly supported in DataFrame API.

#### 4. GROUPING() Function - 4 queries
**Queries**: [27, 36, 70, 86]

The GROUPING() function for identifying aggregation levels has no DataFrame equivalent.

#### 5. EXISTS/NOT EXISTS - 5 queries
**Queries**: [10, 16, 35, 69, 94]

Correlated EXISTS patterns are complex to express with DataFrame joins.

#### 6. INTERSECT/EXCEPT - 3 queries
**Queries**: [8, 38, 87]

Set operations beyond UNION require complex workarounds in DataFrame API.

## Implementation Strategy

### For Compatible Queries
1. Implement using pure DataFrame API operations
2. Focus on correctness and readability
3. Optimize for performance where possible
4. Validate against SQL reference implementations

### For Incompatible Queries
Per user guidance: **These queries are excluded from DataFrame API testing**
- Already covered by SQL parity test suite
- No value in forcing complex SQL patterns into DataFrame API
- Focus testing on queries that naturally fit DataFrame paradigm

## Testing Approach

### Phase 1: Spark Validation
- Test all 34 compatible queries
- Compare DataFrame results with SQL results
- Ensure exact match (order-independent comparison)

### Phase 2: ThunderDuck Differential Testing
- Run same 34 queries on ThunderDuck
- Compare with validated Spark results
- Identify any compatibility gaps

## Recommendations

### For Users
1. **Use DataFrame API for**:
   - Simple to moderate complexity queries
   - ETL operations
   - Data transformations
   - Queries matching the 34 compatible patterns

2. **Use SQL for**:
   - Complex analytical queries
   - Queries with CTEs, ROLLUP, etc.
   - Queries from the 65 incompatible list

### For Development
1. Focus DataFrame API optimization on the 34 compatible query patterns
2. Ensure robust SQL support for complex analytical workloads
3. Document patterns clearly for users

## Conclusion

The analysis shows that approximately 1/3 of TPC-DS queries can be expressed in pure DataFrame API, while 2/3 require SQL-specific features. This aligns with real-world usage where:
- DataFrame API excels at programmatic data manipulation
- SQL is preferred for complex analytical queries

By focusing on the 34 compatible queries, we can thoroughly test DataFrame API compatibility without forcing unnatural implementations of SQL-specific patterns.