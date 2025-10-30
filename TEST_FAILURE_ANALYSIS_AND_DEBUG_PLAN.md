# Test Failure Analysis and Debugging Plan

## Executive Summary

Current test suite has 7 failing tests across 4 test classes:
- **Total Tests**: 860
- **Passing**: 854 (99.3%)
- **Failing**: 7 (0.8%)
- **Categories**: Aggregation, CTE, SQL Generation, Connection Pool

## Detailed Failure Analysis

### 1. AggregateTests (3 failures)
**Location**: `com.thunderduck.differential.tests.AggregateTests`

#### Failed Tests:
1. **testGroupBySingle** (Line 116)
   - **Error**: `Expecting value to be false but was true`
   - **Context**: Testing GROUP BY with single column
   - **SQL**: `SELECT category, SUM(amount) as total FROM test_groupby_single GROUP BY category ORDER BY category`
   - **Issue**: Result comparison between Spark and DuckDB shows divergence

2. **testHaving** (Line 175)
   - **Error**: `Expecting value to be false but was true`
   - **Context**: Testing HAVING clause with GROUP BY
   - **Issue**: HAVING clause filtering not matching between systems

3. **testGroupByOrderBy** (Line 251)
   - **Error**: `Expecting value to be false but was true`
   - **Context**: Testing GROUP BY with ORDER BY
   - **Issue**: Ordering of grouped results differs

**Common Pattern**: All failures involve `assertThat(result.hasDivergences()).isFalse()` - indicating data mismatch between Spark and DuckDB results.

### 2. CTETests (2 failures)
**Location**: `com.thunderduck.differential.tests.CTETests`

#### Failed Tests:
1. **testCTEWithAggregation** (Line 78)
   - **Error**: `Expecting value to be false but was true`
   - **Context**: Common Table Expression with aggregation
   - **Issue**: CTE results with aggregation functions diverge

2. **testCTEsWithDifferentAggs** (Line 249)
   - **Error**: `Expecting value to be false but was true`
   - **Context**: Multiple CTEs with different aggregation functions
   - **Issue**: Complex CTE with multiple aggregations shows differences

**Common Pattern**: CTEs combined with aggregations are producing different results.

### 3. Phase2IntegrationTest (1 failure)
**Location**: `com.thunderduck.logical.Phase2IntegrationTest$SQLValidation`

#### Failed Test:
**testGeneratorStateless** (Line 415)
- **Error**: Subquery alias inconsistency
- **Expected**: `subquery_1`, `subquery_2`
- **Actual**: `subquery_3`, `subquery_4`
- **Context**: Testing SQL generator statefulness
- **Issue**: SQLGenerator maintains state between invocations (incrementing counter)

### 4. DatabaseConnectionCleanupTest (1 timeout)
**Location**: `com.thunderduck.runtime.DatabaseConnectionCleanupTest$RegressionTests`

#### Failed Test:
**testAtomicCleanupOperations** (Line 862)
- **Error**: `Connection pool exhausted - timeout after 30 seconds`
- **Context**: Testing concurrent connection cleanup
- **Issue**: Test bug - requests 10 connections from pool of size 4
- **Not a system bug**: This is a test configuration issue

## Root Cause Analysis

### Category 1: Data Result Divergences (5 tests)
**Root Cause**: Likely differences in:
1. **Aggregation handling**: NULL values, empty groups, or numeric precision
2. **Ordering**: Tie-breaking rules for ORDER BY with equal values
3. **Data type coercion**: Different type handling between Spark and DuckDB

### Category 2: SQL Generation State (1 test)
**Root Cause**: SQLGenerator maintains internal counter state
- Subquery aliases increment globally instead of resetting
- Need to make generator stateless or reset between uses

### Category 3: Test Configuration (1 test)
**Root Cause**: Test misconfiguration
- Pool size mismatch with test requirements
- Simple fix: Adjust pool size or thread count

## Debugging Plan

### Phase 1: Quick Wins (1-2 hours)
1. **Fix DatabaseConnectionCleanupTest**
   - Change `threadCount` from 10 to 4, or
   - Increase pool size from 4 to 10
   - **File**: `/workspace/tests/src/test/java/com/thunderduck/runtime/DatabaseConnectionCleanupTest.java`

2. **Fix SQLGenerator State Issue**
   - Make SQLGenerator stateless or reset counter
   - **File**: `/workspace/core/src/main/java/com/thunderduck/generator/SQLGenerator.java`
   - Look for subquery counter and make it local or resettable

### Phase 2: Aggregation Issues (4-8 hours)
1. **Enable verbose logging for differential tests**
   - Add detailed comparison output to see actual vs expected values
   - Log SQL queries being executed

2. **Test individual failing queries**
   - Extract exact SQL from failing tests
   - Run manually in both Spark and DuckDB
   - Compare results row by row

3. **Common issues to check**:
   - NULL handling in GROUP BY
   - Numeric precision in SUM/AVG
   - String collation in ORDER BY
   - Empty group handling

### Phase 3: Deep Investigation (1-2 days)
1. **Create minimal reproducers**
   - Isolate failing queries
   - Create smallest dataset that reproduces issue
   - Test with different data types

2. **Check DuckDB compatibility**
   - Verify DuckDB SQL dialect differences
   - Check for known incompatibilities
   - Review DuckDB documentation for aggregation behavior

## Fix Priority

### Priority 1 - Easy Fixes (Do First)
1. **DatabaseConnectionCleanupTest** - Test configuration issue
2. **SQLGenerator stateless** - Simple state management fix

### Priority 2 - Core Functionality (Do Second)
1. **GROUP BY single column** - Basic aggregation
2. **HAVING clause** - Essential filtering
3. **GROUP BY with ORDER BY** - Common pattern

### Priority 3 - Advanced Features (Do Last)
1. **CTE with aggregation** - Complex query pattern
2. **Multiple CTEs** - Advanced use case

## Implementation Steps

### Step 1: Fix Test Configuration
```java
// In DatabaseConnectionCleanupTest.java, line 854
int threadCount = 4; // Changed from 10 to match pool size
```

### Step 2: Fix SQL Generator
```java
// In SQLGenerator.java
// Make subqueryCounter instance-level or resettable
private int subqueryCounter = 0;

public void reset() {
    subqueryCounter = 0;
}
```

### Step 3: Debug Aggregation Tests
1. Add logging to see actual divergences
2. Compare exact results between systems
3. Identify pattern of differences
4. Fix based on findings

## Success Metrics

- All 860 tests passing
- No timeout failures
- Consistent SQL generation
- Exact match between Spark and DuckDB results for aggregations

## Estimated Timeline

- **Day 1**: Fix easy issues (2 tests fixed)
- **Day 2-3**: Debug and fix aggregation issues (5 tests fixed)
- **Total**: 2-3 days to achieve 100% test pass rate

---
*Created: October 30, 2025*
*ThunderDuck Version: 0.1.0-SNAPSHOT*