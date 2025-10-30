# Week 17 Implementation Plan: TPC-DS DataFrame API Validation

## Mission Statement
Implement all 100 TPC-DS queries using Spark DataFrame API and validate ThunderDuck produces identical results to native Spark, ensuring complete DataFrame API compatibility for production workloads.

## Current State (End of Week 16)
- ✅ 100% DataFrame API feature coverage achieved
- ✅ All 656 unit tests passing (0 failures)
- ✅ 93/100 TPC-DS SQL queries passing
- ✅ Window functions, complex joins, date/time operations implemented
- ⚠️ DataFrame API implementations not yet validated against Spark

## Week 17 Objectives

### Primary Goals
1. **Implement all 100 TPC-DS queries using DataFrame API**
2. **Run queries on both Spark and ThunderDuck**
3. **Validate result equivalence (differential testing)**
4. **Fix any discrepancies found**
5. **Achieve 100% TPC-DS DataFrame API compatibility**

### Success Criteria
- [ ] All 100 TPC-DS queries implemented in DataFrame API
- [ ] 95%+ queries producing identical results between Spark and ThunderDuck
- [ ] Comprehensive test suite with automated validation
- [ ] Performance benchmarks documented
- [ ] Migration guide updated with findings

## 5-Day Implementation Schedule

### Day 1 (Monday): Framework Setup & Queries 1-20
**Morning (4 hours)**
- [ ] Create differential testing framework
  - Spark execution engine
  - ThunderDuck execution engine
  - Result comparison utilities
  - Error reporting system
- [ ] Set up test data loading for both systems
- [ ] Create base test class with helper methods

**Afternoon (4 hours)**
- [ ] Implement TPC-DS Q1-Q10 in DataFrame API
- [ ] Validate against SQL reference results
- [ ] Document any API gaps discovered

**Deliverables**:
- `/workspace/tests/integration/tpcds_dataframe/` test structure
- `DifferentialTestFramework.py`
- First 10 queries passing validation

### Day 2 (Tuesday): Queries 21-50
**Morning (4 hours)**
- [ ] Implement Q11-Q30 in DataFrame API
- [ ] Focus on complex aggregations and window functions
- [ ] Handle ROLLUP/CUBE operations if present

**Afternoon (4 hours)**
- [ ] Implement Q31-Q50 in DataFrame API
- [ ] Address any compatibility issues found
- [ ] Update FunctionRegistry mappings as needed

**Deliverables**:
- 50% of queries implemented
- Compatibility matrix documenting issues
- Function mapping updates

### Day 3 (Wednesday): Queries 51-80
**Morning (4 hours)**
- [ ] Implement Q51-Q65 in DataFrame API
- [ ] Focus on complex join patterns
- [ ] Handle correlated subqueries

**Afternoon (4 hours)**
- [ ] Implement Q66-Q80 in DataFrame API
- [ ] Performance profiling of slow queries
- [ ] Optimization opportunities documented

**Deliverables**:
- 80% of queries implemented
- Performance benchmark report
- Optimization recommendations

### Day 4 (Thursday): Queries 81-100 & Bug Fixes
**Morning (4 hours)**
- [ ] Implement Q81-Q100 in DataFrame API
- [ ] Complete any remaining complex patterns
- [ ] Final push for 100% implementation

**Afternoon (4 hours)**
- [ ] Fix all discovered discrepancies
- [ ] Re-run failed queries after fixes
- [ ] Implement workarounds for any blockers

**Deliverables**:
- 100% of queries implemented
- Bug fix report
- Workaround documentation

### Day 5 (Friday): Validation & Documentation
**Morning (4 hours)**
- [ ] Final validation run of all 100 queries
- [ ] Generate comprehensive test report
- [ ] Performance comparison Spark vs ThunderDuck

**Afternoon (4 hours)**
- [ ] Update DataFrame API Reference guide
- [ ] Create TPC-DS compatibility matrix
- [ ] Write Week 17 completion report

**Deliverables**:
- Final test results
- Week 17 completion report
- Updated documentation

## Technical Implementation Details

### 1. Differential Testing Framework

```python
class TpcDsDifferentialTest:
    def __init__(self):
        self.spark_session = self.create_spark_session()
        self.thunderduck_session = self.create_thunderduck_session()

    def run_query(self, query_num: int):
        # 1. Run SQL version on Spark for reference
        spark_sql_result = self.run_spark_sql(query_num)

        # 2. Run DataFrame version on Spark
        spark_df_result = self.run_spark_dataframe(query_num)

        # 3. Run DataFrame version on ThunderDuck
        thunderduck_df_result = self.run_thunderduck_dataframe(query_num)

        # 4. Validate all three match
        self.validate_results(
            spark_sql_result,
            spark_df_result,
            thunderduck_df_result
        )
```

### 2. Query Implementation Pattern

```python
def tpcds_q1_dataframe(spark):
    """TPC-DS Query 1 using DataFrame API"""
    store_returns = spark.table("store_returns")
    date_dim = spark.table("date_dim")
    store = spark.table("store")
    customer = spark.table("customer")

    return store_returns.join(
        date_dim,
        store_returns.sr_returned_date_sk == date_dim.d_date_sk
    ).filter(
        date_dim.d_year == 2000
    ).join(
        store,
        store_returns.sr_store_sk == store.s_store_sk
    ).join(
        customer,
        store_returns.sr_customer_sk == customer.c_customer_sk
    ).groupBy(
        "c_customer_id"
    ).agg(
        sum("sr_return_amt").alias("total_return")
    ).filter(
        col("total_return") > avg("total_return").over(Window.partitionBy()) * 1.2
    ).orderBy("c_customer_id")
```

### 3. Result Validation

```python
def validate_results(self, expected, actual, query_num):
    """Comprehensive result validation"""
    # 1. Row count validation
    assert expected.count() == actual.count(), \
        f"Q{query_num}: Row count mismatch"

    # 2. Schema validation
    assert expected.schema == actual.schema, \
        f"Q{query_num}: Schema mismatch"

    # 3. Data validation (order-independent)
    expected_sorted = expected.orderBy(*expected.columns).collect()
    actual_sorted = actual.orderBy(*actual.columns).collect()

    for i, (exp_row, act_row) in enumerate(zip(expected_sorted, actual_sorted)):
        for col in expected.columns:
            if isinstance(exp_row[col], float):
                # Floating point comparison with tolerance
                assert abs(exp_row[col] - act_row[col]) < 1e-6, \
                    f"Q{query_num} Row {i} Col {col}: Value mismatch"
            else:
                assert exp_row[col] == act_row[col], \
                    f"Q{query_num} Row {i} Col {col}: Value mismatch"
```

## Expected Challenges & Mitigations

### 1. Complex SQL Patterns
**Challenge**: Some TPC-DS queries use advanced SQL features
**Mitigation**:
- Implement using multiple DataFrame operations
- Create helper functions for common patterns
- Document limitations and workarounds

### 2. Performance Differences
**Challenge**: ThunderDuck may be slower/faster on certain operations
**Mitigation**:
- Focus on correctness first
- Document performance characteristics
- Identify optimization opportunities for Week 18

### 3. Function Name Mismatches
**Challenge**: Ongoing function name differences
**Mitigation**:
- Continuously update FunctionRegistry
- Create comprehensive mapping documentation
- Consider implementing aliases

### 4. Result Ordering
**Challenge**: Non-deterministic ordering for ties
**Mitigation**:
- Sort by all columns before comparison
- Use set-based comparison where appropriate
- Document expected variations

## Risk Management

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Incomplete DataFrame API | Low | High | Implement missing operations |
| Performance timeouts | Medium | Medium | Increase timeouts, optimize queries |
| Data type mismatches | Medium | Low | Add type coercion layer |
| Memory issues | Low | Medium | Adjust JVM settings |

## Dependencies

### Technical
- PySpark 3.5.0 for DataFrame API
- Native Spark cluster for reference execution
- ThunderDuck server running
- TPC-DS test data loaded

### Resources
- 2 engineers for implementation
- Access to Spark cluster
- Sufficient memory for large queries
- CI/CD pipeline for automated testing

## Deliverables

### Code Artifacts
1. `/workspace/tests/integration/tpcds_dataframe/` - All 100 queries
2. `/workspace/tests/integration/differential_test.py` - Test framework
3. `/workspace/tests/integration/validation_utils.py` - Comparison utilities

### Documentation
1. `WEEK17_COMPLETION_REPORT.md` - Final results and findings
2. `TPCDS_DATAFRAME_COMPATIBILITY.md` - Compatibility matrix
3. Updated `DataFrame_API_Reference.md` - With TPC-DS learnings

### Test Results
1. Full test execution logs
2. Performance comparison spreadsheet
3. Discrepancy analysis report

## Success Metrics

### Quantitative
- **Query Coverage**: 100/100 queries implemented
- **Validation Pass Rate**: Target 95%+
- **Performance**: Within 2x of native Spark
- **Test Execution Time**: < 2 hours for full suite

### Qualitative
- Clear understanding of remaining gaps
- Documented workarounds for limitations
- Confidence in production readiness
- Team knowledge of DataFrame API edge cases

## Next Steps (Week 18 Preview)

Based on Week 17 findings:
1. Performance optimization of slow queries
2. Implementation of missing DataFrame operations
3. Production deployment preparation
4. Customer migration support tools

## Conclusion

Week 17 will definitively validate ThunderDuck's DataFrame API compatibility through comprehensive TPC-DS testing. By implementing all 100 queries and comparing results against native Spark, we'll ensure ThunderDuck is truly a drop-in replacement for Spark DataFrame workloads.

The differential testing approach provides strong confidence in correctness, while the systematic implementation of complex queries will uncover any remaining edge cases. This validation is crucial for production readiness and customer confidence.

---

**Start Date**: Week 17, Day 1
**End Date**: Week 17, Day 5
**Status**: PLANNED
**Owner**: ThunderDuck Team
**Last Updated**: October 30, 2025