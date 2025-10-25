# Week 13 Implementation Plan: DataFrame SQL Generation Fix + TPC-H Integration Tests

**Duration**: TBD (estimated 18-26 hours)
**Goal**: Fix DataFrame API SQL generation and expand TPC-H query coverage with real PySpark client tests

---

## Executive Summary

Week 13 focuses on completing the DataFrame API implementation by fixing the SQL generation buffer corruption issue, then expanding test coverage with comprehensive TPC-H integration tests using a real PySpark Spark Connect client.

**Primary Objective**: Make DataFrame API operations work reliably for complex query patterns.

**Secondary Objective**: Build comprehensive TPC-H integration test suite (Q1-Q22) with PySpark client.

---

## Prerequisites (from Week 12) ✅

- ✅ Plan deserialization infrastructure complete (~800 LOC)
- ✅ AnalyzePlan with schema extraction working
- ✅ SQL queries execute perfectly via Spark Connect
- ✅ TPC-H data generated (SF=0.01, all 8 tables)
- ✅ TPC-H Q1 validated via SQL (4 rows)
- ✅ Protocol-compliant implementation
- ✅ Server runs stably with proper JVM args

---

## Phase 1: DataFrame SQL Generation Fix (Priority 1)

### Day 1-2: SQL Generator Refactoring (6-8 hours)

**Problem**: Buffer corruption when mixing visitor pattern and toSQL() method calls

**Root Cause Analysis**:
```java
// Current issue:
visitSort() calls:     sql.append("SELECT * FROM (");
                       visit(child());  // This calls visitAggregate
visitAggregate() calls: sql.append(plan.toSQL(this));
plan.toSQL() calls:    generator.generate(child());  // Clears buffer!
Result: Missing "SELECT * FROM (" in final SQL
```

**Solution Options**:

**Option A: Pure Visitor Pattern** (Recommended)
- Refactor ALL nodes to use visitor pattern consistently
- No toSQL() methods call generator.generate()
- visitAggregate builds SQL directly in buffer
- visitSort, visitLimit, etc. all build in buffer
- Clean, single pattern throughout

**Option B: Pure toSQL() Pattern**
- Remove all visitor methods
- Everything uses toSQL(generator)
- toSQL methods build standalone SQL
- Main generate() just calls root.toSQL(this)

**Option C: Hybrid with Clear Contract**
- Visitor for simple nodes (Project, Filter, Sort, Limit)
- toSQL() for complex nodes (Aggregate, Join) but they DON'T call generate()
- Clear documentation of which pattern each node uses

**Recommended**: Option A (Pure Visitor Pattern)

**Tasks**:
1. **Refactor Aggregate.toSQL()** (2 hours)
   - Move logic to visitAggregate()
   - Build SQL directly in buffer
   - Remove generate(child()) call

2. **Refactor Join.toSQL()** (2 hours)
   - Move logic to visitJoin()
   - Build SQL directly in buffer

3. **Refactor Union/Intersect/Except** (1 hour)
   - Convert to visitor pattern
   - Build SQL in buffer

4. **Test All Operations** (2 hours)
   - Test filter, project, aggregate, sort, limit
   - Test joins
   - Test set operations
   - Verify no buffer corruption

5. **Update Documentation** (1 hour)
   - Document SQL generation architecture
   - Explain visitor pattern
   - Add developer guide

**Deliverables**:
- ✅ Consistent SQL generation pattern
- ✅ No buffer corruption
- ✅ All DataFrame operations work
- ✅ Clean, maintainable code

**Success Criteria**:
- ✅ TPC-H Q1 via DataFrame API works
- ✅ Complex DataFrame chains work (filter + groupBy + agg + orderBy)
- ✅ No SQL syntax errors
- ✅ All existing tests still pass

---

## Phase 2: TPC-H Integration Test Suite (Priority 2)

### Day 3-4: PySpark Integration Tests (8-12 hours)

**Goal**: Build comprehensive integration test suite using real PySpark Spark Connect client

**Approach**: Python-based integration tests that:
1. Start thunderduck Spark Connect server
2. Connect via PySpark client
3. Execute TPC-H queries via DataFrame API and SQL
4. Validate results against expected output
5. Measure performance

**Test Framework Structure**:
```
tests/integration/
├── test_spark_connect_integration.py
├── test_tpch_queries.py
├── conftest.py (pytest fixtures)
├── expected_results/
│   ├── q1_expected.parquet
│   ├── q3_expected.parquet
│   └── ...
└── utils/
    ├── server_manager.py (start/stop server)
    ├── result_validator.py
    └── performance_tracker.py
```

**Tasks**:

**Task 1: Test Framework Setup** (2 hours)
- Install pytest, pytest-timeout
- Create conftest.py with server startup/shutdown fixtures
- Create ServerManager class to manage server lifecycle
- Add result validation utilities

**Task 2: TPC-H Query Tests** (6-8 hours)

**Tier 1 Queries** (Essential - 4 hours):
- ✅ Q1 (Scan + Agg) - Already validated
- Q3 (Join + Agg) - Multi-table join with aggregation
- Q6 (Selective Scan) - Simple filter and aggregate
- Q13 (Complex) - Outer join with aggregation

**Tier 2 Queries** (Advanced - 4 hours):
- Q5 (Multi-way Join)
- Q10 (Join + Agg + Top-N)
- Q12 (Join + Case When)
- Q18 (Join + Subquery)

**Tier 3 Queries** (Full Suite - defer to later):
- Q2, Q4, Q7-Q22 (incremental addition)

**Test Structure** (per query):
```python
@pytest.mark.tpch
@pytest.mark.timeout(60)
def test_tpch_q1_dataframe_api(spark_connect):
    """Test TPC-H Q1 via DataFrame API"""
    # Given: Load lineitem table
    df = spark_connect.read.parquet("data/tpch_sf001/lineitem.parquet")

    # When: Execute Q1 via DataFrame API
    result = (df
        .filter(col("l_shipdate") <= "1998-12-01")
        .groupBy("l_returnflag", "l_linestatus")
        .agg(
            _sum("l_quantity").alias("sum_qty"),
            _sum("l_extendedprice").alias("sum_base_price"),
            # ... all aggregates
        )
        .orderBy("l_returnflag", "l_linestatus")
    )

    # Then: Validate results
    rows = result.collect()
    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"
    validate_q1_results(rows)  # Check aggregate values

@pytest.mark.tpch
@pytest.mark.timeout(60)
def test_tpch_q1_sql(spark_connect):
    """Test TPC-H Q1 via SQL (reference implementation)"""
    sql = load_tpch_query(1)  # Load from benchmarks/tpch_queries/q1.sql
    result = spark_connect.sql(sql)
    rows = result.collect()
    assert len(rows) == 4
    validate_q1_results(rows)
```

**Task 3: Result Validation** (2 hours)
- Load expected results (pre-computed with Spark)
- Compare row counts
- Compare schemas
- Compare aggregate values (with epsilon for floats)
- Validate sort order

**Deliverables**:
- ✅ pytest-based integration test suite
- ✅ Server lifecycle management
- ✅ 8-12 TPC-H queries tested
- ✅ Result validation framework
- ✅ Both DataFrame API and SQL tests

**Success Criteria**:
- ✅ All Tier 1 queries pass (Q1, Q3, Q6, Q13)
- ✅ Tests run automatically with pytest
- ✅ Clear failure messages
- ✅ Performance data collected

---

## Phase 3: Extended Testing & Refinement (Priority 3)

### Day 5: Error Handling & Edge Cases (4-6 hours)

**Tasks**:
1. **Error Handling Tests** (2 hours)
   - Test invalid queries
   - Test unsupported operations
   - Test malformed plans
   - Verify error messages are clear

2. **Edge Case Tests** (2 hours)
   - Empty DataFrames
   - NULL handling
   - Large result sets
   - Complex nested expressions

3. **Performance Regression Tests** (2 hours)
   - Benchmark all TPC-H queries
   - Ensure < 5s for SF=0.01
   - Track performance over time
   - Identify slow queries

**Deliverables**:
- ✅ Comprehensive error handling tests
- ✅ Edge case coverage
- ✅ Performance baseline established

---

## Week 13 Schedule

### Day 1: SQL Generation Fix (Monday, 6 hours)
- 9:00-12:00: Refactor to pure visitor pattern
- 1:00-4:00: Test all DataFrame operations
- 4:00-5:00: Validate TPC-H Q1 via DataFrame API

### Day 2: SQL Generation Polish (Tuesday, 4 hours)
- 9:00-12:00: Fix remaining edge cases
- 1:00-2:00: Test complex DataFrame chains
- 2:00-3:00: Documentation

### Day 3: TPC-H Integration Tests (Wednesday, 6 hours)
- 9:00-12:00: Setup pytest framework, server manager
- 1:00-4:00: Implement Q1, Q3, Q6 tests
- 4:00-5:00: Run full suite, validate

### Day 4: Extended TPC-H Coverage (Thursday, 6 hours)
- 9:00-12:00: Implement Q13, Q5, Q10 tests
- 1:00-4:00: Result validation framework
- 4:00-5:00: Performance tracking

### Day 5: Testing & Documentation (Friday, 4-6 hours)
- 9:00-12:00: Error handling tests
- 1:00-3:00: Edge case tests
- 3:00-4:00: Documentation updates
- 4:00-5:00: Week 13 completion report

**Total**: 26-28 hours

---

## Deliverables

### Code Deliverables
1. **SQLGenerator.java** - Refactored to pure visitor pattern
2. **tests/integration/test_tpch_queries.py** - PySpark integration tests (NEW)
3. **tests/integration/conftest.py** - pytest fixtures (NEW)
4. **tests/integration/utils/** - Test utilities (NEW)

### Test Coverage
- **DataFrame Operations**: 100% functional (all operations work)
- **TPC-H Tier 1**: Q1, Q3, Q6, Q13 (via DataFrame API and SQL)
- **TPC-H Tier 2**: Q5, Q10, Q12, Q18
- **Integration Tests**: 20-30 tests total
- **Performance**: Baseline established for all queries

### Documentation
- SQL generation architecture guide
- TPC-H integration test guide
- Week 13 completion report
- Performance benchmarking results

---

## Success Criteria

### Functional Requirements
- ✅ All DataFrame operations work reliably
- ✅ TPC-H Q1 via DataFrame API (not just SQL)
- ✅ No SQL generation errors
- ✅ Schema extraction for all query types

### Testing Requirements
- ✅ 20-30 integration tests with PySpark client
- ✅ 8-12 TPC-H queries validated
- ✅ Automated test suite with pytest
- ✅ Clear pass/fail reporting

### Performance Requirements
- ✅ All TPC-H queries < 5s (SF=0.01)
- ⏳ Performance benchmarking (baseline)
- ⏳ No significant regressions

### Quality Requirements
- ✅ Test pass rate ≥ 99.8%
- ✅ All integration tests pass
- ✅ Clean, maintainable code
- ✅ Comprehensive documentation

---

## Risk Mitigation

### Risk 1: SQL Generation Refactoring Breaks Existing Tests
**Mitigation**: Run full test suite after each change, use TDD approach

### Risk 2: PySpark Client Compatibility Issues
**Mitigation**: Test with multiple PySpark versions, follow protocol spec strictly

### Risk 3: TPC-H Query Complexity
**Mitigation**: Start with simple queries (Q1, Q6), incrementally add complexity

---

## Technical Notes

### SQL Generation Refactoring Strategy

**Current Pattern** (Problematic):
```java
// Visitor calls toSQL which calls generate() - buffer corruption
visitSort(Sort plan) {
    sql.append(plan.toSQL(this));  // toSQL calls generate()
}
```

**Target Pattern** (Clean):
```java
// Visitor builds SQL directly in buffer
visitSort(Sort plan) {
    sql.append("SELECT * FROM (");
    visit(plan.child());  // Recursive visit
    sql.append(") AS subquery ORDER BY ");
    // ... build ORDER BY clause
}
```

**Benefits**:
- No buffer corruption
- Clear execution flow
- Easier to debug
- Consistent pattern

---

## TPC-H Query Priorities

### Tier 1 (Essential - Week 13)
| Query | Operations | Complexity | Priority |
|-------|------------|------------|----------|
| Q1 | Scan, Filter, Agg, Sort | Low | ✅ Done |
| Q3 | Join, Filter, Agg, Sort, Limit | Medium | HIGH |
| Q6 | Scan, Filter, Agg | Low | HIGH |
| Q13 | Outer Join, Agg, Sort | Medium | HIGH |

### Tier 2 (Advanced - Week 13 if time)
| Query | Operations | Complexity | Priority |
|-------|------------|------------|----------|
| Q5 | Multi-join, Agg | High | MEDIUM |
| Q10 | Join, Agg, Sort, Limit | Medium | MEDIUM |
| Q12 | Join, Case When, Agg | Medium | MEDIUM |
| Q18 | Join, Subquery, Agg | High | MEDIUM |

### Tier 3 (Full Coverage - Week 14+)
- Q2, Q4, Q7, Q8, Q9, Q11, Q14-Q22
- Deferred to later weeks

---

## Dependencies

**From Core Module**:
- LogicalPlan hierarchy (existing)
- SQLGenerator (to be refactored)
- QueryExecutor (existing)

**From Connect-Server**:
- PlanConverter (existing)
- RelationConverter (existing)
- ExpressionConverter (existing)
- SparkConnectServiceImpl (existing)

**External**:
- pytest >= 7.0
- PySpark 3.5.3
- pandas (for result comparison)

---

## Testing Strategy

### Unit Tests (Existing)
- Continue running core test suite (99.8% pass rate)
- No changes needed

### Integration Tests (NEW)
- Python-based tests with pytest
- Real PySpark Spark Connect client
- Actual server startup/shutdown
- Real query execution
- Result validation

### Performance Tests
- Measure all TPC-H queries
- Track execution time
- Identify optimization opportunities

---

**Plan Version**: 1.0
**Created**: 2025-10-25
**Status**: Ready for Execution
