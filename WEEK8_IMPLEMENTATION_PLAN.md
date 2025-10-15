# Week 8: Comprehensive Differential Test Coverage (200+ Tests)

**Project**: catalyst2sql
**Week**: 8
**Status**: 📋 IN PROGRESS
**Goal**: Expand differential test coverage from 50 to 200+ tests

---

## Objective

Expand the differential testing framework to validate Spark parity across **all advanced SQL features** by adding 150+ new tests to the existing 50 tests, achieving comprehensive coverage of:
- Subqueries (scalar, correlated, IN/EXISTS)
- Window functions (ranking, analytics, framing)
- Set operations (UNION, INTERSECT, EXCEPT)
- Advanced aggregates (STDDEV, PERCENTILE, etc.)
- Complex types (ARRAY, STRUCT, MAP)
- Common Table Expressions (CTEs)
- Additional SQL features

**Success Criteria**: 200+ tests with 100% pass rate (all tests validating Spark parity)

---

## Scope

### ✅ In Scope
- Implementing 150+ new differential tests
- Validating Spark parity for advanced SQL features
- Extending data generators for complex test scenarios
- Documenting test coverage and results

### ❌ Out of Scope (Deferred to Week 9)
- Performance benchmarking and optimization
- CI/CD pipeline setup
- Production deployment configuration

---

## Phase 1: Subquery Tests (30 tests)

### Test Categories

**1.1 Scalar Subqueries (6 tests)**
- Scalar subquery in SELECT clause
- Scalar subquery with aggregation
- Multiple scalar subqueries in SELECT
- Scalar subquery in WHERE clause
- Scalar subquery with CASE WHEN
- Nested scalar subqueries

**1.2 Correlated Subqueries (6 tests)**
- Correlated subquery in WHERE
- Correlated subquery with aggregation
- Correlated EXISTS subquery
- Correlated NOT EXISTS subquery
- Multiple correlated subqueries
- Complex correlation with multiple columns

**1.3 IN/NOT IN Subqueries (6 tests)**
- Simple IN subquery
- NOT IN subquery
- IN subquery with multiple columns
- IN subquery with aggregation
- IN with NULL handling
- Complex IN with JOINs

**1.4 EXISTS/NOT EXISTS (6 tests)**
- Simple EXISTS
- NOT EXISTS
- EXISTS with aggregation
- EXISTS with multiple conditions
- Nested EXISTS
- EXISTS vs IN comparison

**1.5 Complex Subquery Scenarios (6 tests)**
- Subquery in FROM clause (derived tables)
- Multiple nesting levels (3+ deep)
- Subqueries with JOINs
- Subqueries with window functions
- Subqueries with set operations
- Mixed subquery types in single query

**Implementation Files**:
- `SubqueryTests.java` - main test class
- Update `SyntheticDataGenerator.java` - add methods for multi-table scenarios

---

## Phase 2: Window Function Tests (30 tests)

### Test Categories

**2.1 Ranking Functions (6 tests)**
- ROW_NUMBER() basic
- RANK() with ties
- DENSE_RANK() with ties
- NTILE(n) for quartiles
- Multiple ranking functions in single query
- Ranking with NULL values

**2.2 Analytic Functions (6 tests)**
- LEAD() with default values
- LAG() with default values
- FIRST_VALUE()
- LAST_VALUE()
- NTH_VALUE()
- Mixed analytic functions

**2.3 Window Partitioning (6 tests)**
- PARTITION BY single column
- PARTITION BY multiple columns
- Window without partitioning (global)
- PARTITION BY with ORDER BY
- Different partitions in same query
- PARTITION BY with complex expressions

**2.4 Window Ordering (6 tests)**
- ORDER BY single column ASC
- ORDER BY single column DESC
- ORDER BY multiple columns
- ORDER BY with NULLS FIRST/LAST
- ORDER BY with expressions
- Window without ORDER BY

**2.5 Frame Specifications (6 tests)**
- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
- ROWS BETWEEN n PRECEDING AND CURRENT ROW
- ROWS BETWEEN CURRENT ROW AND n FOLLOWING
- ROWS BETWEEN n PRECEDING AND n FOLLOWING
- RANGE BETWEEN (logical range)
- Mixed frame types in single query

**Implementation Files**:
- `WindowFunctionTests.java` - main test class
- Update `SyntheticDataGenerator.java` - add time-series and ranking datasets

---

## Phase 3: Set Operation Tests (20 tests)

### Test Categories

**3.1 UNION Tests (5 tests)**
- UNION (implicit DISTINCT)
- UNION with different column orders
- UNION with type coercion
- UNION with NULL values
- Multiple UNIONs (3+ tables)

**3.2 UNION ALL Tests (5 tests)**
- UNION ALL (preserves duplicates)
- UNION ALL with large datasets
- UNION ALL with NULL handling
- UNION ALL with ORDER BY
- UNION ALL vs UNION performance

**3.3 INTERSECT Tests (5 tests)**
- Simple INTERSECT
- INTERSECT with multiple columns
- INTERSECT with NULL values
- Multiple INTERSECTs
- INTERSECT with complex expressions

**3.4 EXCEPT Tests (5 tests)**
- Simple EXCEPT
- EXCEPT with multiple columns
- EXCEPT with NULL values
- Multiple EXCEPTs
- EXCEPT with ORDER BY

**Implementation Files**:
- `SetOperationTests.java` - main test class
- Reuse existing data generators

---

## Phase 4: Advanced Aggregate Tests (20 tests)

### Test Categories

**4.1 Statistical Aggregates (6 tests)**
- STDDEV() / STDDEV_SAMP()
- STDDEV_POP()
- VARIANCE() / VAR_SAMP()
- VAR_POP()
- COVAR_SAMP()
- CORR() (correlation)

**4.2 Percentile Aggregates (6 tests)**
- PERCENTILE_CONT(0.5) (median)
- PERCENTILE_DISC(0.5)
- PERCENTILE_CONT() with multiple percentiles
- APPROX_PERCENTILE()
- MEDIAN() if supported
- Percentiles with GROUP BY

**4.3 Collection Aggregates (4 tests)**
- COLLECT_LIST()
- COLLECT_SET()
- Array aggregation with ORDER BY
- Collection aggregates with GROUP BY

**4.4 Approximate Aggregates (4 tests)**
- APPROX_COUNT_DISTINCT()
- Approximate vs exact COUNT DISTINCT
- APPROX_PERCENTILE() accuracy
- Large dataset approximation

**Implementation Files**:
- `AdvancedAggregateTests.java` - main test class
- Update `SyntheticDataGenerator.java` - add statistical distribution datasets

---

## Phase 5: Complex Type Tests (20 tests)

### Test Categories

**5.1 ARRAY Operations (7 tests)**
- ARRAY creation with literals
- ARRAY indexing (element access)
- ARRAY_CONTAINS()
- ARRAY_SIZE() / CARDINALITY()
- EXPLODE() / UNNEST()
- Array with NULL elements
- Nested arrays

**5.2 STRUCT Operations (7 tests)**
- STRUCT creation
- STRUCT field access (dot notation)
- STRUCT with NULL fields
- Nested structs
- STRUCT in SELECT
- STRUCT in WHERE
- STRUCT with JOINs

**5.3 MAP Operations (6 tests)**
- MAP creation
- MAP key access
- MAP_KEYS()
- MAP_VALUES()
- MAP with NULL values
- Nested maps

**Implementation Files**:
- `ComplexTypeTests.java` - main test class
- Update `SyntheticDataGenerator.java` - add complex type generation

---

## Phase 6: CTE (WITH Clause) Tests (15 tests)

### Test Categories

**6.1 Simple CTEs (5 tests)**
- Single CTE
- CTE with aggregation
- CTE with JOIN
- CTE with WHERE
- CTE with ORDER BY

**6.2 Multiple CTEs (5 tests)**
- Two CTEs in sequence
- Three+ CTEs in sequence
- CTEs referencing other CTEs
- CTEs with different aggregations
- CTEs with mixed operations

**6.3 Complex CTE Scenarios (5 tests)**
- CTE with subquery
- CTE with window functions
- CTE with set operations
- CTE used multiple times in main query
- Recursive CTE (if supported by both engines)

**Implementation Files**:
- `CTETests.java` - main test class
- Reuse existing data generators

---

## Phase 7: Additional Coverage (15 tests)

### Test Categories

**7.1 String Functions (5 tests)**
- SUBSTRING() / SUBSTR()
- CONCAT() / CONCAT_WS()
- UPPER() / LOWER()
- LENGTH() / CHAR_LENGTH()
- TRIM() / LTRIM() / RTRIM()

**7.2 Date/Timestamp Functions (5 tests)**
- CURRENT_DATE / CURRENT_TIMESTAMP
- DATE_ADD() / DATE_SUB()
- DATEDIFF()
- EXTRACT() / DATE_PART()
- TO_DATE() / TO_TIMESTAMP()

**7.3 Complex Expression Nesting (5 tests)**
- Deeply nested expressions (5+ levels)
- Mixed function calls
- Complex CASE WHEN nesting
- Expression with all data types
- NULL handling in complex expressions

**Implementation Files**:
- `AdditionalCoverageTests.java` - main test class
- Update `SyntheticDataGenerator.java` - add date/timestamp datasets

---

## Implementation Strategy

### Approach

1. **Use Task Agent**: Spawn a single Task Agent to implement all 150 tests in parallel
2. **Incremental Testing**: Test each category as it's implemented
3. **Reuse Framework**: Leverage existing DifferentialTestHarness, validators, and comparators
4. **Data Generation**: Extend SyntheticDataGenerator for new test scenarios
5. **Consistent Structure**: Follow existing test patterns from Week 7

### Task Agent Instructions

The Task Agent should:
1. Create 7 new test class files (one per phase)
2. Implement all 150 tests following the specifications above
3. Extend SyntheticDataGenerator with new data generation methods
4. Run all tests and verify 100% pass rate
5. Generate execution report with test results

### Quality Standards

Each test must:
- Use `@Test` and `@DisplayName` annotations
- Generate synthetic test data via SyntheticDataGenerator
- Write data to Parquet for engine-neutral comparison
- Use `ORDER BY` for deterministic row ordering
- Execute on both Spark and DuckDB
- Use `executeAndCompare()` to validate results
- Assert zero divergences with `assertThat(result.hasDivergences()).isFalse()`

---

## Expected Outcomes

### Test Coverage Summary

| Category | Tests | Total Tests |
|----------|-------|-------------|
| **Phase 1: Subqueries** | 30 | 80 (50 + 30) |
| **Phase 2: Window Functions** | 30 | 110 (80 + 30) |
| **Phase 3: Set Operations** | 20 | 130 (110 + 20) |
| **Phase 4: Advanced Aggregates** | 20 | 150 (130 + 20) |
| **Phase 5: Complex Types** | 20 | 170 (150 + 20) |
| **Phase 6: CTEs** | 15 | 185 (170 + 15) |
| **Phase 7: Additional Coverage** | 15 | 200 (185 + 15) |
| **TOTAL** | **150 new** | **200+ tests** |

### Success Metrics

- **Test Count**: 200+ differential tests
- **Pass Rate**: 100% (all tests passing)
- **Coverage**: All advanced SQL features validated
- **Spark Parity**: Complete validation for production readiness
- **Documentation**: Comprehensive test report

### Deliverables

1. **7 New Test Classes** (~2,500 LOC):
   - SubqueryTests.java
   - WindowFunctionTests.java
   - SetOperationTests.java
   - AdvancedAggregateTests.java
   - ComplexTypeTests.java
   - CTETests.java
   - AdditionalCoverageTests.java

2. **Extended Data Generator**:
   - Updated SyntheticDataGenerator.java with new methods

3. **Test Execution Report**:
   - WEEK8_COMPLETION_REPORT.md with full results

4. **Updated Documentation**:
   - IMPLEMENTATION_PLAN.md marked Week 8 complete
   - Git commits with all changes

---

## Timeline

**Estimated Duration**: 6-8 hours for Task Agent implementation

### Execution Steps

1. **Create Test Classes** (2 hours)
   - Create 7 new test class files
   - Set up test structure and imports

2. **Implement Tests** (3 hours)
   - Write all 150 test methods
   - Follow quality standards
   - Reuse existing patterns

3. **Extend Data Generators** (1 hour)
   - Add new data generation methods
   - Support complex scenarios

4. **Run Tests** (1 hour)
   - Execute all 200+ tests
   - Debug any failures
   - Verify 100% pass rate

5. **Generate Report** (1 hour)
   - Document test results
   - Create completion report
   - Update project status

---

## Risk Mitigation

### Potential Issues

1. **Spark/DuckDB Feature Gaps**: Some advanced features may not be supported by DuckDB
   - **Mitigation**: Mark unsupported features as `.disabled` tests, document in report

2. **Complex Type Support**: ARRAY/STRUCT/MAP may have limited DuckDB support
   - **Mitigation**: Test what's available, document limitations

3. **Recursive CTEs**: May not be supported by both engines
   - **Mitigation**: Skip if unsupported, focus on non-recursive CTEs

4. **Test Execution Time**: 200+ tests may take significant time
   - **Mitigation**: Optimize data generation, use small datasets for tests

---

## Post-Implementation

### Verification Checklist

- [ ] All 7 test classes created
- [ ] All 150+ new tests implemented
- [ ] SyntheticDataGenerator extended
- [ ] All tests passing (200+ tests, 100% pass rate)
- [ ] Test execution report generated
- [ ] IMPLEMENTATION_PLAN.md updated
- [ ] All changes committed to git

### Next Steps (Week 9)

After Week 8 completion:
- Performance benchmarking and optimization
- CI/CD pipeline setup
- Production deployment guide
- Final documentation and hardening

---

**Status**: 📋 Ready for Execution
**Next Action**: Spawn Task Agent to implement all 150+ tests
**Target**: 200+ tests with 100% pass rate validating complete Spark parity
