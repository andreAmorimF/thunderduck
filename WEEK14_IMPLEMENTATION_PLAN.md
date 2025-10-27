# Week 14 Implementation Plan: 100% TPC-H Coverage (22/22 Queries)

**Duration**: 5 days (estimated 20-30 hours)
**Goal**: Achieve 100% TPC-H query coverage and validate Spark parity for all 22 standard queries
**Status**: ACTIVE

---

## Executive Summary

Week 14 focuses on expanding TPC-H test coverage from 8/22 (36%) to 22/22 (100%) queries. With the DATE bug and protobuf build issues now resolved, we have a solid foundation to validate all remaining TPC-H queries against Spark 3.5.3.

**Starting Point**: 8/22 queries validated (100% passing)
**Target**: 22/22 queries validated
**Infrastructure**: Ready (server, testing framework, TPC-H data)

---

## Prerequisites (from Week 13) ✅

- ✅ Spark Connect server running reliably (122MB JAR with all proto classes)
- ✅ DATE marshalling working (writer.start() fix)
- ✅ Protobuf build stable (compile scope fix)
- ✅ TPC-H SF=0.01 data (8 tables, 86,705 rows)
- ✅ Correctness validation framework (value-by-value comparison)
- ✅ 8 queries validated: Q1, Q3, Q5, Q6, Q10, Q12, Q13, Q18

---

## Current Status

### Validated Queries (8/22 = 36%)

**Tier 1** (Simple scans + aggregates):
- ✅ Q1: Pricing Summary Report
- ✅ Q6: Forecasting Revenue Change

**Tier 2** (Joins + moderate complexity):
- ✅ Q3: Shipping Priority
- ✅ Q5: Local Supplier Volume
- ✅ Q10: Returned Item Reporting
- ✅ Q12: Shipping Modes and Order Priority
- ✅ Q13: Customer Distribution
- ✅ Q18: Large Volume Customer

### Remaining Queries (14/22 = 64%)

**Need to obtain and validate**:
- Q2, Q4, Q7, Q8, Q9, Q11, Q14, Q15, Q16, Q17, Q19, Q20, Q21, Q22

---

## Week 14 Plan

### Day 1: Obtain Remaining TPC-H Queries (4-6 hours)

**Task 1.1**: Download standard TPC-H queries (2 hours)
- Source: Official TPC-H benchmark or compatible implementation
- Format: SQL files compatible with Spark SQL
- Validation: Verify queries are for TPC-H specification
- Store: `/workspace/benchmarks/tpch_queries/q2.sql` through `q22.sql`

**Task 1.2**: Review query complexity (1 hour)
- Categorize by complexity (simple, moderate, complex)
- Identify SQL features required (subqueries, EXISTS, NOT IN, etc.)
- Flag potential compatibility issues

**Task 1.3**: Initial smoke test (1-2 hours)
- Run all 14 queries through Thunderduck
- Check for execution errors
- Identify missing SQL features
- Document any failures

**Success Criteria**:
- ✅ All 22 TPC-H query files available
- ✅ Complexity categorization complete
- ✅ Initial execution status known

---

### Day 2-3: Generate Spark Reference Data (8-12 hours)

**Task 2.1**: Create reference data generation script (2 hours)
- Extend existing script from Week 13
- Loop through Q2, Q4, Q7-Q22
- Save JSON reference files

**Task 2.2**: Generate all 14 reference files (4-6 hours)
- Run each query against Spark local mode
- Save results to `/workspace/tests/integration/expected_results/qN_spark_reference.json`
- Validate JSON format
- Verify row counts reasonable

**Task 2.3**: Create validation tests (2-4 hours)
- Extend `test_value_correctness.py`
- Add test methods for all 14 queries
- Use same validation framework as Week 13

**Success Criteria**:
- ✅ 14 new Spark reference JSON files created
- ✅ 14 new test methods added
- ✅ Test infrastructure ready

---

### Day 4: Run Validation & Fix Issues (6-8 hours)

**Task 4.1**: Run all 22 correctness tests (1 hour)
- Execute: `pytest tests/integration/test_value_correctness.py -v`
- Document pass/fail for each query
- Categorize failures by type

**Task 4.2**: Triage failures (1 hour)
- Group by root cause:
  - Missing SQL features (NOT IN, EXISTS, etc.)
  - Type mismatches
  - Calculation errors
  - Protocol issues
- Prioritize fixes

**Task 4.3**: Fix high-priority issues (4-6 hours)
- Implement missing SQL features as needed
- Fix any type conversion issues
- Address calculation errors
- Re-test after each fix

**Success Criteria**:
- ✅ All 22 queries tested
- ✅ Pass/fail status documented
- ✅ Fixes implemented for critical issues
- ✅ Target: ≥18/22 queries passing (≥80%)

---

### Day 5: Performance Validation & Documentation (4-6 hours)

**Task 5.1**: Performance benchmarking (2-3 hours)
- Measure execution time for all passing queries
- Compare Thunderduck vs Spark Connect
- Document speedup metrics
- Identify optimization opportunities

**Task 5.2**: Create Week 14 completion report (2-3 hours)
- Document query coverage (target: ≥80%)
- List all validated queries
- Document any failed queries with root causes
- Summarize performance results
- Recommendations for Week 15

**Success Criteria**:
- ✅ Performance data collected for all passing queries
- ✅ Completion report created
- ✅ Week 14 objectives met

---

## Implementation Strategy

### Approach: Incremental Validation

**Phase A**: Get all queries and run smoke tests
- Goal: Know what works out of the box
- Expected: Some will pass immediately (simple scans/joins)

**Phase B**: Generate reference data
- Goal: Establish ground truth from Spark
- Critical: Must use same data as Thunderduck

**Phase C**: Run correctness tests and fix issues
- Goal: Maximize query coverage
- Pragmatic: Some complex queries may need more work

**Phase D**: Document and celebrate wins
- Goal: Show progress and identify gaps
- Realistic: Might not hit 100%, but >80% is excellent

---

## TPC-H Query Categorization

### Expected to Pass Easily (5-7 queries)
- Queries using only features we've already validated
- Joins, aggregates, filters, GROUP BY, ORDER BY
- Examples: Q2, Q4, Q7, Q14

### May Require Work (4-6 queries)
- Queries with advanced SQL features
- EXISTS, NOT IN, NOT EXISTS
- Complex subqueries
- Examples: Q8, Q9, Q16, Q20, Q21

### High Complexity (2-3 queries)
- Queries with uncommon SQL patterns
- WITH clauses (Q15)
- Multiple correlated subqueries (Q22)
- May need feature implementation

---

## Success Metrics

### Minimum Success (Week 14 Complete)
- ✅ All 22 TPC-H query files obtained
- ✅ All 22 queries tested
- ✅ ≥18/22 passing (≥80% coverage)
- ✅ Failures documented with root causes

### Target Success (Exceed Expectations)
- ✅ 20/22 passing (≥90% coverage)
- ✅ Performance data for all passing queries
- ✅ Clear roadmap for remaining queries

### Stretch Goal (Amazing)
- ✅ 22/22 passing (100% coverage!)
- ✅ All queries faster than Spark
- ✅ Production-ready for TPC-H workloads

---

## Resource Requirements

### TPC-H Query Sources
- TPC-H GitHub repositories (compatible implementations)
- Or: Generate from TPC-H specification
- Verify: Compatible with Spark SQL syntax

### Development Time
- Day 1: 4-6 hours (query acquisition + smoke test)
- Day 2-3: 8-12 hours (reference generation + test creation)
- Day 4: 6-8 hours (validation + fixes)
- Day 5: 4-6 hours (performance + documentation)
- **Total**: 22-32 hours

### Infrastructure
- ✅ TPC-H data: Already generated (SF=0.01)
- ✅ Test framework: Already built
- ✅ Server: Working reliably
- ✅ Correctness validation: Proven methodology

---

## Risk Mitigation

### Risk 1: Missing SQL Features
**Probability**: Medium
**Impact**: High
**Mitigation**:
- Document missing features clearly
- Prioritize based on query importance
- Some queries may be deferred to Week 15

### Risk 2: Complex Query Failures
**Probability**: High
**Impact**: Medium
**Mitigation**:
- Focus on query coverage percentage, not 100%
- Document failures with reproduction steps
- Create feature roadmap for complex queries

### Risk 3: Performance Issues
**Probability**: Low
**Impact**: Medium
**Mitigation**:
- Performance not required for initial validation
- Focus on correctness first
- Optimization can be Week 15 focus

---

## Deliverables

### Required (Week 14 Complete)
1. All 22 TPC-H query SQL files
2. Test methods for all 22 queries
3. Spark reference data for all queries
4. Pass/fail status for each query
5. Week 14 completion report

### Optional (Nice to Have)
6. Performance benchmarks for passing queries
7. Feature gap analysis
8. Optimization recommendations

---

## Next Steps (Day 1)

**Immediate Actions**:

1. **Find TPC-H queries** (Q2, Q4, Q7-Q11, Q14-Q17, Q19-Q22)
   - Search for official TPC-H benchmark queries
   - Verify SQL syntax compatibility with Spark
   - Download/create remaining 14 queries

2. **Smoke test new queries**
   - Run each through Thunderduck
   - Check for execution errors
   - Document initial status

3. **Generate reference data**
   - Run all 14 new queries through Spark local mode
   - Save JSON reference files
   - Verify output format matches Week 13 pattern

**Expected Time**: 4-6 hours for Day 1 tasks

---

## Week 14 Timeline

| Day | Tasks | Hours | Deliverable |
|-----|-------|-------|-------------|
| 1 | Query acquisition + smoke test | 4-6 | All 22 queries available |
| 2-3 | Reference generation + test creation | 8-12 | Reference data + tests |
| 4 | Validation + fixes | 6-8 | Test results + fixes |
| 5 | Performance + documentation | 4-6 | Completion report |
| **Total** | **All tasks** | **22-32** | **≥80% TPC-H coverage** |

---

**Plan Created**: 2025-10-27
**Status**: Ready to execute
**First Task**: Obtain remaining 14 TPC-H query SQL files
