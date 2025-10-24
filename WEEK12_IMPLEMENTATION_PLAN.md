# Week 12 Implementation Plan: TPC-H Q1 Integration

**Duration**: November 18-22, 2025 (5 days, 28 hours)
**Goal**: Execute TPC-H Q1 end-to-end via Spark Connect protocol with plan deserialization

---

## Executive Summary

Week 12 extends the MVP Spark Connect Server from Week 11 to support TPC-H Query 1, requiring plan deserialization for DataFrame operations. This involves translating Spark Connect protocol plans (Protobuf) into thunderduck LogicalPlans, then generating DuckDB SQL for execution. TPC-H Q1 is the perfect test case as it includes column expressions, GROUP BY, aggregations, and ORDER BY.

**Critical Path**: Plan deserialization is the foundation for all DataFrame API support. Without it, clients can only execute raw SQL strings, severely limiting functionality.

---

## Prerequisites (from Week 11) ✅

- Working gRPC server on port 15002
- ExecutePlan RPC with Arrow streaming
- Session management with timeout
- PySpark compatibility (SqlCommand, ShowString, config)
- connect-server module building successfully

---

## Day 1: Plan Deserialization Foundation (Monday, 6 hours)

### Morning (3 hours): Protobuf Plan Analysis

**Task 1.1: Study Spark Connect Plan Structure** (1.5 hours)
- Analyze relations.proto for Relation types
- Study expressions.proto for Expression types
- Document plan tree structure for TPC-H Q1
- Map Protobuf types to thunderduck LogicalPlan nodes

**Task 1.2: Create PlanConverter Class** (1.5 hours)
```java
// connect-server/src/main/java/com/thunderduck/connect/converter/PlanConverter.java
public class PlanConverter {
    public LogicalPlan convert(Plan plan) { }
    private LogicalPlan convertRelation(Relation relation) { }
    private Expression convertExpression(org.apache.spark.connect.proto.Expression expr) { }
}
```

### Afternoon (3 hours): Basic Relation Converters

**Task 1.3: Implement Read Relation** (1.5 hours)
- Handle Read.DataSource.format = "parquet"
- Extract file paths from options
- Create TableScan LogicalPlan node
- Unit test with lineitem.parquet read

**Task 1.4: Implement Project Relation** (1.5 hours)
- Convert Project.expressions list
- Map column references (UnresolvedAttribute)
- Handle aliased expressions (Alias)
- Unit test with column selection

**Tests**: 10 unit tests
- ReadRelationTest: 5 tests (parquet, paths, options)
- ProjectRelationTest: 5 tests (columns, aliases, expressions)

---

## Day 2: Expression Translation (Tuesday, 6 hours)

### Morning (3 hours): Basic Expressions

**Task 2.1: Column References** (1 hour)
- UnresolvedAttribute → ColumnExpression
- Handle qualified names (table.column)
- Support column position references
- Unit test: 5 tests

**Task 2.2: Literals** (1 hour)
- Convert Literal.LiteralType (all types)
- Handle nulls properly
- Date/timestamp formatting
- Unit test: 10 tests (one per type)

**Task 2.3: Arithmetic & Comparison** (1 hour)
- Binary operators (+, -, *, /, %)
- Comparison operators (=, !=, <, <=, >, >=)
- Unary operators (-, NOT)
- Unit test: 10 tests

### Afternoon (3 hours): Complex Expressions

**Task 2.4: Function Calls** (1.5 hours)
- UnresolvedFunction → FunctionExpression
- Map function names (SUM, AVG, COUNT)
- Handle DISTINCT aggregates
- Unit test: 10 tests

**Task 2.5: CAST Expressions** (1.5 hours)
- Cast → CastExpression
- Type conversion mapping
- Implicit cast handling
- Unit test: 5 tests

**Tests**: 40 expression tests total

---

## Day 3: Aggregate & Sort Relations (Wednesday, 6 hours)

### Morning (3 hours): Aggregate Translation

**Task 3.1: GROUP BY Implementation** (1.5 hours)
```java
private LogicalPlan convertAggregate(Aggregate aggregate) {
    List<Expression> groupingExprs = convertExpressions(aggregate.getGroupingExpressionsList());
    List<Expression> aggregateExprs = convertExpressions(aggregate.getAggregateExpressionsList());
    LogicalPlan child = convertRelation(aggregate.getInput());
    return new AggregateNode(groupingExprs, aggregateExprs, child);
}
```
- Handle grouping expressions
- Map aggregate functions
- Support HAVING clause (if present)
- Unit test: 10 tests

**Task 3.2: Aggregate Functions** (1.5 hours)
- SUM, AVG, COUNT, MIN, MAX
- COUNT(DISTINCT)
- Handle partial aggregates
- Unit test: 10 tests

### Afternoon (3 hours): Sort & Filter

**Task 3.3: Sort Relation** (1.5 hours)
- Convert Sort.order list
- Handle ASC/DESC, NULLS FIRST/LAST
- Multiple sort columns
- Unit test: 5 tests

**Task 3.4: Filter Relation** (1.5 hours)
- Convert WHERE conditions
- Complex boolean logic (AND/OR/NOT)
- IN/NOT IN predicates
- Unit test: 10 tests

**Tests**: 35 tests for aggregate/sort/filter

---

## Day 4: TPC-H Q1 End-to-End (Thursday, 5 hours)

### Morning (3 hours): TPC-H Q1 Implementation

**Task 4.1: TPC-H Q1 DataFrame Operations** (1.5 hours)
```python
# The DataFrame operations that must work:
df = spark.read.parquet("lineitem.parquet")
df = df.filter(col("l_shipdate") <= "1998-12-01")
df = df.group_by("l_returnflag", "l_linestatus").agg(
    F.sum("l_quantity").alias("sum_qty"),
    F.sum("l_extendedprice").alias("sum_base_price"),
    F.sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("sum_disc_price"),
    F.sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).alias("sum_charge"),
    F.avg("l_quantity").alias("avg_qty"),
    F.avg("l_extendedprice").alias("avg_price"),
    F.avg("l_discount").alias("avg_disc"),
    F.count("*").alias("count_order")
)
df = df.order_by("l_returnflag", "l_linestatus")
df.show()
```

**Task 4.2: Generate TPC-H Data** (0.5 hours)
```bash
# Use DuckDB to generate SF=0.01 (10MB)
duckdb << EOF
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);
COPY (SELECT * FROM lineitem) TO 'data/tpch_sf001/lineitem.parquet';
EOF
```

**Task 4.3: Python Client Script** (1 hour)
```python
# tpch_q1_test.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Load data
df = spark.read.parquet("data/tpch_sf001/lineitem.parquet")

# Execute Q1
result = df.filter(...).groupBy(...).agg(...).orderBy(...)
result.show()

# Validate results
assert result.count() == 4  # Q1 returns 4 rows
```

### Afternoon (2 hours): Integration Testing

**Task 4.4: End-to-End Test Suite** (1 hour)
- TPCHIntegrationTest.java
- Test data loading
- Test query execution
- Validate result schema
- Validate row count

**Task 4.5: Performance Measurement** (1 hour)
- Measure query execution time
- Compare with embedded mode
- Target: < 5 seconds for SF=0.01
- Document overhead breakdown

**Tests**: 5 integration tests

---

## Day 5: Debugging & Polish (Friday, 5 hours)

### Morning (3 hours): Debug & Fix Issues

**Task 5.1: Debug Plan Deserialization** (1.5 hours)
- Add detailed logging to PlanConverter
- Test with actual PySpark client
- Fix any type mapping issues
- Handle edge cases

**Task 5.2: SQL Generation Validation** (1.5 hours)
- Log generated SQL for debugging
- Verify SQL correctness
- Test in DuckDB directly
- Fix any SQL generation bugs

### Afternoon (2 hours): Documentation & Tests

**Task 5.3: Documentation** (1 hour)
- Update connect-server/README.md
- Document supported operations
- Add TPC-H Q1 example
- Document limitations

**Task 5.4: Additional Tests** (1 hour)
- Add edge case tests
- Test error handling
- Test unsupported operations
- Clean up test code

**Tests**: 10 additional tests

---

## Deliverables

### Code Deliverables
1. **PlanConverter.java** - Main plan deserialization (500+ LOC)
2. **RelationConverter.java** - Relation type handlers (300+ LOC)
3. **ExpressionConverter.java** - Expression converters (400+ LOC)
4. **TPCHIntegrationTest.java** - Integration tests (200+ LOC)
5. **tpch_q1_test.py** - Python client test (100+ LOC)

### Test Coverage
- **Unit Tests**: 100 tests
  - Plan conversion: 20 tests
  - Expression conversion: 40 tests
  - Relation conversion: 35 tests
  - Edge cases: 5 tests
- **Integration Tests**: 5 tests
  - TPC-H Q1 end-to-end
  - Performance validation
  - Schema validation
  - Result correctness
  - Error handling

### Documentation
- Updated README with DataFrame examples
- Plan deserialization architecture diagram
- Supported operations reference
- Performance benchmark results

---

## Success Criteria

### Functional Requirements
- ✅ TPC-H Q1 executes via PySpark DataFrame API
- ✅ Results match expected output (4 rows)
- ✅ All aggregate functions work (SUM, AVG, COUNT)
- ✅ GROUP BY with multiple columns works
- ✅ ORDER BY with multiple columns works
- ✅ Complex expressions (arithmetic) work

### Performance Requirements
- ✅ Query execution < 5 seconds (SF=0.01)
- ✅ Server overhead < 100ms vs embedded
- ✅ Plan deserialization < 50ms
- ✅ Arrow streaming efficient (< 10% overhead)

### Quality Requirements
- ✅ 100% test pass rate (105 tests)
- ✅ Code coverage ≥ 85%
- ✅ Zero compiler warnings
- ✅ Documentation complete

---

## Risk Mitigation

### Risk 1: Complex Plan Trees
**Mitigation**: Start with simple operations, incrementally add complexity. TPC-H Q1 is well-understood.

### Risk 2: Type Mapping Issues
**Mitigation**: Extensive logging, unit tests for each type, validate against Spark behavior.

### Risk 3: Performance Overhead
**Mitigation**: Profile plan deserialization, optimize hot paths, cache converted plans if needed.

### Risk 4: Missing Operations
**Mitigation**: Focus on Q1 requirements only, defer other operations to Week 13.

---

## Daily Schedule

### Monday (Plan Foundation)
- 9:00-12:00: Protobuf analysis, PlanConverter setup
- 1:00-4:00: Read and Project relations
- 4:00-5:00: Run tests, commit

### Tuesday (Expressions)
- 9:00-12:00: Basic expressions (columns, literals, operators)
- 1:00-4:00: Complex expressions (functions, CAST)
- 4:00-5:00: Run tests, commit

### Wednesday (Aggregates & Sort)
- 9:00-12:00: GROUP BY and aggregate functions
- 1:00-4:00: ORDER BY and WHERE
- 4:00-5:00: Run tests, commit

### Thursday (TPC-H Q1)
- 9:00-12:00: TPC-H Q1 implementation, data generation
- 1:00-3:00: Integration testing
- 3:00-4:00: Performance measurement
- 4:00-5:00: Run tests, commit

### Friday (Polish)
- 9:00-12:00: Debug and fix issues
- 1:00-3:00: Documentation and additional tests
- 3:00-4:00: Final testing
- 4:00-5:00: Week 12 completion report

---

## Technical Notes

### Plan Tree Structure for TPC-H Q1
```
Sort
└── Aggregate
    └── Filter
        └── Read (lineitem.parquet)
```

### Generated SQL Target
```sql
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM read_parquet('data/tpch_sf001/lineitem.parquet')
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

### Key Implementation Classes
- **PlanConverter**: Main entry point for plan deserialization
- **RelationConverter**: Handles Relation types (Read, Project, Filter, Aggregate, Sort)
- **ExpressionConverter**: Handles Expression types (Column, Literal, Function, Cast, Binary)
- **TypeMapper**: Maps Spark types to thunderduck types
- **FunctionRegistry**: Maps Spark functions to DuckDB functions

---

## Dependencies on Core Module

From thunderduck core:
- LogicalPlan hierarchy
- Expression classes
- SQLGenerator
- QueryExecutor
- DuckDBConnectionManager

---

## Testing Strategy

### Unit Test Pattern
```java
@Test
void testAggregateConversion() {
    // Given: Protobuf Aggregate relation
    Aggregate aggregate = Aggregate.newBuilder()
        .addGroupingExpressions(...)
        .addAggregateExpressions(...)
        .setInput(...)
        .build();

    // When: Convert to LogicalPlan
    LogicalPlan plan = converter.convertRelation(aggregate);

    // Then: Verify structure
    assertThat(plan).isInstanceOf(AggregateNode.class);
    // ... verify details
}
```

### Integration Test Pattern
```java
@Test
void testTpchQ1EndToEnd() {
    // Given: TPC-H Q1 as DataFrame operations
    Plan plan = buildTpchQ1Plan();

    // When: Execute via server
    ExecutePlanResponse response = server.executePlan(plan);

    // Then: Verify results
    assertThat(response.getRowCount()).isEqualTo(4);
    // ... verify data
}
```

---

**Plan Version**: 1.0
**Created**: 2025-10-17
**Status**: Ready for Execution