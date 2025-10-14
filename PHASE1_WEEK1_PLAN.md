# Phase 1, Week 1: Detailed Implementation Plan
## Core Infrastructure Setup

**Timeline**: Days 1-5 (Week 1)
**Goal**: Establish foundational components with 50+ passing tests
**Success Criteria**: Core module compiles, type mapping 100% accurate, 80%+ coverage

---

## Day 1: Project Structure & Build System

### Task 1.1: Maven Multi-Module Setup
**Owner**: Build Engineer
**Duration**: 2-3 hours

**Subtasks**:
- [x] Create parent POM with dependency management
- [x] Create core module structure
- [x] Create tests module structure
- [x] Configure Java 17 compiler
- [x] Add DuckDB JDBC dependency (1.1.3)
- [x] Add Apache Arrow dependencies (17.0.0)
- [x] Add Spark SQL API (3.5.3, provided scope)
- [x] Add JUnit 5 (5.10.0)
- [x] Add AssertJ (3.24.2)
- [x] Configure Maven Surefire for parallel test execution

**Deliverable**: `pom.xml` files, compiling project structure

**Verification**:
```bash
mvn clean compile  # Should succeed
mvn clean test     # Should succeed (0 tests)
```

---

## Day 2: Logical Plan Representation

### Task 2.1: Base LogicalPlan Class
**Owner**: Core Developer
**Duration**: 1 hour

**Implementation**:
```java
// core/src/main/java/com/catalyst2sql/logical/LogicalPlan.java
public abstract class LogicalPlan {
    protected List<LogicalPlan> children;
    protected StructType schema;

    public abstract String toSQL(SQLGenerator generator);
    public abstract StructType inferSchema();
    public List<LogicalPlan> children() { return children; }
    public StructType schema() { return schema; }
}
```

### Task 2.2: Leaf Nodes (3 types)
**Owner**: Core Developer
**Duration**: 2 hours

**Classes to implement**:
1. `TableScan` - Read from Parquet/Delta/Iceberg
2. `InMemoryRelation` - In-memory data
3. `LocalRelation` - Empty relation

### Task 2.3: Unary Operators (5 types)
**Owner**: Core Developer
**Duration**: 3 hours

**Classes to implement**:
1. `Project` - SELECT columns
2. `Filter` - WHERE conditions
3. `Sort` - ORDER BY
4. `Limit` - LIMIT/OFFSET
5. `Aggregate` - GROUP BY

### Task 2.4: Binary Operators (2 types)
**Owner**: Core Developer
**Duration**: 2 hours

**Classes to implement**:
1. `Join` - All join types
2. `Union` - UNION/UNION ALL

**Deliverable**: 11 logical plan classes, all compiling

---

## Day 3: Type Mapping System

### Task 3.1: TypeMapper Core
**Owner**: Type System Engineer
**Duration**: 2 hours

**Implementation**:
```java
// core/src/main/java/com/catalyst2sql/types/TypeMapper.java
public class TypeMapper {
    public static String toDuckDBType(DataType sparkType);
    public static DataType toSparkType(String duckdbType);
    public static boolean isCompatible(DataType spark, String duckdb);
}
```

**Type mappings to implement**:
- Primitive types (8): byte, short, int, long, float, double, boolean, string
- Temporal types (2): date, timestamp
- Binary type (1): binary
- Decimal type (1): decimal(p, s)
- Complex types (3): array, map, struct

### Task 3.2: Type Mapping Tests (50+ tests)
**Owner**: QA Engineer
**Duration**: 3 hours

**Test categories**:
- Primitive type mappings (8 tests)
- Temporal type mappings (2 tests)
- Decimal precision/scale variations (10 tests)
- Array type mappings (10 tests)
- Map type mappings (10 tests)
- Struct type mappings (10 tests)
- Edge cases (nullability, nested types) (10 tests)

**Deliverable**: TypeMapper class + 50+ passing tests

---

## Day 4: Function Registry & Expression System

### Task 4.1: FunctionRegistry Core
**Owner**: Function Engineer
**Duration**: 3 hours

**Implementation**:
```java
// core/src/main/java/com/catalyst2sql/functions/FunctionRegistry.java
public class FunctionRegistry {
    private static final Map<String, String> DIRECT_MAPPINGS;
    private static final Map<String, FunctionTranslator> CUSTOM_TRANSLATORS;

    public String translate(String sparkFunction, List<Expression> args);
    public boolean isSupported(String sparkFunction);
}
```

**Function categories to implement**:
- String functions (15): upper, lower, trim, substring, concat, etc.
- Math functions (15): abs, ceil, floor, round, sqrt, pow, etc.
- Date functions (10): year, month, day, date_add, datediff, etc.
- Aggregate functions (10): sum, avg, min, max, count, etc.

### Task 4.2: Expression System
**Owner**: Core Developer
**Duration**: 2 hours

**Classes to implement**:
- `Expression` (abstract base)
- `Literal` (constants)
- `ColumnReference` (column refs)
- `BinaryExpression` (arithmetic, comparison)
- `UnaryExpression` (negation, not)
- `FunctionCall` (function invocations)

### Task 4.3: Expression Translation Tests (50+ tests)
**Owner**: QA Engineer
**Duration**: 2 hours

**Test categories**:
- Literal expressions (5 tests)
- Column references (5 tests)
- Arithmetic operators (10 tests)
- Comparison operators (10 tests)
- Logical operators (10 tests)
- Function calls (10 tests)

**Deliverable**: FunctionRegistry + Expression system + 50+ tests

---

## Day 5: Testing Framework & Integration

### Task 5.1: Test Infrastructure
**Owner**: QA Engineer
**Duration**: 2 hours

**Components to create**:
1. `TestDataBuilder` - Fluent API for test data
2. `DifferentialAssertion` - Spark comparison utilities
3. `TestBase` - Base test class with setup/teardown
4. `TestCategories` - JUnit 5 tags (@Tier1, @Tier2, etc.)

### Task 5.2: Run All Tests & Coverage
**Owner**: QA Engineer
**Duration**: 1 hour

**Commands**:
```bash
# Run all tests
mvn clean test

# Generate coverage report
mvn clean verify -Pcoverage

# View coverage
open target/site/jacoco/index.html
```

**Success criteria**:
- ✅ 100+ tests passing (50 type + 50 expression)
- ✅ 0 test failures
- ✅ 80%+ line coverage on core module
- ✅ Build time < 5 minutes

### Task 5.3: Week 1 Completion Report
**Owner**: Coordinator
**Duration**: 1 hour

**Report sections**:
1. Implementation summary
2. Test results (count, pass rate, coverage)
3. Performance metrics (build time, test execution time)
4. Issues encountered and resolutions
5. Next steps for Week 2

**Deliverable**: Completion report with all metrics

---

## Parallel Execution Strategy

### Track A: Core Implementation (Days 2-4)
- Logical plan representation
- Type mapping system
- Function registry
- Expression system

### Track B: Testing Infrastructure (Days 3-5)
- Test framework setup
- Type mapping tests
- Expression translation tests
- Test infrastructure utilities

### Track C: Build System (Days 1, 5)
- Maven configuration (Day 1)
- CI/CD integration (Day 5)
- Coverage reporting (Day 5)

---

## Resource Allocation

| Role | Engineer | Time Allocation |
|------|----------|----------------|
| Build Engineer | Engineer 1 | 20% (1 day) |
| Core Developer | Engineer 2 | 80% (4 days) |
| Type System Engineer | Engineer 3 | 40% (2 days) |
| Function Engineer | Engineer 3 | 40% (2 days) |
| QA Engineer | Engineer 4 | 100% (5 days) |
| Coordinator | Tech Lead | 20% (1 day) |

---

## Success Metrics

### Code Metrics
- Classes implemented: 25+
- Lines of code: ~2,500
- Test classes: 10+
- Test methods: 100+

### Quality Metrics
- Test pass rate: 100%
- Line coverage: 80%+
- Branch coverage: 75%+
- Build success: 100%

### Performance Metrics
- Build time: < 5 min
- Test execution: < 1 min
- Memory usage: < 2 GB

---

## Risk Mitigation

### Risk 1: Spark API Compatibility
**Mitigation**: Use Spark 3.5.3 source code as reference, validate each type mapping

### Risk 2: DuckDB Type Mismatches
**Mitigation**: Comprehensive differential testing, validate with real DuckDB queries

### Risk 3: Timeline Slippage
**Mitigation**: Focus on MVP (basic types first), defer complex types to Week 2 if needed

---

## Deliverables Checklist

- [x] Parent POM with dependency management
- [x] Core module with logical plan classes (11 classes)
- [x] TypeMapper with all primitive types (15 mappings)
- [x] FunctionRegistry with 50+ functions (120+ implemented)
- [x] Expression system (6 classes)
- [x] Test infrastructure (TestDataBuilder, DifferentialAssertion)
- [ ] 50+ type mapping tests (100% passing) **CRITICAL: 0 implemented**
- [ ] 50+ expression tests (100% passing) **CRITICAL: 0 implemented**
- [ ] Coverage report (80%+ coverage) **BLOCKED: No tests to measure**
- [x] Week 1 completion report

---

**Status**: WEEK 1 COMPLETE (Infrastructure) - TESTS MISSING
**Overall Assessment**: PARTIAL SUCCESS ⚠️
**Critical Gap**: Zero tests implemented (0% of 100+ target)
**Next Action**: IMMEDIATE - Implement test suites (Days 6-7) before Week 2
**Completion Report**: See WEEK1_COMPLETION_REPORT.md
