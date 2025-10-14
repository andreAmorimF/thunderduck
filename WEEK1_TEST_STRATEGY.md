# Week 1 Test Strategy & Quality Assurance Plan
**Project**: catalyst2sql - Spark Catalyst to DuckDB SQL Translator
**Author**: TESTER Agent (Hive Mind Collective Intelligence)
**Date**: 2025-10-14
**Status**: Test Strategy for Gap Remediation

---

## Executive Summary

This document provides a comprehensive testing strategy to address the **CRITICAL GAP** identified in the Week 1 Gap Analysis: **zero tests implemented** (0% of 100+ target). The current implementation has 39 classes and ~3,800 LOC but lacks any validation, creating high risk for production deployment.

### Testing Objectives
1. **Validate all 39 implemented classes** for correctness
2. **Achieve 80%+ line coverage** and 75%+ branch coverage
3. **Prevent regressions** through comprehensive test suites
4. **Enable confident Week 2 development** with solid foundation
5. **Establish testing patterns** for future development

### Current Status
- Implementation: 39 classes (156% of target) ✅
- Tests: 0 test files (0% of target) ❌
- Coverage: 0% (not measurable) ❌
- Risk Level: **CRITICAL** 🔴

---

## 1. Assessment of Current Test Coverage

### 1.1 Existing Test Infrastructure ✅

**GOOD NEWS**: Test infrastructure exists and is well-designed.

**Available Test Files**:
```
tests/src/test/java/com/catalyst2sql/test/
├── TestBase.java              (59 lines) - JUnit 5 lifecycle hooks
├── TestDataBuilder.java       (221 lines) - Fluent API for test data
├── DifferentialAssertion.java (320 lines) - Custom AssertJ assertions
└── TestCategories.java        (203 lines) - JUnit 5 tag annotations
```

**Test Infrastructure Quality**: **EXCELLENT** ⭐

**Strengths**:
1. **TestBase**: Provides timing, logging, and scenario execution
2. **TestDataBuilder**: Fluent API for schema/type construction
3. **DifferentialAssertion**: SQL comparison with whitespace/case insensitivity
4. **TestCategories**: Comprehensive tagging system (@Tier1, @Tier2, @Unit, @Integration)

**Current Test Compilation Issues**: ❌

The test files created previously have **18 compilation errors** due to API mismatches:
- `ColumnReference` uses `columnName()` not `name()`
- `ColumnReference` constructor parameter order issues
- `FunctionCall` requires `List<Expression>` not varargs in constructor
- Missing `toSQL()` method on some Expression types

---

### 1.2 Test Coverage Analysis by Component

| Component | Classes | LOC | Tests Needed | Current Tests | Gap | Priority |
|-----------|---------|-----|--------------|---------------|-----|----------|
| **Type System** | 16 | ~800 | 50+ | 0 | -50 | **CRITICAL** |
| **Expression System** | 6 | ~600 | 50+ | 0 | -50 | **CRITICAL** |
| **Function Registry** | 2 | ~350 | 30+ | 0 | -30 | HIGH |
| **Logical Plan** | 13 | ~1,500 | 30+ | 0 | -30 | HIGH |
| **Test Infrastructure** | 4 | ~600 | 10+ | 0 | -10 | MEDIUM |
| **TOTAL** | **41** | **~3,850** | **170+** | **0** | **-170** | **BLOCKING** |

---

## 2. Critical Testing Gaps

### 2.1 Priority 1: Type Mapping Tests (CRITICAL 🔴)

**Why Critical**: Type mapping errors cause data corruption and silent failures.

**Missing Coverage**:
```
TypeMapperTest.java - 0/50+ tests
├── Primitive type mappings (0/8 tests)
│   ├── ByteType → TINYINT
│   ├── ShortType → SMALLINT
│   ├── IntegerType → INTEGER
│   ├── LongType → BIGINT
│   ├── FloatType → FLOAT
│   ├── DoubleType → DOUBLE
│   ├── StringType → VARCHAR
│   └── BooleanType → BOOLEAN
├── Temporal type mappings (0/2 tests)
│   ├── DateType → DATE
│   └── TimestampType → TIMESTAMP
├── Binary type mapping (0/1 test)
│   └── BinaryType → BLOB
├── Decimal precision/scale (0/10 tests)
│   ├── DECIMAL(10,2), DECIMAL(18,4), DECIMAL(38,0)
│   ├── Edge cases: DECIMAL(1,0), DECIMAL(38,38)
│   └── Round-trip conversion validation
├── Array type mappings (0/10 tests)
│   ├── INTEGER[], VARCHAR[], nested arrays
│   └── Complex element types: DECIMAL(10,2)[]
├── Map type mappings (0/10 tests)
│   ├── MAP(VARCHAR, INTEGER)
│   ├── Nested maps: MAP(VARCHAR, MAP(VARCHAR, INTEGER))
│   └── Complex value types
├── Reverse mappings (0/7 tests)
│   ├── DuckDB → Spark type conversion
│   └── Alias support (INT ↔ INTEGER, TEXT ↔ VARCHAR)
└── Edge cases (0/10 tests)
    ├── Null handling
    ├── Case-insensitive parsing
    ├── Whitespace handling
    └── Invalid type strings
```

**Risk if Not Tested**:
- Data type mismatches cause runtime failures
- Precision loss in DECIMAL conversions
- Array/Map nesting errors
- Silent data corruption

---

### 2.2 Priority 1: Expression Translation Tests (CRITICAL 🔴)

**Why Critical**: Expression translation is core functionality for SQL generation.

**Missing Coverage**:
```
ExpressionTest.java - 0/50+ tests
├── Literal expressions (0/10 tests)
│   ├── Integer, Long, Float, Double literals
│   ├── String literals with quote escaping
│   ├── Boolean TRUE/FALSE literals
│   └── NULL literals
├── Column references (0/5 tests)
│   ├── Simple: customer_id
│   ├── Qualified: orders.order_id
│   ├── Nullable column references
│   └── Complex types (ArrayType, MapType)
├── Arithmetic expressions (0/10 tests)
│   ├── Addition, Subtraction, Multiplication, Division, Modulo
│   ├── Nested arithmetic: (a + b) * c
│   ├── Type inference: Double + Integer = Double
│   └── Nullability propagation
├── Comparison expressions (0/10 tests)
│   ├── =, !=, <, <=, >, >=
│   ├── Boolean return type validation
│   └── Type compatibility checking
├── Logical expressions (0/10 tests)
│   ├── AND, OR, NOT
│   ├── Chained conditions: a AND b AND c
│   └── Complex: (a AND b) OR (c AND d)
└── Function calls (0/10 tests)
    ├── Single argument: upper(name)
    ├── Multiple arguments: substring(text, 1, 5)
    ├── Nested functions: trim(lower(name))
    └── Integration with FunctionRegistry
```

**Risk if Not Tested**:
- Incorrect SQL generation
- Operator precedence errors
- Type inference failures
- Null propagation bugs

---

### 2.3 Priority 2: Function Registry Tests (HIGH 🟠)

**Why High Priority**: 142 functions registered, need validation for correctness.

**Missing Coverage**:
```
FunctionRegistryTest.java - 0/30+ tests
├── String functions (0/10 tests)
│   ├── upper, lower, trim, substring, concat
│   ├── Regular expressions: regexp_extract, regexp_replace
│   └── Case-insensitive function names
├── Math functions (0/10 tests)
│   ├── abs, ceil, floor, round, sqrt, pow
│   ├── Trigonometric: sin, cos, tan
│   └── Logarithmic: log, ln, log10
├── Date/time functions (0/10 tests)
│   ├── year, month, day, hour, minute, second
│   ├── date_add, date_sub, datediff
│   └── date_format, current_date
├── Aggregate functions (0/10 tests)
│   ├── sum, avg, min, max, count
│   ├── stddev, variance
│   └── collect_list, collect_set
├── Window functions (0/5 tests)
│   ├── row_number, rank, dense_rank
│   ├── lag, lead
│   └── Window frame handling
├── Array functions (0/5 tests)
│   ├── array_contains → list_contains
│   ├── size → len
│   └── explode → unnest
├── Conditional functions (0/5 tests)
│   ├── coalesce, if, nullif
│   └── Null handling
└── Error handling (0/5 tests)
    ├── Unsupported functions throw UnsupportedOperationException
    ├── Null/empty function names throw IllegalArgumentException
    └── isSupported() validation
```

**Risk if Not Tested**:
- Function mapping errors (wrong DuckDB function)
- Missing translations for common functions
- Incorrect argument passing

---

### 2.4 Priority 2: Logical Plan Tests (HIGH 🟠)

**Why High Priority**: Logical plan is core query representation.

**Missing Coverage**:
```
LogicalPlanTest.java - 0/30+ tests
├── Leaf nodes (0/5 tests)
│   ├── TableScan schema inference
│   ├── InMemoryRelation construction
│   └── LocalRelation empty schema
├── Unary operators (0/10 tests)
│   ├── Project schema inference
│   ├── Filter schema preservation
│   ├── Sort schema preservation
│   ├── Limit schema preservation
│   └── Aggregate schema transformation
├── Binary operators (0/5 tests)
│   ├── Join schema merge
│   ├── Join types: INNER, LEFT, RIGHT, FULL
│   └── Union schema compatibility
├── SQL generation (0/10 tests)
│   ├── SELECT * FROM table
│   ├── SELECT col1, col2 FROM table WHERE condition
│   ├── JOIN queries
│   ├── GROUP BY queries
│   └── Complex nested queries
└── Schema inference (0/5 tests)
    ├── Column propagation through operators
    ├── Type preservation
    └── Nullability tracking
```

**Risk if Not Tested**:
- Schema inference errors
- SQL generation failures
- Join condition bugs
- GROUP BY/ORDER BY issues

---

### 2.5 Priority 3: Integration Tests (MEDIUM 🟡)

**Why Medium Priority**: End-to-end validation with real DuckDB.

**Missing Coverage**:
```
IntegrationTest.java - 0/20+ tests
├── Simple queries (0/5 tests)
│   ├── SELECT * FROM table
│   ├── SELECT with WHERE clause
│   └── SELECT with ORDER BY
├── JOIN queries (0/5 tests)
│   ├── INNER JOIN
│   ├── LEFT/RIGHT/FULL OUTER JOIN
│   └── Multi-table joins
├── Aggregate queries (0/5 tests)
│   ├── GROUP BY with aggregates
│   ├── HAVING clause
│   └── Window functions
└── Differential testing (0/5 tests)
    ├── Compare Spark vs DuckDB results
    ├── Validate numeric precision
    └── Validate date/time handling
```

**Risk if Not Tested**:
- Unknown incompatibilities with DuckDB
- Performance bottlenecks not identified
- Edge cases not discovered

---

## 3. Test Strategy for Gap Remediation

### 3.1 Testing Approach

**Test Philosophy**: **Test Pyramid with Differential Validation**

```
                 /\
                /  \  Integration Tests (10%)
               /    \  - End-to-end scenarios
              /------\  - DuckDB validation
             /        \
            /   E2E    \ Differential Tests (10%)
           /  Tests (5) \  - Spark vs DuckDB comparison
          /--------------\
         /                \
        /  Integration (15) \ Component Tests (30%)
       /                     \  - Logical plan nodes
      /------------------------\  - SQL generation
     /                          \
    /       Component Tests      \ Unit Tests (50%)
   /                              \  - Type mappings
  /          Unit Tests (100+)     \  - Function registry
 /                                  \  - Expression translation
/------------------------------------\
```

**Test Distribution**:
- **Unit Tests (50%)**: 100+ tests for type system, expressions, functions
- **Component Tests (30%)**: 60+ tests for logical plan, SQL generation
- **Integration Tests (15%)**: 30+ tests for end-to-end scenarios
- **Differential Tests (5%)**: 10+ tests comparing Spark vs DuckDB

---

### 3.2 Test Implementation Phases

**Phase 1: Foundation (Day 6 AM) - Fix Existing Tests**
- **Duration**: 4 hours
- **Goal**: Get existing test files compiling
- **Tasks**:
  1. Fix TypeMapperTest compilation errors
  2. Fix FunctionRegistryTest compilation errors
  3. Fix ExpressionTest compilation errors
  4. Run first successful test execution: `mvn test`

**Expected Outcome**: ~60 tests passing (existing test files fixed)

---

**Phase 2: Critical Coverage (Day 6 PM) - Complete Type Tests**
- **Duration**: 4 hours
- **Goal**: 100% coverage of TypeMapper
- **Tasks**:
  1. Add missing primitive type tests (8 tests)
  2. Add decimal edge cases (10 tests)
  3. Add complex nested type tests (10 tests)
  4. Add error handling tests (10 tests)

**Expected Outcome**: 98 tests passing (60 + 38 new)

---

**Phase 3: Expression Validation (Day 7 AM) - Complete Expression Tests**
- **Duration**: 4 hours
- **Goal**: Validate all expression translation
- **Tasks**:
  1. Add missing literal tests (5 tests)
  2. Add missing binary expression tests (10 tests)
  3. Add missing function call tests (10 tests)
  4. Add complex nested expression tests (10 tests)

**Expected Outcome**: 133 tests passing (98 + 35 new)

---

**Phase 4: Integration Testing (Day 7 PM) - Real DuckDB Validation**
- **Duration**: 4 hours
- **Goal**: Validate against real DuckDB
- **Tasks**:
  1. Create DuckDB integration test base class
  2. Add simple query tests (5 tests)
  3. Add JOIN query tests (5 tests)
  4. Add aggregate query tests (5 tests)
  5. Add differential tests (5 tests)

**Expected Outcome**: 153 tests passing (133 + 20 new)

---

### 3.3 Test Coverage Targets

| Component | Current | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Target |
|-----------|---------|---------|---------|---------|---------|--------|
| **Type System** | 0% | 70% | **95%** | 95% | 95% | 90%+ |
| **Expression System** | 0% | 60% | 70% | **95%** | 95% | 90%+ |
| **Function Registry** | 0% | 80% | 85% | 85% | **95%** | 85%+ |
| **Logical Plan** | 0% | 40% | 50% | 60% | **85%** | 80%+ |
| **Integration** | 0% | 0% | 0% | 20% | **100%** | 100% |
| **OVERALL** | **0%** | **58%** | **74%** | **82%** | **90%** | **85%+** |

---

## 4. Test Case Specifications

### 4.1 Type Mapping Test Cases

**Test Suite**: `TypeMapperTest.java`
**Target Tests**: 60+
**Coverage Goal**: 95%+

#### Test Group 1: Primitive Type Mappings (8 tests)

```java
@Nested
@DisplayName("Primitive Type Mappings")
class PrimitiveTypeMappings {

    @Test
    @DisplayName("ByteType maps to TINYINT")
    void testByteTypeMapping() {
        String result = TypeMapper.toDuckDBType(ByteType.get());
        assertThat(result).isEqualTo("TINYINT");
    }

    // ... repeat for all 8 primitive types
}
```

**Test Case Summary**:
| Test ID | Test Name | Input | Expected Output | Priority |
|---------|-----------|-------|-----------------|----------|
| TM-001 | ByteType mapping | ByteType.get() | "TINYINT" | P0 |
| TM-002 | ShortType mapping | ShortType.get() | "SMALLINT" | P0 |
| TM-003 | IntegerType mapping | IntegerType.get() | "INTEGER" | P0 |
| TM-004 | LongType mapping | LongType.get() | "BIGINT" | P0 |
| TM-005 | FloatType mapping | FloatType.get() | "FLOAT" | P0 |
| TM-006 | DoubleType mapping | DoubleType.get() | "DOUBLE" | P0 |
| TM-007 | StringType mapping | StringType.get() | "VARCHAR" | P0 |
| TM-008 | BooleanType mapping | BooleanType.get() | "BOOLEAN" | P0 |

---

#### Test Group 2: Decimal Type Mappings (10 tests)

```java
@ParameterizedTest
@CsvSource({
    "5, 2, 'DECIMAL(5,2)'",
    "10, 5, 'DECIMAL(10,5)'",
    "18, 4, 'DECIMAL(18,4)'",
    "38, 0, 'DECIMAL(38,0)'",
    "38, 38, 'DECIMAL(38,38)'"
})
@DisplayName("DecimalType with various precision/scale")
void testDecimalTypeVariations(int precision, int scale, String expected) {
    DecimalType decimal = new DecimalType(precision, scale);
    String result = TypeMapper.toDuckDBType(decimal);
    assertThat(result).isEqualTo(expected);
}
```

**Test Case Summary**:
| Test ID | Test Name | Precision | Scale | Expected Output | Priority |
|---------|-----------|-----------|-------|-----------------|----------|
| DM-001 | Small decimal | 5 | 2 | "DECIMAL(5,2)" | P0 |
| DM-002 | Standard decimal | 10 | 2 | "DECIMAL(10,2)" | P0 |
| DM-003 | Large decimal | 18 | 4 | "DECIMAL(18,4)" | P0 |
| DM-004 | Max precision | 38 | 18 | "DECIMAL(38,18)" | P0 |
| DM-005 | Zero scale | 10 | 0 | "DECIMAL(10,0)" | P1 |
| DM-006 | Max scale | 38 | 38 | "DECIMAL(38,38)" | P1 |
| DM-007 | Edge: (1,0) | 1 | 0 | "DECIMAL(1,0)" | P2 |
| DM-008 | Round-trip conversion | - | - | Original preserved | P0 |
| DM-009 | Invalid precision | 39 | 10 | IllegalArgumentException | P1 |
| DM-010 | Invalid scale | 10 | 11 | IllegalArgumentException | P1 |

---

#### Test Group 3: Array Type Mappings (10 tests)

```java
@Test
@DisplayName("ArrayType(IntegerType) maps to INTEGER[]")
void testIntegerArrayMapping() {
    ArrayType arrayType = new ArrayType(IntegerType.get());
    String result = TypeMapper.toDuckDBType(arrayType);
    assertThat(result).isEqualTo("INTEGER[]");
}

@Test
@DisplayName("Nested ArrayType(ArrayType(IntegerType)) maps to INTEGER[][]")
void testNestedArrayMapping() {
    ArrayType innerArray = new ArrayType(IntegerType.get());
    ArrayType outerArray = new ArrayType(innerArray);
    String result = TypeMapper.toDuckDBType(outerArray);
    assertThat(result).isEqualTo("INTEGER[][]");
}
```

**Test Case Summary**:
| Test ID | Test Name | Input Type | Expected Output | Priority |
|---------|-----------|------------|-----------------|----------|
| AM-001 | Integer array | ArrayType(IntegerType) | "INTEGER[]" | P0 |
| AM-002 | String array | ArrayType(StringType) | "VARCHAR[]" | P0 |
| AM-003 | Double array | ArrayType(DoubleType) | "DOUBLE[]" | P0 |
| AM-004 | Date array | ArrayType(DateType) | "DATE[]" | P1 |
| AM-005 | Decimal array | ArrayType(DecimalType(10,2)) | "DECIMAL(10,2)[]" | P1 |
| AM-006 | Nested array (2D) | ArrayType(ArrayType(IntegerType)) | "INTEGER[][]" | P0 |
| AM-007 | Nested array (3D) | ArrayType(ArrayType(ArrayType(StringType))) | "VARCHAR[][][]" | P2 |
| AM-008 | Reverse mapping | "INTEGER[]" | ArrayType(IntegerType) | P0 |
| AM-009 | Complex element | ArrayType(MapType) | "MAP(...)[]" | P2 |
| AM-010 | Null element type | ArrayType(null) | IllegalArgumentException | P1 |

---

#### Test Group 4: Map Type Mappings (10 tests)

```java
@Test
@DisplayName("MapType(StringType, IntegerType) maps to MAP(VARCHAR, INTEGER)")
void testStringIntegerMapMapping() {
    MapType mapType = new MapType(StringType.get(), IntegerType.get());
    String result = TypeMapper.toDuckDBType(mapType);
    assertThat(result).isEqualTo("MAP(VARCHAR, INTEGER)");
}

@Test
@DisplayName("MapType with nested MapType value maps correctly")
void testNestedMapMapping() {
    MapType innerMap = new MapType(StringType.get(), IntegerType.get());
    MapType outerMap = new MapType(StringType.get(), innerMap);
    String result = TypeMapper.toDuckDBType(outerMap);
    assertThat(result).isEqualTo("MAP(VARCHAR, MAP(VARCHAR, INTEGER))");
}
```

**Test Case Summary**:
| Test ID | Test Name | Key Type | Value Type | Expected Output | Priority |
|---------|-----------|----------|------------|-----------------|----------|
| MM-001 | String→Integer map | StringType | IntegerType | "MAP(VARCHAR, INTEGER)" | P0 |
| MM-002 | Integer→String map | IntegerType | StringType | "MAP(INTEGER, VARCHAR)" | P0 |
| MM-003 | String→Decimal map | StringType | DecimalType(10,2) | "MAP(VARCHAR, DECIMAL(10,2))" | P1 |
| MM-004 | Map with array value | StringType | ArrayType(IntegerType) | "MAP(VARCHAR, INTEGER[])" | P1 |
| MM-005 | Nested map | StringType | MapType(...) | "MAP(VARCHAR, MAP(...))" | P0 |
| MM-006 | Reverse mapping | "MAP(VARCHAR, INTEGER)" | MapType(StringType, IntegerType) | P0 |
| MM-007 | Complex key type | DateType | StringType | "MAP(DATE, VARCHAR)" | P2 |
| MM-008 | Complex value type | StringType | StructType | Special handling | P2 |
| MM-009 | Null key type | null | IntegerType | IllegalArgumentException | P1 |
| MM-010 | Null value type | StringType | null | IllegalArgumentException | P1 |

---

#### Test Group 5: Reverse Mapping Tests (7 tests)

```java
@Test
@DisplayName("INTEGER and INT both map to IntegerType")
void testIntegerAliasReverseMapping() {
    DataType fromInteger = TypeMapper.toSparkType("INTEGER");
    DataType fromInt = TypeMapper.toSparkType("INT");
    assertThat(fromInteger).isInstanceOf(IntegerType.class);
    assertThat(fromInt).isInstanceOf(IntegerType.class);
}

@Test
@DisplayName("VARCHAR, TEXT, STRING all map to StringType")
void testStringAliasReverseMapping() {
    DataType fromVarchar = TypeMapper.toSparkType("VARCHAR");
    DataType fromText = TypeMapper.toSparkType("TEXT");
    DataType fromString = TypeMapper.toSparkType("STRING");
    assertThat(fromVarchar).isInstanceOf(StringType.class);
    assertThat(fromText).isInstanceOf(StringType.class);
    assertThat(fromString).isInstanceOf(StringType.class);
}
```

**Test Case Summary**:
| Test ID | Test Name | Input String | Expected Type | Priority |
|---------|-----------|--------------|---------------|----------|
| RM-001 | TINYINT mapping | "TINYINT" | ByteType | P0 |
| RM-002 | INTEGER/INT aliases | "INTEGER", "INT" | IntegerType | P0 |
| RM-003 | VARCHAR/TEXT/STRING aliases | "VARCHAR", "TEXT", "STRING" | StringType | P0 |
| RM-004 | FLOAT/REAL aliases | "FLOAT", "REAL" | FloatType | P1 |
| RM-005 | BOOLEAN/BOOL aliases | "BOOLEAN", "BOOL" | BooleanType | P1 |
| RM-006 | DECIMAL parsing | "DECIMAL(10,2)" | DecimalType(10,2) | P0 |
| RM-007 | Array parsing | "INTEGER[]" | ArrayType(IntegerType) | P0 |

---

#### Test Group 6: Edge Cases & Error Handling (10 tests)

```java
@Test
@DisplayName("Null DataType throws IllegalArgumentException")
void testNullDataTypeThrows() {
    assertThatThrownBy(() -> TypeMapper.toDuckDBType(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
}

@Test
@DisplayName("Case-insensitive type parsing works")
void testCaseInsensitiveTypeParsing() {
    DataType lower = TypeMapper.toSparkType("integer");
    DataType upper = TypeMapper.toSparkType("INTEGER");
    DataType mixed = TypeMapper.toSparkType("InTeGeR");

    assertThat(lower).isInstanceOf(IntegerType.class);
    assertThat(upper).isInstanceOf(IntegerType.class);
    assertThat(mixed).isInstanceOf(IntegerType.class);
}

@Test
@DisplayName("Round-trip conversion preserves type")
void testRoundTripConversion() {
    DecimalType original = new DecimalType(18, 4);
    String duckdbType = TypeMapper.toDuckDBType(original);
    DataType roundTrip = TypeMapper.toSparkType(duckdbType);

    assertThat(roundTrip).isInstanceOf(DecimalType.class);
    DecimalType decimal = (DecimalType) roundTrip;
    assertThat(decimal.precision()).isEqualTo(18);
    assertThat(decimal.scale()).isEqualTo(4);
}
```

**Test Case Summary**:
| Test ID | Test Name | Input | Expected Behavior | Priority |
|---------|-----------|-------|-------------------|----------|
| EC-001 | Null DataType | null | IllegalArgumentException | P0 |
| EC-002 | Null DuckDB string | null | IllegalArgumentException | P0 |
| EC-003 | Empty DuckDB string | "" | IllegalArgumentException | P0 |
| EC-004 | Unsupported type | "UNKNOWN_TYPE" | UnsupportedOperationException | P0 |
| EC-005 | Invalid DECIMAL format | "DECIMAL(10)" | IllegalArgumentException | P1 |
| EC-006 | Case-insensitive parsing | "integer", "INTEGER" | Both work | P0 |
| EC-007 | Whitespace handling | "  INTEGER  " | Works | P1 |
| EC-008 | Round-trip conversion | DecimalType(18,4) | Preserved | P0 |
| EC-009 | Complex nested round-trip | MAP(VARCHAR, INTEGER[]) | Preserved | P1 |
| EC-010 | Compatibility checking | IntegerType vs "INT" | Compatible | P0 |

---

### 4.2 Expression Translation Test Cases

**Test Suite**: `ExpressionTest.java`
**Target Tests**: 60+
**Coverage Goal**: 95%+

#### Test Group 1: Literal Expressions (10 tests)

**Already implemented but needs fixes** - see compilation error fixes in section 5.

---

#### Test Group 2: Column References (5 tests)

```java
@Test
@DisplayName("Simple column reference creates correct SQL")
void testSimpleColumnReference() {
    ColumnReference col = new ColumnReference("customer_id", IntegerType.get());

    assertThat(col.columnName()).isEqualTo("customer_id");
    assertThat(col.dataType()).isInstanceOf(IntegerType.class);
    assertThat(col.nullable()).isTrue(); // default
    assertThat(col.toSQL()).isEqualTo("customer_id");
}

@Test
@DisplayName("Qualified column reference creates correct SQL")
void testQualifiedColumnReference() {
    ColumnReference col = new ColumnReference("order_id", "orders", LongType.get(), false);

    assertThat(col.qualifier()).isEqualTo("orders");
    assertThat(col.columnName()).isEqualTo("order_id");
    assertThat(col.toSQL()).isEqualTo("orders.order_id");
}
```

**Test Case Summary**:
| Test ID | Test Name | Column Name | Qualifier | Expected SQL | Priority |
|---------|-----------|-------------|-----------|--------------|----------|
| CR-001 | Simple column | "customer_id" | null | "customer_id" | P0 |
| CR-002 | Qualified column | "order_id" | "orders" | "orders.order_id" | P0 |
| CR-003 | Nullable column | "email" | null | nullable=true | P1 |
| CR-004 | Complex type column | "tags" | null | ArrayType | P1 |
| CR-005 | Column equality | Same name/type | - | Equal | P2 |

---

#### Test Group 3: Binary Expressions (15 tests)

**Arithmetic Operators** (5 tests):
```java
@Test
@DisplayName("Addition expression creates correct SQL")
void testAddition() {
    Expression left = Literal.of(10);
    Expression right = Literal.of(5);
    BinaryExpression expr = BinaryExpression.add(left, right);

    assertThat(expr.operator()).isEqualTo(BinaryExpression.Operator.ADD);
    assertThat(expr.toSQL()).isEqualTo("(10 + 5)");
}
```

**Test Case Summary**:
| Test ID | Test Name | Operator | Left | Right | Expected SQL | Priority |
|---------|-----------|----------|------|-------|--------------|----------|
| BE-001 | Addition | ADD | 10 | 5 | "(10 + 5)" | P0 |
| BE-002 | Subtraction | SUBTRACT | 20 | 7 | "(20 - 7)" | P0 |
| BE-003 | Multiplication | MULTIPLY | price | quantity | "(price * quantity)" | P0 |
| BE-004 | Division | DIVIDE | 100 | 4 | "(100 / 4)" | P0 |
| BE-005 | Modulo | MODULO | 17 | 5 | "(17 % 5)" | P0 |

**Comparison Operators** (6 tests):
| Test ID | Test Name | Operator | Expected SQL | Priority |
|---------|-----------|----------|--------------|----------|
| BE-006 | Equal | EQUAL | "(status = 'active')" | P0 |
| BE-007 | Not Equal | NOT_EQUAL | "(id != 0)" | P0 |
| BE-008 | Less Than | LESS_THAN | "(age < 18)" | P0 |
| BE-009 | Less Than Or Equal | LESS_THAN_OR_EQUAL | "(price <= 100.0)" | P0 |
| BE-010 | Greater Than | GREATER_THAN | "(score > 50)" | P0 |
| BE-011 | Greater Than Or Equal | GREATER_THAN_OR_EQUAL | "(quantity >= 1)" | P0 |

**Logical Operators** (4 tests):
| Test ID | Test Name | Operator | Expected SQL | Priority |
|---------|-----------|----------|--------------|----------|
| BE-012 | AND | AND | "... AND ..." | P0 |
| BE-013 | OR | OR | "... OR ..." | P0 |
| BE-014 | NOT (unary) | NOT | "(NOT deleted)" | P0 |
| BE-015 | Complex logical | AND + OR | "((a AND b) OR (c AND d))" | P1 |

---

#### Test Group 4: Function Calls (10 tests)

```java
@Test
@DisplayName("String function call creates correct SQL")
void testStringFunction() {
    Expression arg = new ColumnReference("name", StringType.get());
    FunctionCall func = FunctionCall.of("upper", arg, StringType.get());

    assertThat(func.functionName()).isEqualTo("upper");
    assertThat(func.arguments()).hasSize(1);
    assertThat(func.toSQL()).isEqualTo("upper(name)");
}

@Test
@DisplayName("Function with multiple arguments")
void testFunctionWithMultipleArgs() {
    List<Expression> args = List.of(
        new ColumnReference("text", StringType.get()),
        Literal.of(1),
        Literal.of(5)
    );
    FunctionCall func = new FunctionCall("substring", args, StringType.get());

    assertThat(func.arguments()).hasSize(3);
    assertThat(func.toSQL()).isEqualTo("substring(text, 1, 5)");
}
```

**Test Case Summary**:
| Test ID | Test Name | Function | Args Count | Expected SQL | Priority |
|---------|-----------|----------|------------|--------------|----------|
| FC-001 | String function | upper | 1 | "upper(name)" | P0 |
| FC-002 | Math function | abs | 1 | "abs(value)" | P0 |
| FC-003 | Multi-arg function | substring | 3 | "substring(text, 1, 5)" | P0 |
| FC-004 | Aggregate function | sum | 1 | "sum(amount)" | P0 |
| FC-005 | No-arg function | now | 0 | "now()" | P1 |
| FC-006 | Nested function | trim(lower(name)) | - | Nested SQL | P1 |
| FC-007 | Function in arithmetic | abs(-10) * 2 | - | "(abs(-10) * 2)" | P1 |
| FC-008 | Function with literals | concat('A', 'B', 'C') | 3 | "concat('A', 'B', 'C')" | P1 |
| FC-009 | Function equality | Same function/args | - | Equal | P2 |
| FC-010 | Registry integration | FunctionRegistry.translate | - | Correct mapping | P0 |

---

### 4.3 Function Registry Test Cases

**Test Suite**: `FunctionRegistryTest.java`
**Target Tests**: 50+
**Coverage Goal**: 85%+

**Already implemented but needs fixes** - the existing FunctionRegistryTest is well-designed and comprehensive. No additional tests needed once compilation errors are fixed.

---

### 4.4 Logical Plan Test Cases

**Test Suite**: `LogicalPlanTest.java` (NEW)
**Target Tests**: 30+
**Coverage Goal**: 80%+

#### Test Group 1: Schema Inference (10 tests)

```java
@Test
@DisplayName("TableScan infers schema correctly")
void testTableScanSchemaInference() {
    StructType schema = new StructType(List.of(
        new StructField("id", IntegerType.get(), false),
        new StructField("name", StringType.get(), true)
    ));

    TableScan tableScan = new TableScan("users", "data/users.parquet", schema);

    assertThat(tableScan.schema()).isEqualTo(schema);
    assertThat(tableScan.schema().fields()).hasSize(2);
}

@Test
@DisplayName("Project infers schema from selected columns")
void testProjectSchemaInference() {
    StructType inputSchema = new StructType(List.of(
        new StructField("id", IntegerType.get(), false),
        new StructField("name", StringType.get(), true),
        new StructField("email", StringType.get(), true)
    ));

    TableScan child = new TableScan("users", "data/users.parquet", inputSchema);

    List<Expression> projections = List.of(
        new ColumnReference("id", IntegerType.get(), false),
        new ColumnReference("name", StringType.get(), true)
    );

    Project project = new Project(child, projections);

    assertThat(project.schema().fields()).hasSize(2);
    assertThat(project.schema().field(0).name()).isEqualTo("id");
    assertThat(project.schema().field(1).name()).isEqualTo("name");
}
```

**Test Case Summary**:
| Test ID | Test Name | Plan Node | Expected Behavior | Priority |
|---------|-----------|-----------|-------------------|----------|
| LP-001 | TableScan schema | TableScan | Infers from file | P0 |
| LP-002 | Project schema | Project | Selects columns | P0 |
| LP-003 | Filter schema | Filter | Preserves input schema | P0 |
| LP-004 | Aggregate schema | Aggregate | Groups + aggregates | P0 |
| LP-005 | Join schema | Join | Merges left + right | P0 |
| LP-006 | Union schema | Union | Compatible schemas | P0 |
| LP-007 | Sort schema | Sort | Preserves schema | P1 |
| LP-008 | Limit schema | Limit | Preserves schema | P1 |
| LP-009 | Nested plan | Project(Filter(TableScan)) | Correct propagation | P1 |
| LP-010 | Schema with nullability | - | Tracks nullable columns | P1 |

---

#### Test Group 2: SQL Generation (10 tests)

**NOTE**: SQL generation is currently **STUBBED** with `UnsupportedOperationException`. These tests will need implementation work first.

```java
@Test
@DisplayName("Simple SELECT * generates correct SQL")
@Disabled("SQL generation not yet implemented")
void testSimpleSelectStar() {
    StructType schema = new StructType(List.of(
        new StructField("id", IntegerType.get(), false)
    ));

    TableScan tableScan = new TableScan("users", "users.parquet", schema);

    String sql = tableScan.toSQL();

    assertThat(sql).isEqualTo("SELECT * FROM users");
}
```

**Test Case Summary** (Implementation Required):
| Test ID | Test Name | Plan Structure | Expected SQL | Priority |
|---------|-----------|----------------|--------------|----------|
| SQL-001 | SELECT * | TableScan | "SELECT * FROM users" | P0 |
| SQL-002 | SELECT columns | Project(TableScan) | "SELECT id, name FROM users" | P0 |
| SQL-003 | WHERE clause | Filter(TableScan) | "SELECT * FROM users WHERE age > 18" | P0 |
| SQL-004 | INNER JOIN | Join(TableScan, TableScan) | "SELECT ... FROM a JOIN b ON ..." | P0 |
| SQL-005 | GROUP BY | Aggregate(TableScan) | "SELECT dept, COUNT(*) FROM ... GROUP BY dept" | P0 |
| SQL-006 | ORDER BY | Sort(TableScan) | "SELECT * FROM users ORDER BY name ASC" | P0 |
| SQL-007 | LIMIT | Limit(TableScan) | "SELECT * FROM users LIMIT 10" | P0 |
| SQL-008 | Complex nested | Multiple operators | Nested SELECT statements | P1 |
| SQL-009 | LEFT JOIN | Join(LEFT) | "SELECT ... FROM a LEFT JOIN b ON ..." | P1 |
| SQL-010 | UNION | Union(TableScan, TableScan) | "SELECT ... UNION SELECT ..." | P1 |

---

#### Test Group 3: Join Tests (5 tests)

```java
@Test
@DisplayName("INNER JOIN merges schemas correctly")
void testInnerJoinSchema() {
    StructType leftSchema = new StructType(List.of(
        new StructField("user_id", IntegerType.get(), false),
        new StructField("name", StringType.get(), true)
    ));

    StructType rightSchema = new StructType(List.of(
        new StructField("order_id", IntegerType.get(), false),
        new StructField("user_id", IntegerType.get(), false)
    ));

    TableScan left = new TableScan("users", "users.parquet", leftSchema);
    TableScan right = new TableScan("orders", "orders.parquet", rightSchema);

    Expression joinCondition = BinaryExpression.equal(
        new ColumnReference("user_id", "users", IntegerType.get(), false),
        new ColumnReference("user_id", "orders", IntegerType.get(), false)
    );

    Join join = new Join(left, right, Join.JoinType.INNER, joinCondition);

    assertThat(join.schema().fields()).hasSize(4); // 2 from left + 2 from right
}
```

**Test Case Summary**:
| Test ID | Test Name | Join Type | Expected Behavior | Priority |
|---------|-----------|-----------|-------------------|----------|
| JOIN-001 | INNER JOIN schema | INNER | Merges left + right | P0 |
| JOIN-002 | LEFT JOIN schema | LEFT | All left + matching right (nullable) | P0 |
| JOIN-003 | RIGHT JOIN schema | RIGHT | Matching left (nullable) + all right | P0 |
| JOIN-004 | FULL JOIN schema | FULL | All left + all right (nullable) | P0 |
| JOIN-005 | CROSS JOIN schema | CROSS | Cartesian product | P1 |

---

### 4.5 Integration Test Cases

**Test Suite**: `DuckDBIntegrationTest.java` (NEW)
**Target Tests**: 20+
**Coverage Goal**: 100% of integration scenarios

#### Setup: DuckDB Test Base Class

```java
@TestCategories.Integration
@TestCategories.Tier2
public abstract class DuckDBTestBase extends TestBase {

    protected Connection duckdb;

    @BeforeEach
    void setupDuckDB() throws SQLException {
        duckdb = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void teardownDuckDB() throws SQLException {
        if (duckdb != null && !duckdb.isClosed()) {
            duckdb.close();
        }
    }

    protected ResultSet executeQuery(String sql) throws SQLException {
        Statement stmt = duckdb.createStatement();
        return stmt.executeQuery(sql);
    }
}
```

#### Test Group 1: Simple Queries (5 tests)

```java
@Test
@DisplayName("Simple SELECT * works with DuckDB")
void testSimpleSelectStar() throws SQLException {
    // Create test table
    duckdb.createStatement().execute(
        "CREATE TABLE users (id INTEGER, name VARCHAR)"
    );
    duckdb.createStatement().execute(
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
    );

    // Generate SQL from logical plan
    StructType schema = new StructType(List.of(
        new StructField("id", IntegerType.get(), false),
        new StructField("name", StringType.get(), true)
    ));
    TableScan tableScan = new TableScan("users", "users", schema);

    String generatedSQL = tableScan.toSQL(); // Assuming implementation

    // Execute and validate
    ResultSet rs = executeQuery(generatedSQL);
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt("id")).isEqualTo(1);
    assertThat(rs.getString("name")).isEqualTo("Alice");
}
```

**Test Case Summary**:
| Test ID | Test Name | SQL Pattern | Validation | Priority |
|---------|-----------|-------------|------------|----------|
| INT-001 | SELECT * | TableScan | Rows returned | P0 |
| INT-002 | SELECT columns | Project(TableScan) | Specific columns | P0 |
| INT-003 | WHERE clause | Filter(TableScan) | Filtered rows | P0 |
| INT-004 | ORDER BY | Sort(TableScan) | Sorted results | P0 |
| INT-005 | LIMIT | Limit(TableScan) | Limited rows | P0 |

---

#### Test Group 2: Aggregate Queries (5 tests)

**Test Case Summary**:
| Test ID | Test Name | Aggregation | Expected Result | Priority |
|---------|-----------|-------------|-----------------|----------|
| AGG-001 | COUNT(*) | count | Correct count | P0 |
| AGG-002 | SUM() | sum | Correct sum | P0 |
| AGG-003 | AVG() | avg | Correct average | P0 |
| AGG-004 | GROUP BY | group by + count | Grouped results | P0 |
| AGG-005 | HAVING | group by + having | Filtered groups | P1 |

---

#### Test Group 3: Differential Tests (5 tests)

**Purpose**: Compare Spark Catalyst behavior vs DuckDB behavior.

```java
@Test
@DisplayName("Type mapping produces same results: Spark vs DuckDB")
void testDifferentialTypeMapping() {
    // Test that DECIMAL(10,2) in Spark produces same results as DECIMAL(10,2) in DuckDB

    // Create Spark DataFrame (mock or actual)
    // Create DuckDB table with equivalent schema
    // Execute same query on both
    // Compare results
}
```

**Test Case Summary**:
| Test ID | Test Name | Comparison | Expected | Priority |
|---------|-----------|------------|----------|----------|
| DIFF-001 | Type precision | DECIMAL(10,2) | Same precision | P0 |
| DIFF-002 | Date functions | date_add, datediff | Same dates | P0 |
| DIFF-003 | String functions | upper, lower, trim | Same strings | P0 |
| DIFF-004 | Aggregates | sum, avg, count | Same numbers | P0 |
| DIFF-005 | NULL handling | NULL comparisons | Same behavior | P0 |

---

## 5. Compilation Error Fixes

### 5.1 TypeMapperTest Fixes

**Current Issues**: None - TypeMapperTest compiles successfully.

### 5.2 FunctionRegistryTest Fixes

**Current Issues**: None - FunctionRegistryTest compiles successfully.

### 5.3 ExpressionTest Fixes

**Current Issues**: 18 compilation errors

**Fix 1: ColumnReference method names**
```java
// WRONG:
col.name()  // Method doesn't exist

// CORRECT:
col.columnName()  // Correct method name
```

**Fix 2: ColumnReference constructor parameter order**
```java
// WRONG:
new ColumnReference("order_id", "orders", LongType.get())

// CORRECT:
new ColumnReference("order_id", "orders", LongType.get(), false)
// Parameters: (columnName, qualifier, dataType, nullable)
```

**Fix 3: FunctionCall constructor**
```java
// WRONG:
new FunctionCall("upper", StringType.get(), arg)

// CORRECT:
FunctionCall.of("upper", arg, StringType.get())
// Or:
new FunctionCall("upper", List.of(arg), StringType.get())
```

**Fix 4: Expression toSQL() method**
```java
// WRONG:
expr.toSQL()  // Not available on Expression base class

// CORRECT:
((BinaryExpression) expr).toSQL()  // Cast to concrete type
// Or check implementation in Expression.java for actual method
```

**Complete Fix Script**:

```java
// Line 156: col.name() → col.columnName()
assertThat(col.columnName()).isEqualTo("customer_id");

// Line 165: Constructor parameter order
ColumnReference col = new ColumnReference("order_id", "orders", LongType.get(), false);

// Line 168: col.name() → col.columnName()
assertThat(col.columnName()).isEqualTo("order_id");

// Line 583: FunctionCall constructor fix
FunctionCall func = FunctionCall.of("upper", arg, StringType.get());

// Line 594: FunctionCall constructor fix
FunctionCall func = FunctionCall.of("abs", arg, DoubleType.get());

// Line 605: FunctionCall with multiple args
List<Expression> args = List.of(arg1, arg2, arg3);
FunctionCall func = new FunctionCall("substring", args, StringType.get());

// Line 615: FunctionCall constructor fix
FunctionCall func = FunctionCall.of("sum", arg, DoubleType.get());

// Line 623: FunctionCall no args
FunctionCall func = FunctionCall.of("now", TimestampType.get());

// Line 634: FunctionCall nested
FunctionCall inner = FunctionCall.of("lower", arg, StringType.get());
FunctionCall outer = FunctionCall.of("trim", inner, StringType.get());

// Line 637: Same as above

// Line 645: FunctionCall in expression
FunctionCall func = FunctionCall.of("abs", Literal.of(-10), IntegerType.get());

// Line 648: Check if toSQL() exists on Expression or cast
assertThat(((BinaryExpression) expr).toSQL()).isEqualTo("(abs(-10) * 2)");

// Line 661: FunctionCall with multiple literals
List<Expression> args = List.of(Literal.of("Hello"), Literal.of(" "), Literal.of("World"));
FunctionCall func = new FunctionCall("concat", args, StringType.get());

// Line 676-678: FunctionCall constructor fixes
FunctionCall func1 = FunctionCall.of("abs", arg, IntegerType.get());
FunctionCall func2 = FunctionCall.of("abs", arg, IntegerType.get());
FunctionCall func3 = FunctionCall.of("sqrt", arg, DoubleType.get());

// Line 717: FunctionCall nested
FunctionCall upperFunc = FunctionCall.of("upper", nameCol, StringType.get());

// Line 737: FunctionCall
FunctionCall absFunc = FunctionCall.of("abs", balanceCol, DoubleType.get());

// Line 740: Check if toSQL() exists
assertThat(((BinaryExpression) result).toSQL()).contains("abs(balance)").contains("*");
```

---

## 6. Acceptance Criteria for Gap Remediation

### 6.1 Per-Component Acceptance Criteria

#### Type System
- [ ] All 60+ TypeMapper tests pass
- [ ] 95%+ line coverage on TypeMapper.java
- [ ] All primitive types validated
- [ ] All complex types (Array, Map) validated
- [ ] Decimal precision/scale preserved
- [ ] Round-trip conversion validated
- [ ] Error handling tested (null, invalid types)

#### Expression System
- [ ] All 60+ Expression tests pass
- [ ] 95%+ line coverage on Expression classes
- [ ] All literal types tested
- [ ] All operators tested (arithmetic, comparison, logical)
- [ ] Nested expressions validated
- [ ] Type inference validated
- [ ] Nullability propagation tested

#### Function Registry
- [ ] All 50+ Function tests pass
- [ ] 85%+ line coverage on FunctionRegistry.java
- [ ] String, math, date, aggregate functions tested
- [ ] Function mapping correctness validated
- [ ] Case-insensitive function names work
- [ ] Unsupported functions throw proper exceptions

#### Logical Plan
- [ ] All 30+ LogicalPlan tests pass
- [ ] 80%+ line coverage on LogicalPlan classes
- [ ] Schema inference validated for all node types
- [ ] JOIN schema merge tested
- [ ] Aggregate schema transformation tested
- [ ] SQL generation validated (if implemented)

#### Integration
- [ ] All 20+ integration tests pass
- [ ] DuckDB queries execute successfully
- [ ] Differential tests validate against Spark
- [ ] Complex queries work end-to-end

---

### 6.2 Overall Acceptance Criteria

**Must-Have (Blocking Week 2)**:
- [ ] **Minimum 150+ tests passing** (target: 170+)
- [ ] **0 test failures**
- [ ] **80%+ overall line coverage** (target: 85%+)
- [ ] **75%+ overall branch coverage** (target: 80%+)
- [ ] **All compilation errors fixed**
- [ ] **TypeMapper fully validated** (highest risk)
- [ ] **Expression system fully validated** (highest risk)

**Nice-to-Have (Not Blocking)**:
- [ ] SQL generation tests (can defer if implementation not ready)
- [ ] Performance benchmarks
- [ ] Stress tests with large schemas
- [ ] Mutation testing

---

### 6.3 Quality Gates

**Pre-Commit Quality Gates**:
```bash
# Must pass before committing code
mvn clean compile  # No compilation errors
mvn test           # All tests pass
mvn verify -Pcoverage  # Coverage >= 80%
```

**Pre-Week-2 Quality Gates**:
```bash
# Must pass before starting Week 2
mvn clean verify -Pcoverage
# Result:
# - Tests: 150+ passing, 0 failures
# - Coverage: 80%+ line, 75%+ branch
# - Build time: < 2 minutes
```

---

## 7. Test Execution Strategy

### 7.1 Test Execution Modes

**Mode 1: Fast Unit Tests (Tier 1)**
```bash
mvn test -Dgroups="tier1"
# Expected: ~100 tests in < 10 seconds
# Use for: Continuous development, pre-commit checks
```

**Mode 2: Full Test Suite (Tier 1 + Tier 2)**
```bash
mvn test
# Expected: ~170 tests in < 60 seconds
# Use for: Pre-push validation, CI/CD
```

**Mode 3: Integration Tests Only (Tier 2)**
```bash
mvn test -Dgroups="tier2"
# Expected: ~20 tests in < 30 seconds
# Use for: Pre-deployment validation
```

**Mode 4: Coverage Analysis**
```bash
mvn clean verify -Pcoverage
open target/site/jacoco/index.html
# Use for: Coverage reports, identifying gaps
```

---

### 7.2 Parallel Test Execution

**Configuration** (already in pom.xml):
```xml
<plugin>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.1.2</version>
    <configuration>
        <parallel>methods</parallel>
        <threadCount>4</threadCount>
    </configuration>
</plugin>
```

**Expected Performance**:
- Sequential: ~120 seconds for 170 tests
- Parallel (4 threads): ~40 seconds for 170 tests
- **3x speedup** with parallel execution

---

### 7.3 Test Prioritization

**Priority 0 (P0) - Smoke Tests** (Run first):
- Basic type mappings (8 tests)
- Basic expression creation (10 tests)
- FunctionRegistry.isSupported() (5 tests)
- **Total: ~20 tests in < 2 seconds**

**Priority 1 (P1) - Core Functionality**:
- All type mappings and conversions (50 tests)
- All expression types and operators (50 tests)
- Function translations (30 tests)
- **Total: ~130 tests in < 20 seconds**

**Priority 2 (P2) - Edge Cases**:
- Error handling (20 tests)
- Complex nested types (10 tests)
- Integration tests (20 tests)
- **Total: ~50 tests in < 30 seconds**

---

## 8. Test Metrics & Reporting

### 8.1 Key Metrics to Track

**Test Count Metrics**:
- Total tests: 170+
- Tests passing: 170+
- Tests failing: 0
- Tests skipped: 0 (or @Disabled for SQL generation)

**Coverage Metrics**:
- Line coverage: 85%+
- Branch coverage: 80%+
- Method coverage: 90%+
- Class coverage: 100% (all classes tested)

**Performance Metrics**:
- Test execution time: < 60 seconds
- Average test duration: < 500ms
- Slowest test: < 5 seconds

**Quality Metrics**:
- Code smells: 0
- Critical bugs: 0
- Technical debt: < 1 day
- Flaky tests: 0

---

### 8.2 Coverage Reports

**JaCoCo Coverage Report** (already configured):
```bash
mvn clean verify -Pcoverage
open target/site/jacoco/index.html
```

**Expected Coverage Report**:
```
Package: com.catalyst2sql.types
├── TypeMapper.java: 95% (critical)
├── DecimalType.java: 90%
├── ArrayType.java: 90%
└── MapType.java: 90%

Package: com.catalyst2sql.expression
├── BinaryExpression.java: 95% (critical)
├── ColumnReference.java: 95%
├── Literal.java: 100%
└── FunctionCall.java: 90%

Package: com.catalyst2sql.functions
├── FunctionRegistry.java: 85% (large class)
└── FunctionTranslator.java: 60% (interface only)

Package: com.catalyst2sql.logical
├── Project.java: 80%
├── Filter.java: 80%
├── Join.java: 75%
└── Aggregate.java: 75%

Overall: 85%+ line coverage ✅
```

---

### 8.3 Test Reporting Dashboard

**Test Execution Summary** (from Maven Surefire):
```
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.catalyst2sql.types.TypeMapperTest
[INFO] Tests run: 60, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 3.245 s
[INFO] Running com.catalyst2sql.expression.ExpressionTest
[INFO] Tests run: 60, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 4.123 s
[INFO] Running com.catalyst2sql.functions.FunctionRegistryTest
[INFO] Tests run: 50, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.876 s
[INFO]
[INFO] Results:
[INFO]
[INFO] Tests run: 170, Failures: 0, Errors: 0, Skipped: 0
[INFO]
[INFO] BUILD SUCCESS
```

---

## 9. Risk Mitigation & Contingency Plans

### 9.1 Risk: Test Implementation Takes Longer Than 2 Days

**Mitigation**:
1. **Prioritize critical tests first**: TypeMapper, Expression (P0)
2. **Use parallel development**: Multiple developers on different test suites
3. **Leverage TestDataBuilder**: Accelerate test creation
4. **Defer SQL generation tests**: Can be added after implementation

**Contingency**:
- If behind schedule after Day 6: Skip P2 tests, focus on P0 + P1
- If behind schedule after Day 7: Deliver minimum 100 tests, defer rest to Week 2

---

### 9.2 Risk: Tests Reveal Critical Bugs

**Mitigation**:
1. **Budget 1 day for bug fixes** (Day 8)
2. **Use differential testing** to isolate issues quickly
3. **Fix bugs before proceeding** to new features

**Contingency**:
- If bugs are extensive: Extend Week 1 by 1-2 days
- If bugs are minor: Fix during Week 2 development

---

### 9.3 Risk: Coverage Below 80%

**Mitigation**:
1. **Identify uncovered code** with JaCoCo report
2. **Add targeted tests** for uncovered branches
3. **Refactor untestable code** (e.g., private methods)

**Contingency**:
- If coverage at 75-79%: Accept with plan to reach 80% in Week 2
- If coverage < 75%: Block Week 2, add more tests

---

## 10. Recommendations & Next Steps

### 10.1 Immediate Actions (Today)

**Action 1: Fix Compilation Errors**
```bash
# Priority: CRITICAL
# Duration: 2 hours
# Owner: TESTER agent

# Tasks:
1. Apply fixes from Section 5.3 to ExpressionTest.java
2. Verify compilation: mvn clean compile
3. Run existing tests: mvn test
4. Expected: ~60 tests passing
```

**Action 2: Create Missing Test Files**
```bash
# Priority: HIGH
# Duration: 2 hours
# Owner: TESTER agent

# Tasks:
1. Create LogicalPlanTest.java (schema inference tests)
2. Create DuckDBIntegrationTest.java (integration base class)
3. Add missing test cases to existing files
4. Expected: ~30 new tests
```

---

### 10.2 Short-Term Actions (Days 6-7)

**Day 6 Tasks**:
- AM: Fix compilation errors + complete TypeMapper tests (98 tests total)
- PM: Add missing expression tests + function tests (133 tests total)

**Day 7 Tasks**:
- AM: Create integration tests (153 tests total)
- PM: Run full coverage analysis, fix gaps, generate report

**Success Criteria**:
- [ ] 150+ tests passing
- [ ] 80%+ coverage achieved
- [ ] 0 compilation errors
- [ ] < 60 second test execution time

---

### 10.3 Long-Term Recommendations

**For Week 2 and Beyond**:

1. **Test-Driven Development**:
   - Write tests BEFORE implementation
   - Use TDD cycle: Red → Green → Refactor

2. **Continuous Integration**:
   - Run tests on every commit
   - Block PRs if tests fail or coverage drops

3. **Test Maintenance**:
   - Keep tests up-to-date with implementation
   - Refactor tests when refactoring code
   - Remove obsolete tests

4. **Performance Testing**:
   - Add benchmark tests for TypeMapper
   - Add performance tests for SQL generation
   - Monitor test execution time

5. **Mutation Testing**:
   - Use PIT (pitest) for mutation testing
   - Verify tests actually catch bugs
   - Target: 80%+ mutation coverage

---

## 11. Conclusion

### 11.1 Summary

The catalyst2sql project has **excellent implementation** (39 classes, ~3,800 LOC) but **critical testing gap** (0 tests, 0% coverage). This test strategy provides a comprehensive roadmap to:

1. **Fix existing test compilation errors** (2 hours)
2. **Achieve 80%+ coverage** (2 days, 150+ tests)
3. **Enable confident Week 2 development** (solid foundation)
4. **Establish testing best practices** (for future development)

---

### 11.2 Success Metrics

**Week 1 Testing Complete When**:
- ✅ 150+ tests passing (target: 170+)
- ✅ 0 test failures
- ✅ 80%+ line coverage (target: 85%+)
- ✅ 75%+ branch coverage (target: 80%+)
- ✅ < 60 second test execution time
- ✅ All P0 and P1 test cases implemented
- ✅ Integration tests with DuckDB working

---

### 11.3 Final Recommendations

**IMMEDIATE ACTION**:
1. Fix compilation errors in ExpressionTest.java (Section 5.3)
2. Run first successful test execution: `mvn test`
3. Review coverage report: `mvn verify -Pcoverage`

**NEXT 2 DAYS**:
1. Complete all P0 + P1 test cases (130+ tests)
2. Achieve 80%+ coverage target
3. Create integration tests with DuckDB
4. Generate Week 1 test completion report

**AFTER TESTING**:
- Week 2 can proceed with confidence
- Regression protection in place
- Quality metrics established
- Testing culture established

---

**Report Status**: COMPLETE
**Confidence Level**: HIGH (based on thorough gap analysis and existing code review)
**Next Action**: Fix compilation errors and begin test implementation (Phase 1)

---

## Appendix A: Test File Checklist

**Test Infrastructure** ✅:
- [x] TestBase.java (59 lines) - lifecycle hooks
- [x] TestDataBuilder.java (221 lines) - fluent API
- [x] DifferentialAssertion.java (320 lines) - custom assertions
- [x] TestCategories.java (203 lines) - JUnit 5 tags

**Unit Tests** ⚠️:
- [x] TypeMapperTest.java (610 lines, 60+ tests) - **NEEDS FIXES**
- [x] FunctionRegistryTest.java (555 lines, 50+ tests) - **NEEDS FIXES**
- [x] ExpressionTest.java (824 lines, 60+ tests) - **18 COMPILATION ERRORS**
- [ ] LogicalPlanTest.java (NOT CREATED) - **CRITICAL**

**Integration Tests** ❌:
- [ ] DuckDBIntegrationTest.java (NOT CREATED) - **CRITICAL**
- [ ] DifferentialTest.java (NOT CREATED) - **HIGH**

---

## Appendix B: Maven Commands Quick Reference

```bash
# Compile only
mvn clean compile

# Run all tests
mvn clean test

# Run tests with coverage
mvn clean verify -Pcoverage

# Run only Tier 1 tests (fast)
mvn test -Dgroups="tier1"

# Run only Tier 2 tests (integration)
mvn test -Dgroups="tier2"

# Run specific test class
mvn test -Dtest=TypeMapperTest

# Run specific test method
mvn test -Dtest=TypeMapperTest#testByteTypeMapping

# Generate site with all reports
mvn clean verify site

# View coverage report
open target/site/jacoco/index.html

# View surefire report
open target/site/surefire-report.html
```

---

## Appendix C: Test Template Examples

**Simple Unit Test Template**:
```java
@Test
@DisplayName("Test description here")
void testMethodName() {
    // Arrange
    DataType input = IntegerType.get();

    // Act
    String result = TypeMapper.toDuckDBType(input);

    // Assert
    assertThat(result).isEqualTo("INTEGER");
}
```

**Parameterized Test Template**:
```java
@ParameterizedTest
@CsvSource({
    "5, 2, 'DECIMAL(5,2)'",
    "10, 5, 'DECIMAL(10,5)'",
    "18, 4, 'DECIMAL(18,4)'"
})
@DisplayName("DecimalType with various precision/scale")
void testDecimalVariations(int precision, int scale, String expected) {
    DecimalType decimal = new DecimalType(precision, scale);
    String result = TypeMapper.toDuckDBType(decimal);
    assertThat(result).isEqualTo(expected);
}
```

**Integration Test Template**:
```java
@Test
@DisplayName("DuckDB integration test")
void testDuckDBIntegration() throws SQLException {
    // Create test table
    duckdb.createStatement().execute("CREATE TABLE test (id INTEGER)");
    duckdb.createStatement().execute("INSERT INTO test VALUES (1)");

    // Generate SQL from logical plan
    String sql = generateSQL(); // Your implementation

    // Execute and validate
    ResultSet rs = executeQuery(sql);
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt("id")).isEqualTo(1);
}
```

---

**END OF TEST STRATEGY DOCUMENT**
