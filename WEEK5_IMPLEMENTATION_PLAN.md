# Week 5 Implementation Plan - Advanced Aggregation & Window Functions

**Project**: catalyst2sql - Spark Catalyst to DuckDB SQL Translation
**Week**: 5 (Advanced Aggregation & Window Function Features)
**Date**: October 15, 2025
**Prerequisites**: Weeks 1-4 Complete (100%)

---

## Executive Summary

Week 5 focuses on **advanced aggregation and window function features** that build upon the basic implementations delivered in Week 3. Since `Aggregate` and `WindowFunction` classes already exist with basic SQL generation capabilities, this week enhances them with production-grade features including HAVING clauses, DISTINCT aggregates, ROLLUP/CUBE/GROUPING SETS, window frames, named windows, and advanced value window functions.

**What Week 5 Achieves**:
- ✅ Enhanced aggregation with HAVING, DISTINCT, ROLLUP/CUBE/GROUPING SETS
- ✅ Advanced window functions with frames, named windows, value functions
- ✅ 100+ comprehensive tests (55+ aggregation, 45+ window)
- ✅ TPC-H Q13, Q18 execution with 5x+ performance improvement
- ✅ Production-ready error handling and validation
- ✅ Memory-efficient implementations for large datasets

**Key Deliverables**:
1. **10 new classes/enhancements** (HAVING support, frame specifications, named windows)
2. **100+ comprehensive tests** across aggregation and window function features
3. **2 TPC-H queries** (Q13, Q18) running with validation
4. **Performance optimizations** for aggregate pushdown and window execution
5. **Integration tests** combining aggregates + windows + joins

---

## Week 5 Objectives

### 1. Enhanced Aggregation Features (Phase 1)
- Implement HAVING clause support with complex predicates
- Add DISTINCT aggregate functions (COUNT DISTINCT, SUM DISTINCT)
- Implement ROLLUP, CUBE, GROUPING SETS for multi-dimensional aggregation
- Add advanced aggregate functions (STDDEV, VARIANCE, PERCENTILE)
- Create 55+ comprehensive aggregation tests

### 2. Advanced Window Function Features (Phase 2)
- Implement window frame specifications (ROWS BETWEEN, RANGE BETWEEN)
- Add named window support (WINDOW clause)
- Implement value window functions (NTH_VALUE, PERCENT_RANK, CUME_DIST)
- Support multiple windows in single query
- Create 45+ comprehensive window function tests

### 3. Performance & Integration (Phase 3)
- Run TPC-H Q13, Q18 with aggregation-heavy queries
- Implement aggregate pushdown optimization rule
- Optimize window function execution
- Create integration tests combining aggregates + windows + joins
- Add memory efficiency tests for large aggregations

### 4. Success Criteria
- ✅ 100% feature completion (all HAVING, DISTINCT, ROLLUP, CUBE, frames, named windows)
- ✅ 100+ tests passing (55+ aggregation, 45+ window)
- ✅ TPC-H Q13, Q18 passing with 5x+ speedup vs unoptimized
- ✅ Zero compilation errors
- ✅ 100% JavaDoc coverage on all public APIs
- ✅ Memory-efficient implementations validated

---

## Phase 1: Enhanced Aggregation Features (26 hours)

### Task W5-1: HAVING Clause Support (6 hours)

**Objective**: Add HAVING clause filtering on aggregate results with complex predicates.

**Implementation Details**:

**1. Enhance Aggregate.java** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/logical/Aggregate.java`):
- Add `Expression havingCondition` field (nullable)
- Add `withHaving(Expression condition)` method
- Update `toSQL()` to generate HAVING clause
- Update `inferSchema()` to validate HAVING references

**Code Changes** (~150 lines):
```java
public class Aggregate extends LogicalPlan {
    private final List<Expression> groupingExpressions;
    private final List<AggregateExpression> aggregateExpressions;
    private final Expression havingCondition;  // NEW

    // Enhanced constructor
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions,
                    Expression havingCondition) {
        super(child);
        this.groupingExpressions = new ArrayList<>(
            Objects.requireNonNull(groupingExpressions));
        this.aggregateExpressions = new ArrayList<>(
            Objects.requireNonNull(aggregateExpressions));
        this.havingCondition = havingCondition;  // Can be null
    }

    // Backward compatibility constructor
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions) {
        this(child, groupingExpressions, aggregateExpressions, null);
    }

    public Expression havingCondition() {
        return havingCondition;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // ... existing SELECT and FROM logic ...

        // GROUP BY clause
        if (!groupingExpressions.isEmpty()) {
            sql.append(" GROUP BY ");
            sql.append(String.join(", ",
                groupingExpressions.stream()
                    .map(Expression::toSQL)
                    .collect(Collectors.toList())));
        }

        // HAVING clause (NEW)
        if (havingCondition != null) {
            sql.append(" HAVING ");
            sql.append(havingCondition.toSQL());
        }

        return sql.toString();
    }
}
```

**SQL Examples**:
```sql
-- Simple HAVING
SELECT category, SUM(amount) AS total
FROM sales
GROUP BY category
HAVING SUM(amount) > 1000

-- Complex HAVING with multiple conditions
SELECT customer_id, COUNT(*) AS order_count, AVG(amount) AS avg_amount
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5 AND AVG(amount) > 100
```

**Tests** (12 tests in `HavingClauseTest.java`):
1. Simple HAVING with single aggregate condition
2. HAVING with multiple AND conditions
3. HAVING with OR conditions
4. HAVING referencing aggregate aliases
5. HAVING with aggregate functions not in SELECT
6. HAVING without GROUP BY (global aggregation)
7. HAVING with BETWEEN predicate
8. HAVING with IN predicate
9. HAVING with complex nested conditions
10. HAVING validation (must use aggregates or grouping keys)
11. HAVING with NULL handling
12. HAVING with CASE expressions

---

### Task W5-2: DISTINCT Aggregates (6 hours)

**Objective**: Support DISTINCT keyword in aggregate functions for unique value aggregation.

**Implementation Details**:

**1. Enhance AggregateExpression** (in `Aggregate.java`):
- Add `boolean distinct` field
- Add `isDistinct()` method
- Update `toSQL()` to include DISTINCT keyword

**Code Changes** (~100 lines):
```java
public static class AggregateExpression extends Expression {
    private final String function;
    private final Expression argument;
    private final String alias;
    private final boolean distinct;  // NEW

    public AggregateExpression(String function,
                              Expression argument,
                              String alias,
                              boolean distinct) {
        this.function = Objects.requireNonNull(function);
        this.argument = argument;
        this.alias = alias;
        this.distinct = distinct;
    }

    // Backward compatibility constructor
    public AggregateExpression(String function,
                              Expression argument,
                              String alias) {
        this(function, argument, alias, false);
    }

    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append(function.toUpperCase());
        sql.append("(");

        if (distinct) {
            sql.append("DISTINCT ");
        }

        if (argument != null) {
            sql.append(argument.toSQL());
        } else {
            // COUNT(*) case
            if (!distinct) {
                sql.append("*");
            }
        }

        sql.append(")");
        return sql.toString();
    }
}
```

**SQL Examples**:
```sql
-- COUNT DISTINCT
SELECT COUNT(DISTINCT customer_id) FROM orders

-- SUM DISTINCT
SELECT category, SUM(DISTINCT price) AS unique_prices_sum
FROM products
GROUP BY category

-- Multiple DISTINCT aggregates
SELECT
    category,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(DISTINCT product_id) AS unique_products,
    AVG(amount) AS avg_amount
FROM sales
GROUP BY category
```

**Tests** (10 tests in `DistinctAggregateTest.java`):
1. COUNT(DISTINCT column)
2. SUM(DISTINCT column)
3. AVG(DISTINCT column)
4. Multiple DISTINCT aggregates in same query
5. DISTINCT with GROUP BY
6. DISTINCT with HAVING
7. COUNT(DISTINCT *) validation (should error)
8. DISTINCT with NULL values
9. DISTINCT with complex expressions
10. DISTINCT vs non-DISTINCT comparison

---

### Task W5-3: ROLLUP, CUBE, GROUPING SETS (8 hours)

**Objective**: Implement multi-dimensional aggregation with ROLLUP, CUBE, and GROUPING SETS.

**Implementation Details**:

**1. Create GroupingType enum** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/logical/GroupingType.java`):
```java
public enum GroupingType {
    SIMPLE,       // Standard GROUP BY
    ROLLUP,       // GROUP BY ROLLUP(cols)
    CUBE,         // GROUP BY CUBE(cols)
    GROUPING_SETS // GROUP BY GROUPING SETS((set1), (set2), ...)
}
```

**2. Create GroupingSets class** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/logical/GroupingSets.java`, ~200 lines):
```java
/**
 * Represents grouping sets for multi-dimensional aggregation.
 *
 * Examples:
 * - ROLLUP(year, month, day) generates 4 grouping sets
 * - CUBE(a, b, c) generates 8 grouping sets
 * - GROUPING SETS((a,b), (a,c), (b,c)) generates 3 custom sets
 */
public class GroupingSets {
    private final GroupingType type;
    private final List<List<Expression>> sets;

    // Factory methods
    public static GroupingSets rollup(List<Expression> columns) {
        // Generate hierarchical grouping sets
    }

    public static GroupingSets cube(List<Expression> columns) {
        // Generate all combinations
    }

    public static GroupingSets groupingSets(List<List<Expression>> sets) {
        // Custom grouping sets
    }

    public String toSQL() {
        switch (type) {
            case ROLLUP:
                return "ROLLUP(" + columnsToSQL() + ")";
            case CUBE:
                return "CUBE(" + columnsToSQL() + ")";
            case GROUPING_SETS:
                return "GROUPING SETS(" + setsToSQL() + ")";
            default:
                return columnsToSQL();
        }
    }
}
```

**3. Enhance Aggregate.java**:
- Add `GroupingSets groupingSets` field (optional)
- Update `toSQL()` to handle ROLLUP/CUBE/GROUPING SETS syntax

**SQL Examples**:
```sql
-- ROLLUP (hierarchical aggregation)
SELECT year, month, SUM(amount) AS total
FROM sales
GROUP BY ROLLUP(year, month)
-- Generates: (year, month), (year), ()

-- CUBE (all combinations)
SELECT region, category, product, SUM(amount) AS total
FROM sales
GROUP BY CUBE(region, category, product)
-- Generates 8 grouping sets

-- GROUPING SETS (custom combinations)
SELECT region, category, SUM(amount) AS total
FROM sales
GROUP BY GROUPING SETS((region, category), (region), (category))
```

**Tests** (15 tests in `GroupingSetsTest.java`):
1. ROLLUP with 2 columns
2. ROLLUP with 3 columns
3. ROLLUP with single column
4. CUBE with 2 columns (4 sets)
5. CUBE with 3 columns (8 sets)
6. GROUPING SETS with custom sets
7. GROUPING SETS with empty set (grand total)
8. ROLLUP with aggregate functions
9. CUBE with HAVING clause
10. GROUPING SETS with DISTINCT
11. GROUPING() function usage
12. ROLLUP NULL handling
13. Multiple grouping dimensions
14. GROUPING SETS validation
15. SQL generation correctness

---

### Task W5-4: Advanced Aggregate Functions (6 hours)

**Objective**: Add statistical and percentile aggregate functions.

**Implementation Details**:

**1. Create new aggregate function classes** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/expression/aggregate/`):

**StatisticalAggregates.java** (~250 lines):
```java
// Standard deviation (sample and population)
public class StdDevAggregate extends AggregateExpression {
    private final boolean population;  // false = sample, true = population

    @Override
    public String toSQL() {
        return population ?
            "STDDEV_POP(" + argument.toSQL() + ")" :
            "STDDEV_SAMP(" + argument.toSQL() + ")";
    }
}

// Variance (sample and population)
public class VarianceAggregate extends AggregateExpression {
    private final boolean population;

    @Override
    public String toSQL() {
        return population ?
            "VAR_POP(" + argument.toSQL() + ")" :
            "VAR_SAMP(" + argument.toSQL() + ")";
    }
}

// Percentile (continuous and discrete)
public class PercentileAggregate extends AggregateExpression {
    private final double percentile;  // 0.0 to 1.0
    private final boolean continuous;

    @Override
    public String toSQL() {
        String func = continuous ? "PERCENTILE_CONT" : "PERCENTILE_DISC";
        return String.format("%s(%s) WITHIN GROUP (ORDER BY %s)",
            func, percentile, argument.toSQL());
    }
}

// Median (special case of percentile)
public class MedianAggregate extends AggregateExpression {
    @Override
    public String toSQL() {
        return "MEDIAN(" + argument.toSQL() + ")";
    }
}
```

**SQL Examples**:
```sql
-- Standard deviation and variance
SELECT
    category,
    STDDEV_SAMP(price) AS price_stddev,
    VAR_SAMP(price) AS price_variance
FROM products
GROUP BY category

-- Percentiles
SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY salary) AS p95_salary
FROM employees

-- Statistical summary
SELECT
    AVG(price) AS mean,
    MEDIAN(price) AS median,
    STDDEV_SAMP(price) AS stddev,
    MIN(price) AS min,
    MAX(price) AS max
FROM products
```

**Tests** (18 tests in `AdvancedAggregatesTest.java`):
1. STDDEV_SAMP simple
2. STDDEV_POP simple
3. VAR_SAMP simple
4. VAR_POP simple
5. STDDEV with GROUP BY
6. VARIANCE with NULL values
7. PERCENTILE_CONT at 0.5 (median)
8. PERCENTILE_CONT at 0.95
9. PERCENTILE_DISC simple
10. MEDIAN aggregate
11. Multiple statistical aggregates
12. STDDEV with HAVING
13. PERCENTILE with PARTITION
14. Statistical functions with DISTINCT
15. Empty dataset handling
16. Single value dataset
17. All NULL values
18. Statistical aggregates validation

---

## Phase 2: Advanced Window Function Features (24 hours)

### Task W5-5: Window Frame Specifications (8 hours)

**Objective**: Implement ROWS BETWEEN and RANGE BETWEEN for sliding window frames.

**Implementation Details**:

**1. Create FrameBoundary classes** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/expression/window/FrameBoundary.java`, ~150 lines):
```java
public abstract class FrameBoundary {
    public abstract String toSQL();

    public static class UnboundedPreceding extends FrameBoundary {
        @Override
        public String toSQL() { return "UNBOUNDED PRECEDING"; }
    }

    public static class UnboundedFollowing extends FrameBoundary {
        @Override
        public String toSQL() { return "UNBOUNDED FOLLOWING"; }
    }

    public static class CurrentRow extends FrameBoundary {
        @Override
        public String toSQL() { return "CURRENT ROW"; }
    }

    public static class Preceding extends FrameBoundary {
        private final int offset;

        @Override
        public String toSQL() { return offset + " PRECEDING"; }
    }

    public static class Following extends FrameBoundary {
        private final int offset;

        @Override
        public String toSQL() { return offset + " FOLLOWING"; }
    }
}
```

**2. Create WindowFrame class** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/expression/window/WindowFrame.java`, ~200 lines):
```java
public class WindowFrame {
    public enum FrameType { ROWS, RANGE, GROUPS }

    private final FrameType type;
    private final FrameBoundary start;
    private final FrameBoundary end;

    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append(type.name());
        sql.append(" BETWEEN ");
        sql.append(start.toSQL());
        sql.append(" AND ");
        sql.append(end.toSQL());
        return sql.toString();
    }

    // Common frame specifications
    public static WindowFrame unboundedPrecedingToCurrentRow() {
        return new WindowFrame(
            FrameType.ROWS,
            new FrameBoundary.UnboundedPreceding(),
            new FrameBoundary.CurrentRow()
        );
    }

    public static WindowFrame currentRowToUnboundedFollowing() {
        return new WindowFrame(
            FrameType.ROWS,
            new FrameBoundary.CurrentRow(),
            new FrameBoundary.UnboundedFollowing()
        );
    }

    public static WindowFrame rowsBetween(int startOffset, int endOffset) {
        return new WindowFrame(
            FrameType.ROWS,
            new FrameBoundary.Preceding(startOffset),
            new FrameBoundary.Following(endOffset)
        );
    }
}
```

**3. Enhance WindowFunction.java**:
- Add `WindowFrame frame` field (optional)
- Update `toSQL()` to include frame specification

**SQL Examples**:
```sql
-- Moving average (3-row window)
SELECT
    date,
    amount,
    AVG(amount) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3
FROM sales

-- Cumulative sum
SELECT
    date,
    amount,
    SUM(amount) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sum
FROM sales

-- Centered moving average
SELECT
    date,
    AVG(amount) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) AS centered_avg_5
FROM sales

-- RANGE frame (value-based)
SELECT
    timestamp,
    amount,
    SUM(amount) OVER (
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
    ) AS hourly_total
FROM events
```

**Tests** (15 tests in `WindowFrameTest.java`):
1. ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
2. ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
3. ROWS BETWEEN 2 PRECEDING AND CURRENT ROW (moving window)
4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (centered)
5. ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
6. RANGE BETWEEN (value-based frame)
7. Frame with PARTITION BY
8. Frame with ORDER BY
9. Multiple window functions with different frames
10. Frame validation (start must be before end)
11. Frame with aggregate functions
12. Frame with NULL handling
13. Empty partition with frame
14. Single row partition
15. Frame SQL generation correctness

---

### Task W5-6: Named Windows (WINDOW Clause) (6 hours)

**Objective**: Support named window definitions for reusability and readability.

**Implementation Details**:

**1. Create NamedWindow class** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/expression/window/NamedWindow.java`, ~150 lines):
```java
/**
 * Represents a named window definition.
 *
 * Example:
 * WINDOW w1 AS (PARTITION BY category ORDER BY price)
 */
public class NamedWindow {
    private final String name;
    private final List<Expression> partitionBy;
    private final List<Sort.SortOrder> orderBy;
    private final WindowFrame frame;

    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append(name);
        sql.append(" AS (");

        // PARTITION BY
        if (!partitionBy.isEmpty()) {
            sql.append("PARTITION BY ");
            sql.append(partitionBy.stream()
                .map(Expression::toSQL)
                .collect(Collectors.joining(", ")));
        }

        // ORDER BY
        if (!orderBy.isEmpty()) {
            if (!partitionBy.isEmpty()) sql.append(" ");
            sql.append("ORDER BY ");
            // ... order by logic ...
        }

        // Frame
        if (frame != null) {
            sql.append(" ");
            sql.append(frame.toSQL());
        }

        sql.append(")");
        return sql.toString();
    }
}
```

**2. Create WindowClause logical plan node** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/logical/WindowClause.java`, ~200 lines):
```java
/**
 * Logical plan node for queries with WINDOW clause.
 *
 * SELECT
 *   product,
 *   ROW_NUMBER() OVER w1,
 *   RANK() OVER w1
 * FROM products
 * WINDOW w1 AS (PARTITION BY category ORDER BY price)
 */
public class WindowClause extends LogicalPlan {
    private final LogicalPlan child;
    private final List<NamedWindow> windows;
    private final List<Expression> windowExpressions;

    @Override
    public String toSQL(SQLGenerator generator) {
        // SELECT with window function references
        // WINDOW clause definitions
        // FROM clause
    }
}
```

**SQL Examples**:
```sql
-- Single named window
SELECT
    product,
    ROW_NUMBER() OVER w AS row_num,
    RANK() OVER w AS rank,
    DENSE_RANK() OVER w AS dense_rank
FROM products
WINDOW w AS (PARTITION BY category ORDER BY price DESC)

-- Multiple named windows
SELECT
    date,
    amount,
    AVG(amount) OVER daily AS daily_avg,
    SUM(amount) OVER monthly AS monthly_total
FROM sales
WINDOW
    daily AS (PARTITION BY DATE_TRUNC('day', date) ORDER BY date),
    monthly AS (PARTITION BY DATE_TRUNC('month', date) ORDER BY date)

-- Named window with frame
SELECT
    date,
    SUM(amount) OVER w AS moving_sum
FROM sales
WINDOW w AS (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

**Tests** (10 tests in `NamedWindowTest.java`):
1. Single named window
2. Multiple named windows
3. Named window with PARTITION BY
4. Named window with ORDER BY
5. Named window with frame specification
6. Window function reference to named window
7. Multiple functions using same named window
8. Named window without PARTITION BY
9. Named window validation (must be referenced)
10. SQL generation correctness

---

### Task W5-7: Value Window Functions (6 hours)

**Objective**: Implement NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE window functions.

**Implementation Details**:

**1. Create ValueWindowFunctions** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/expression/window/ValueWindowFunctions.java`, ~300 lines):
```java
// NTH_VALUE - value at specific position in window
public class NthValue extends WindowFunction {
    private final Expression expression;
    private final int position;

    public NthValue(Expression expression, int position,
                    List<Expression> partitionBy,
                    List<Sort.SortOrder> orderBy,
                    WindowFrame frame) {
        super("NTH_VALUE",
              Arrays.asList(expression, new Literal(position)),
              partitionBy, orderBy, frame);
    }

    @Override
    public String toSQL() {
        return String.format("NTH_VALUE(%s, %d) OVER (%s)",
            expression.toSQL(), position, overClauseToSQL());
    }
}

// PERCENT_RANK - relative rank (0.0 to 1.0)
public class PercentRank extends WindowFunction {
    public PercentRank(List<Expression> partitionBy,
                       List<Sort.SortOrder> orderBy) {
        super("PERCENT_RANK", Collections.emptyList(),
              partitionBy, orderBy, null);
    }

    @Override
    public String toSQL() {
        return "PERCENT_RANK() OVER (" + overClauseToSQL() + ")";
    }
}

// CUME_DIST - cumulative distribution (0.0 to 1.0)
public class CumeDist extends WindowFunction {
    public CumeDist(List<Expression> partitionBy,
                    List<Sort.SortOrder> orderBy) {
        super("CUME_DIST", Collections.emptyList(),
              partitionBy, orderBy, null);
    }

    @Override
    public String toSQL() {
        return "CUME_DIST() OVER (" + overClauseToSQL() + ")";
    }
}

// NTILE - divide rows into N buckets
public class Ntile extends WindowFunction {
    private final int buckets;

    public Ntile(int buckets,
                 List<Expression> partitionBy,
                 List<Sort.SortOrder> orderBy) {
        super("NTILE", Arrays.asList(new Literal(buckets)),
              partitionBy, orderBy, null);
        this.buckets = buckets;
    }

    @Override
    public String toSQL() {
        return String.format("NTILE(%d) OVER (%s)",
            buckets, overClauseToSQL());
    }
}
```

**SQL Examples**:
```sql
-- NTH_VALUE (get second highest salary in each department)
SELECT
    employee,
    salary,
    NTH_VALUE(salary, 2) OVER (
        PARTITION BY department
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_highest
FROM employees

-- PERCENT_RANK (relative ranking 0-1)
SELECT
    student,
    score,
    PERCENT_RANK() OVER (ORDER BY score DESC) AS percentile
FROM test_scores

-- CUME_DIST (cumulative distribution)
SELECT
    product,
    price,
    CUME_DIST() OVER (ORDER BY price) AS cumulative_dist
FROM products

-- NTILE (divide into quartiles)
SELECT
    customer,
    total_spent,
    NTILE(4) OVER (ORDER BY total_spent DESC) AS spending_quartile
FROM customer_totals
```

**Tests** (14 tests in `ValueWindowFunctionsTest.java`):
1. NTH_VALUE with position 1 (FIRST_VALUE equivalent)
2. NTH_VALUE with position N
3. NTH_VALUE with frame specification
4. NTH_VALUE with NULL handling
5. PERCENT_RANK basic usage
6. PERCENT_RANK with ties
7. PERCENT_RANK with single value
8. CUME_DIST basic usage
9. CUME_DIST with ties
10. NTILE with 4 buckets (quartiles)
11. NTILE with uneven distribution
12. NTILE with small dataset
13. Multiple value functions in same query
14. Value functions SQL generation

---

### Task W5-8: Window Function Optimizations (4 hours)

**Objective**: Optimize window function execution for performance.

**Implementation Details**:

**1. Create WindowFunctionOptimizationRule** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/optimizer/WindowFunctionOptimizationRule.java`, ~300 lines):
```java
/**
 * Optimization rule for window functions.
 *
 * Optimizations:
 * 1. Merge adjacent window operations with same partitioning
 * 2. Push filters before window computation when safe
 * 3. Reorder window functions by partition/order compatibility
 * 4. Eliminate redundant window computations
 */
public class WindowFunctionOptimizationRule implements OptimizationRule {

    @Override
    public LogicalPlan transform(LogicalPlan plan) {
        if (plan instanceof Project) {
            return optimizeWindowProject((Project) plan);
        }
        return plan;
    }

    /**
     * Merge multiple window functions with compatible OVER clauses.
     *
     * Before:
     * SELECT
     *   ROW_NUMBER() OVER (PARTITION BY a ORDER BY b),
     *   RANK() OVER (PARTITION BY a ORDER BY b)
     *
     * After: Single window computation shared by both functions
     */
    private LogicalPlan optimizeWindowProject(Project project) {
        // Group window functions by OVER clause
        Map<WindowSpec, List<WindowFunction>> grouped =
            groupWindowsBySpec(project.expressions());

        // If multiple functions share same spec, they can share computation
        if (hasSharedSpecs(grouped)) {
            return createOptimizedWindowPlan(project, grouped);
        }

        return project;
    }
}
```

**Tests** (6 tests in `WindowOptimizationTest.java`):
1. Merge adjacent windows with same partitioning
2. Keep separate windows with different partitioning
3. Reorder windows for efficiency
4. Push filter before window when safe
5. Eliminate redundant window computations
6. Window optimization with frames

---

## Phase 3: Performance & Integration (22 hours)

### Task W5-9: TPC-H Q13, Q18 Implementation (8 hours)

**Objective**: Implement and run TPC-H aggregation-heavy queries with validation.

**Implementation Details**:

**1. TPC-H Q13** (Customer Distribution - Aggregation with OUTER JOIN):
```sql
-- Query 13: Customer Distribution
-- Counts the number of customers with specific order counts
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c.c_custkey,
        COUNT(o.o_orderkey) AS c_count
    FROM customer c
    LEFT OUTER JOIN orders o
        ON c.c_custkey = o.o_custkey
        AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
```

**Logical Plan Construction** (`TPCHQueries.java`):
```java
public static LogicalPlan query13(String dataPath) {
    // Scan customer and orders
    TableScan customer = new TableScan(dataPath + "/customer.parquet",
        CUSTOMER_SCHEMA, TableFormat.PARQUET);
    TableScan orders = new TableScan(dataPath + "/orders.parquet",
        ORDERS_SCHEMA, TableFormat.PARQUET);

    // Filter on orders.o_comment
    Filter ordersFiltered = new Filter(orders,
        new Not(new Like(
            new ColumnReference("o_comment", StringType, Optional.of("o")),
            new Literal("%special%requests%", StringType)
        ))
    );

    // LEFT OUTER JOIN
    Join joined = new Join(
        customer, ordersFiltered,
        JoinType.LEFT_OUTER,
        new Equal(
            new ColumnReference("c_custkey", IntegerType, Optional.of("c")),
            new ColumnReference("o_custkey", IntegerType, Optional.of("o"))
        )
    );

    // Inner aggregation: GROUP BY c_custkey, COUNT(o_orderkey)
    Aggregate innerAgg = new Aggregate(
        joined,
        Arrays.asList(new ColumnReference("c_custkey", IntegerType)),
        Arrays.asList(new AggregateExpression("COUNT",
            new ColumnReference("o_orderkey", IntegerType),
            "c_count"))
    );

    // Outer aggregation: GROUP BY c_count, COUNT(*)
    Aggregate outerAgg = new Aggregate(
        innerAgg,
        Arrays.asList(new ColumnReference("c_count", LongType)),
        Arrays.asList(new AggregateExpression("COUNT",
            new Literal(1, IntegerType),  // COUNT(*)
            "custdist"))
    );

    // ORDER BY
    Sort sorted = new Sort(outerAgg,
        Arrays.asList(
            new Sort.SortOrder(
                new ColumnReference("custdist", LongType),
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            ),
            new Sort.SortOrder(
                new ColumnReference("c_count", LongType),
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            )
        )
    );

    return sorted;
}
```

**2. TPC-H Q18** (Large Volume Customer - Multiple Aggregations):
```sql
-- Query 18: Large Volume Customer
-- Finds customers with orders totaling more than a threshold
SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity) AS total_quantity
FROM customer c
JOIN orders o ON c.c_custkey = o.c_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderkey IN (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
```

**Logical Plan Construction** (similar pattern with HAVING clause and subquery).

**Benchmark Integration** (`TPCHBenchmark.java`):
```java
@Benchmark
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public void tpchQ13_sqlGeneration(Blackhole blackhole) {
    LogicalPlan plan = TPCHQueries.query13(dataPath);
    String sql = generator.generate(plan);
    blackhole.consume(sql);
}

@Benchmark
public void tpchQ13_optimized_execution(Blackhole blackhole) {
    LogicalPlan plan = TPCHQueries.query13(dataPath);
    LogicalPlan optimized = optimizer.optimize(plan);
    String sql = generator.generate(optimized);
    // Execute and measure
    ResultSet results = executeQuery(sql);
    blackhole.consume(results);
}
```

**Tests** (6 tests in `TPCHAggregationQueriesTest.java`):
1. Q13 logical plan construction
2. Q13 SQL generation
3. Q13 execution correctness (small dataset)
4. Q18 logical plan construction
5. Q18 SQL generation with HAVING
6. Q18 execution correctness

---

### Task W5-10: Aggregate Pushdown Optimization (6 hours)

**Objective**: Implement optimization rule to push aggregates closer to data sources.

**Implementation Details**:

**1. Create AggregatePushdownRule** (`/workspaces/catalyst2sql/core/src/main/java/com/catalyst2sql/optimizer/AggregatePushdownRule.java`, ~400 lines):
```java
/**
 * Optimization rule to push aggregates through joins when safe.
 *
 * Transformation example:
 *
 * Before:
 * Aggregate(sum(amount))
 *   Join(orders, customers)
 *
 * After (if aggregate only uses orders columns):
 * Join(
 *   Aggregate(sum(amount)) over orders,
 *   customers
 * )
 *
 * This reduces the join cardinality by pre-aggregating.
 */
public class AggregatePushdownRule implements OptimizationRule {

    @Override
    public LogicalPlan transform(LogicalPlan plan) {
        if (plan instanceof Aggregate) {
            Aggregate agg = (Aggregate) plan;
            if (agg.child() instanceof Join) {
                return tryPushAggregateIntoJoin(agg, (Join) agg.child());
            }
        }
        return plan;
    }

    /**
     * Try to push aggregate into one side of join.
     * Only safe if:
     * 1. Join is INNER
     * 2. Aggregate only references columns from one side
     * 3. Join condition preserves grouping keys
     */
    private LogicalPlan tryPushAggregateIntoJoin(Aggregate agg, Join join) {
        // Analyze which side(s) aggregate references
        Set<String> aggColumns = extractColumnReferences(agg);
        Set<String> leftColumns = getOutputColumns(join.left());
        Set<String> rightColumns = getOutputColumns(join.right());

        boolean onlyLeft = aggColumns.stream()
            .allMatch(leftColumns::contains);
        boolean onlyRight = aggColumns.stream()
            .allMatch(rightColumns::contains);

        if (onlyLeft && join.joinType() == JoinType.INNER) {
            // Push aggregate into left side
            Aggregate leftAgg = new Aggregate(
                join.left(),
                agg.groupingExpressions(),
                agg.aggregateExpressions()
            );

            return new Join(
                leftAgg,
                join.right(),
                join.joinType(),
                join.condition()
            );
        } else if (onlyRight && join.joinType() == JoinType.INNER) {
            // Push into right side
            // Similar logic
        }

        return agg;  // Can't push down
    }
}
```

**Optimization Examples**:
```sql
-- Before optimization
SELECT customer_id, SUM(amount) AS total
FROM (
    SELECT * FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
)
GROUP BY customer_id

-- After aggregate pushdown (if only orders.customer_id referenced)
SELECT customer_id, SUM(total) AS total
FROM (
    SELECT customer_id, SUM(amount) AS total
    FROM orders o
    GROUP BY customer_id
) o
JOIN order_details od ON o.order_id = od.order_id
```

**Tests** (8 tests in `AggregatePushdownTest.java`):
1. Push aggregate through INNER join (left side)
2. Push aggregate through INNER join (right side)
3. Cannot push through LEFT OUTER join
4. Cannot push when aggregate references both sides
5. Push with multiple grouping keys
6. Push with HAVING clause
7. Push validation (join condition compatibility)
8. Correctness verification

---

### Task W5-11: Integration Tests (4 hours)

**Objective**: Create comprehensive integration tests combining features.

**Implementation Details**:

**1. Create Week5IntegrationTest.java** (`/workspaces/catalyst2sql/tests/src/test/java/com/catalyst2sql/integration/Week5IntegrationTest.java`, ~600 lines):

```java
@TestCategories.Integration
public class Week5IntegrationTest {

    // Aggregates + Windows + Joins
    @Test
    public void testComplexAggregationWithWindowFunctions() {
        // Query: For each customer, show:
        // - Total orders
        // - Total amount
        // - Rank by amount
        // - Percentile

        LogicalPlan plan = // ... construct plan with:
            // - JOIN customers + orders
            // - GROUP BY customer
            // - Window functions over aggregates

        String sql = generator.generate(plan);

        assertThat(sql).contains("GROUP BY");
        assertThat(sql).contains("OVER (");
        assertThat(sql).contains("RANK()");
    }

    // HAVING + DISTINCT + Window
    @Test
    public void testHavingWithDistinctAndWindow() {
        // SELECT
        //   category,
        //   COUNT(DISTINCT product_id) AS unique_products,
        //   RANK() OVER (ORDER BY COUNT(DISTINCT product_id) DESC)
        // FROM sales
        // GROUP BY category
        // HAVING COUNT(DISTINCT product_id) > 10
    }

    // ROLLUP + Window frames
    @Test
    public void testRollupWithWindowFrames() {
        // Multi-dimensional aggregation with sliding windows
    }

    // Multiple named windows
    @Test
    public void testMultipleNamedWindowsWithAggregates() {
        // Reuse window definitions across multiple functions
    }

    // Optimized execution
    @Test
    public void testOptimizedAggregateExecution() {
        // Build plan, optimize, verify speedup
        LogicalPlan original = // ... complex aggregate + join
        LogicalPlan optimized = optimizer.optimize(original);

        long originalTime = measureExecution(original);
        long optimizedTime = measureExecution(optimized);

        assertThat(optimizedTime).isLessThan(originalTime);
    }
}
```

**Test Coverage** (15 tests):
1. Aggregates + Window functions in same query
2. HAVING with window functions
3. DISTINCT aggregates with window frames
4. ROLLUP with window partitioning
5. CUBE with named windows
6. Multiple aggregation levels with windows
7. Subquery with aggregates and windows
8. Complex HAVING with window functions
9. Statistical aggregates with percentile windows
10. Optimization correctness (aggregate pushdown)
11. Performance comparison (optimized vs unoptimized)
12. Memory efficiency test (large aggregations)
13. NULL handling in aggregates and windows
14. Edge cases (empty results, single row)
15. End-to-end integration

---

### Task W5-12: Memory Efficiency Tests (4 hours)

**Objective**: Validate memory-efficient execution for large aggregations.

**Implementation Details**:

**1. Create MemoryEfficiencyTest.java** (`/workspaces/catalyst2sql/tests/src/test/java/com/catalyst2sql/performance/MemoryEfficiencyTest.java`, ~400 lines):

```java
@TestCategories.Performance
public class MemoryEfficiencyTest {

    @Test
    public void testLargeGroupByMemoryUsage() {
        // Measure memory for aggregating 10M rows
        Runtime runtime = Runtime.getRuntime();
        long beforeMemory = getUsedMemory(runtime);

        // Execute large GROUP BY
        LogicalPlan plan = createLargeAggregation();
        String sql = generator.generate(plan);
        executeQuery(sql);

        long afterMemory = getUsedMemory(runtime);
        long memoryUsed = afterMemory - beforeMemory;

        // Should use less than 500MB for 10M rows
        assertThat(memoryUsed).isLessThan(500 * 1024 * 1024);
    }

    @Test
    public void testDistinctAggregateMemoryEfficiency() {
        // COUNT(DISTINCT) on high-cardinality column
        // Should use streaming distinct algorithm
    }

    @Test
    public void testWindowFunctionStreamingExecution() {
        // Window functions should stream, not buffer entire partition
    }

    @Test
    public void testRollupMemoryScaling() {
        // ROLLUP memory usage should scale with grouping sets, not data size
    }
}
```

**Tests** (6 tests):
1. Large GROUP BY memory usage
2. DISTINCT aggregate memory efficiency
3. Window function streaming execution
4. ROLLUP memory scaling
5. Multiple aggregation levels memory usage
6. Percentile aggregate memory efficiency

---

## Dependencies

### Week 5 Builds On:

**Week 3 Deliverables**:
- ✅ Basic Aggregate class with GROUP BY support
- ✅ Basic WindowFunction class with OVER clause
- ✅ Subquery support (ScalarSubquery, InSubquery, ExistsSubquery)
- ✅ Query optimizer framework

**Week 4 Deliverables**:
- ✅ 4 optimizer rules (FilterPushdown, ColumnPruning, ProjectionPushdown, JoinReordering)
- ✅ TPC-H benchmark framework (22 queries)
- ✅ ValidationException and QueryValidator
- ✅ QueryLogger with structured logging

**Required Components**:
- Expression framework (ColumnReference, Literal, BinaryExpression)
- LogicalPlan nodes (TableScan, Join, Filter, Project, Sort)
- SQL Generator with visitor pattern
- DuckDB integration for query execution
- Test infrastructure with JUnit 5

---

## Deliverables

### 1. Code Deliverables (10 new classes/enhancements, ~3,500 lines)

**Enhanced Classes**:
1. `Aggregate.java` - HAVING clause support (~150 lines added)
2. `Aggregate.AggregateExpression` - DISTINCT support (~100 lines added)
3. `WindowFunction.java` - Frame specification support (~150 lines added)

**New Classes**:
4. `GroupingType.java` - ROLLUP/CUBE/GROUPING SETS enum (~50 lines)
5. `GroupingSets.java` - Multi-dimensional aggregation (~200 lines)
6. `StatisticalAggregates.java` - STDDEV, VARIANCE, PERCENTILE (~250 lines)
7. `FrameBoundary.java` - Window frame boundary classes (~150 lines)
8. `WindowFrame.java` - ROWS/RANGE BETWEEN support (~200 lines)
9. `NamedWindow.java` - Named window definitions (~150 lines)
10. `WindowClause.java` - Logical plan for WINDOW clause (~200 lines)
11. `ValueWindowFunctions.java` - NTH_VALUE, PERCENT_RANK, etc. (~300 lines)
12. `WindowFunctionOptimizationRule.java` - Window optimizations (~300 lines)
13. `AggregatePushdownRule.java` - Aggregate pushdown rule (~400 lines)

**Benchmark Additions**:
14. `TPCHQueries.query13()` - TPC-H Q13 implementation (~150 lines)
15. `TPCHQueries.query18()` - TPC-H Q18 implementation (~150 lines)

**Total New Code**: ~3,500 lines

### 2. Test Deliverables (100+ tests, ~3,000 lines)

**Test Suites** (10 test files):
1. `HavingClauseTest.java` - 12 tests (~400 lines)
2. `DistinctAggregateTest.java` - 10 tests (~350 lines)
3. `GroupingSetsTest.java` - 15 tests (~500 lines)
4. `AdvancedAggregatesTest.java` - 18 tests (~550 lines)
5. `WindowFrameTest.java` - 15 tests (~450 lines)
6. `NamedWindowTest.java` - 10 tests (~300 lines)
7. `ValueWindowFunctionsTest.java` - 14 tests (~400 lines)
8. `WindowOptimizationTest.java` - 6 tests (~200 lines)
9. `TPCHAggregationQueriesTest.java` - 6 tests (~250 lines)
10. `AggregatePushdownTest.java` - 8 tests (~300 lines)
11. `Week5IntegrationTest.java` - 15 tests (~600 lines)
12. `MemoryEfficiencyTest.java` - 6 tests (~400 lines)

**Total Tests**: 135 tests (~4,700 lines)

### 3. Documentation Deliverables

**JavaDoc**:
- 100% coverage on all new public APIs
- Usage examples for each new feature
- Algorithm documentation for optimization rules

**Completion Report**:
- `WEEK5_COMPLETION_REPORT.md` - Full report with metrics, achievements, lessons learned

**Performance Results**:
- TPC-H Q13, Q18 benchmark results
- Optimization speedup measurements
- Memory usage analysis

---

## Success Criteria

### Functional Requirements
- ✅ HAVING clause working with complex predicates
- ✅ DISTINCT aggregates (COUNT, SUM, AVG) functional
- ✅ ROLLUP, CUBE, GROUPING SETS generating correct SQL
- ✅ Statistical aggregates (STDDEV, VARIANCE, PERCENTILE) working
- ✅ Window frames (ROWS BETWEEN, RANGE BETWEEN) functional
- ✅ Named windows (WINDOW clause) working
- ✅ Value window functions (NTH_VALUE, PERCENT_RANK, CUME_DIST) functional
- ✅ All features generate valid DuckDB SQL

### Testing Requirements
- ✅ 135+ tests created (55+ aggregation, 45+ window, 35+ integration/performance)
- ✅ 100% test pass rate
- ✅ Integration tests cover complex combinations
- ✅ Edge cases handled (NULL, empty results, single row)
- ✅ Memory efficiency validated

### Performance Requirements
- ✅ TPC-H Q13 running with <2s execution time (SF=1)
- ✅ TPC-H Q18 running with <3s execution time (SF=1)
- ✅ Aggregate pushdown showing 5x+ speedup on applicable queries
- ✅ Window optimization showing 2x+ speedup on multi-window queries
- ✅ Memory usage <500MB for 10M row aggregations

### Quality Requirements
- ✅ Zero compilation errors
- ✅ BUILD SUCCESS on entire codebase
- ✅ 100% JavaDoc coverage
- ✅ Comprehensive error handling
- ✅ Production-ready code quality
- ✅ All SQL validated against DuckDB

---

## Implementation Timeline

**Estimated Effort**: 72 hours (~9 working days)

**Phase 1: Enhanced Aggregation** (26 hours / 3.25 days):
- Day 1: W5-1 HAVING Clause (6h) + W5-2 DISTINCT Aggregates (6h)
- Day 2: W5-3 ROLLUP/CUBE/GROUPING SETS (8h)
- Day 3: W5-4 Advanced Aggregate Functions (6h)

**Phase 2: Advanced Window Functions** (24 hours / 3 days):
- Day 4: W5-5 Window Frame Specifications (8h)
- Day 5: W5-6 Named Windows (6h) + W5-7 Value Window Functions (6h)
- Day 6: W5-8 Window Optimizations (4h) + buffer

**Phase 3: Performance & Integration** (22 hours / 2.75 days):
- Day 7: W5-9 TPC-H Q13, Q18 (8h)
- Day 8: W5-10 Aggregate Pushdown (6h) + W5-11 Integration Tests (4h)
- Day 9: W5-12 Memory Efficiency (4h) + final validation + completion report

**Buffer**: Day 9 afternoon for issues and documentation

---

## Risk Assessment

### High Risk: Window Frame Complexity
**Risk**: Window frame specifications are complex with many edge cases.
**Mitigation**:
- Start with simple ROWS BETWEEN cases
- Extensive testing with various boundary combinations
- Reference DuckDB documentation for SQL semantics
- Use TPC-H queries as validation

### Medium Risk: ROLLUP/CUBE Cardinality
**Risk**: CUBE can generate 2^N grouping sets, causing performance issues.
**Mitigation**:
- Document limitations (max 10 dimensions recommended)
- Add validation to warn on large CUBEs
- Test with realistic dimension counts (2-5)
- Use GROUPING SETS for precise control

### Medium Risk: TPC-H Data Generation
**Risk**: Large TPC-H datasets take time to generate.
**Mitigation**:
- Start with SF=0.01 for development
- Use SF=1 for final validation
- Cache generated data
- Focus on query correctness first, then performance

### Low Risk: Memory Efficiency Testing
**Risk**: Memory tests can be flaky depending on GC behavior.
**Mitigation**:
- Use multiple runs and average results
- Focus on relative comparisons vs absolute values
- Test memory trends, not exact numbers
- Use DuckDB's streaming execution

---

## Technical Highlights

### 1. HAVING Clause Implementation
```java
// Clean backward-compatible API
Aggregate agg = new Aggregate(
    child,
    groupingExprs,
    aggExprs,
    havingCondition  // Optional - can be null
);

// SQL Generation
SELECT category, SUM(amount) AS total
FROM sales
GROUP BY category
HAVING SUM(amount) > 1000
```

### 2. DISTINCT Aggregates
```java
// Simple boolean flag
AggregateExpression countDistinct = new AggregateExpression(
    "COUNT",
    new ColumnReference("customer_id"),
    "unique_customers",
    true  // distinct = true
);

// SQL: COUNT(DISTINCT customer_id)
```

### 3. Window Frames
```java
// Fluent API for common patterns
WindowFrame frame = WindowFrame.rowsBetween(2, 0);  // 2 PRECEDING to CURRENT
WindowFrame cumulative = WindowFrame.unboundedPrecedingToCurrentRow();

// SQL: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
```

### 4. Named Windows
```java
// Define once, reuse multiple times
NamedWindow w1 = new NamedWindow(
    "w1",
    partitionBy,
    orderBy,
    frame
);

// SQL:
// WINDOW w1 AS (PARTITION BY category ORDER BY price)
// ROW_NUMBER() OVER w1
// RANK() OVER w1
```

---

## Code Quality Standards

### Documentation
- ✅ 100% JavaDoc on all public APIs
- ✅ Usage examples in class-level JavaDoc
- ✅ Algorithm descriptions for optimization rules
- ✅ SQL generation examples

### Design Principles
- ✅ Backward compatibility (existing code unaffected)
- ✅ Visitor pattern consistency
- ✅ Immutable data structures
- ✅ Null safety with Objects.requireNonNull()
- ✅ Clean separation of concerns

### Testing Strategy
- ✅ Unit tests for each feature
- ✅ Integration tests for combinations
- ✅ Performance tests for regression detection
- ✅ Memory efficiency validation
- ✅ TPC-H queries for real-world validation

---

## Future Enhancements (Beyond Week 5)

### Short Term (Week 6)
1. **Correlated Subqueries**: Advanced subquery features
2. **LATERAL Joins**: Table-generating functions
3. **Recursive CTEs**: WITH RECURSIVE support
4. **Advanced Optimizations**: Subquery decorrelation

### Medium Term (Weeks 7-8)
1. **Query Plan Caching**: Reuse compiled plans
2. **Parallel Execution**: Multi-threaded query execution
3. **Incremental Aggregation**: Update aggregates efficiently
4. **Materialized Views**: Pre-computed aggregations

### Long Term (Weeks 9-12)
1. **Cost-Based Optimization**: Statistics-driven optimization
2. **Adaptive Execution**: Runtime query re-optimization
3. **Distributed Execution**: Multi-node query execution
4. **Production Deployment**: Monitoring, scaling, reliability

---

## Conclusion

Week 5 represents a significant advancement in catalyst2sql's aggregation and window function capabilities. By building on the solid foundation from Weeks 1-4, we add production-grade features that handle complex analytical workloads.

**Key Achievements Planned**:
- ✅ Complete HAVING clause support
- ✅ Full DISTINCT aggregate capabilities
- ✅ Enterprise-grade ROLLUP/CUBE/GROUPING SETS
- ✅ Comprehensive window frames and named windows
- ✅ Advanced statistical and value window functions
- ✅ Performance optimizations with measurable speedups
- ✅ 100+ comprehensive tests
- ✅ TPC-H validation with Q13, Q18

The implementation follows a proven phased approach (aggregation → windows → integration) with comprehensive testing at each step. Success criteria are clear, risks are mitigated, and the timeline is realistic.

Upon completion, catalyst2sql will have industry-leading aggregation and window function support, positioning it for production use in complex analytical workloads.

---

**Plan Created**: October 15, 2025
**Planned Effort**: 72 hours (9 working days)
**Expected Completion**: Week 5 of implementation cycle
**Dependencies**: Weeks 1-4 (100% complete)
**Risk Level**: Medium (window frame complexity, TPC-H data)
**Success Probability**: High (building on proven foundation)

Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
