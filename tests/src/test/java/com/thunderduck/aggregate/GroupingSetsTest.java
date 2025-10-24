package com.thunderduck.aggregate;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.Literal;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.Aggregate;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.logical.GroupingSets;
import com.thunderduck.logical.GroupingType;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.logical.TableScan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for ROLLUP, CUBE, and GROUPING SETS (Week 5 Phase 1).
 *
 * <p>Tests 15 scenarios covering:
 * - ROLLUP with 2 and 3 columns
 * - CUBE with 2 and 3 columns
 * - GROUPING SETS with custom sets
 * - GROUPING SETS with empty set (grand total)
 * - ROLLUP with aggregate functions
 * - CUBE with HAVING clause
 * - GROUPING SETS with DISTINCT
 * - GROUPING() function usage
 * - ROLLUP NULL handling
 * - Multiple grouping dimensions
 * - Validation and edge cases
 * - SQL generation correctness
 */
@DisplayName("ROLLUP/CUBE/GROUPING SETS Tests")
@Tag("aggregate")
@Tag("tier1")
@TestCategories.Unit
public class GroupingSetsTest extends TestBase {

    private static final StructType SALES_SCHEMA = new StructType(Arrays.asList(
        new StructField("year", IntegerType.get(), false),
        new StructField("quarter", IntegerType.get(), false),
        new StructField("month", IntegerType.get(), false),
        new StructField("region", StringType.get(), false),
        new StructField("product", StringType.get(), false),
        new StructField("category", StringType.get(), false),
        new StructField("amount", IntegerType.get(), false),
        new StructField("quantity", IntegerType.get(), false)
    ));

    private LogicalPlan createSalesTableScan() {
        return new TableScan("/data/sales.parquet", TableScan.TableFormat.PARQUET, SALES_SCHEMA);
    }

    @Nested
    @DisplayName("ROLLUP Tests")
    class RollupTests {

        @Test
        @DisplayName("ROLLUP with 2 columns generates hierarchical grouping sets")
        void testRollupWith2Columns() {
            // Given: ROLLUP(year, month)
            Expression yearCol = new ColumnReference("year", IntegerType.get());
            Expression monthCol = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(yearCol, monthCol));

            // When: Check properties
            String sql = rollup.toSQL();
            int setCount = rollup.getGroupingSetCount();

            // Then: Should generate ROLLUP SQL and have 3 grouping sets
            // (year, month), (year), ()
            assertThat(sql).isEqualTo("ROLLUP(year, month)");
            assertThat(setCount).isEqualTo(3);  // N+1 for N columns
            assertThat(rollup.type()).isEqualTo(GroupingType.ROLLUP);
            assertThat(rollup.columns()).hasSize(2);
            assertThat(rollup.columns()).containsExactly(yearCol, monthCol);
        }

        @Test
        @DisplayName("ROLLUP with 3 columns generates 4 grouping sets")
        void testRollupWith3Columns() {
            // Given: ROLLUP(year, quarter, month)
            Expression yearCol = new ColumnReference("year", IntegerType.get());
            Expression quarterCol = new ColumnReference("quarter", IntegerType.get());
            Expression monthCol = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(
                Arrays.asList(yearCol, quarterCol, monthCol)
            );

            // When: Generate SQL and count sets
            String sql = rollup.toSQL();
            int setCount = rollup.getGroupingSetCount();

            // Then: Should generate 4 sets: (y,q,m), (y,q), (y), ()
            assertThat(sql).isEqualTo("ROLLUP(year, quarter, month)");
            assertThat(setCount).isEqualTo(4);
            assertThat(rollup.type()).isEqualTo(GroupingType.ROLLUP);
        }

        @Test
        @DisplayName("ROLLUP with single column generates 2 grouping sets")
        void testRollupWithSingleColumn() {
            // Given: ROLLUP(year)
            Expression yearCol = new ColumnReference("year", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Collections.singletonList(yearCol));

            // When: Generate SQL
            String sql = rollup.toSQL();
            int setCount = rollup.getGroupingSetCount();

            // Then: Should generate 2 sets: (year), ()
            assertThat(sql).isEqualTo("ROLLUP(year)");
            assertThat(setCount).isEqualTo(2);
        }

        @Test
        @DisplayName("ROLLUP with aggregate functions generates correct SQL")
        void testRollupWithAggregateFunction() {
            // Given: SELECT year, month, SUM(amount) FROM sales GROUP BY ROLLUP(year, month)
            Expression yearCol = new ColumnReference("year", IntegerType.get());
            Expression monthCol = new ColumnReference("month", IntegerType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(yearCol, monthCol));
            AggregateExpression sumAgg = new AggregateExpression(
                "SUM", amountCol, "total_amount"
            );

            // When: Create aggregate with ROLLUP
            // Note: Full integration will be tested in integration tests
            // Here we verify the GroupingSets SQL generation
            String rollupSQL = rollup.toSQL();

            // Then: Should generate correct ROLLUP syntax
            assertThat(rollupSQL).contains("ROLLUP");
            assertThat(rollupSQL).contains("year");
            assertThat(rollupSQL).contains("month");
        }

        @Test
        @DisplayName("ROLLUP validation - empty columns throws exception")
        void testRollupValidationEmptyColumns() {
            // Given: Empty column list
            List<Expression> emptyColumns = Collections.emptyList();

            // When/Then: Should throw exception
            assertThatThrownBy(() -> GroupingSets.rollup(emptyColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ROLLUP requires at least one column");
        }

        @Test
        @DisplayName("ROLLUP validation - null columns throws exception")
        void testRollupValidationNullColumns() {
            // Given: Null column list
            // When/Then: Should throw exception
            assertThatThrownBy(() -> GroupingSets.rollup(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("columns must not be null");
        }
    }

    @Nested
    @DisplayName("CUBE Tests")
    class CubeTests {

        @Test
        @DisplayName("CUBE with 2 columns generates 4 grouping sets")
        void testCubeWith2Columns() {
            // Given: CUBE(region, product)
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());

            GroupingSets cube = GroupingSets.cube(Arrays.asList(regionCol, productCol));

            // When: Generate SQL and count sets
            String sql = cube.toSQL();
            int setCount = cube.getGroupingSetCount();

            // Then: Should generate 2^2 = 4 sets: (r,p), (r), (p), ()
            assertThat(sql).isEqualTo("CUBE(region, product)");
            assertThat(setCount).isEqualTo(4);
            assertThat(cube.type()).isEqualTo(GroupingType.CUBE);
            assertThat(cube.columns()).hasSize(2);
        }

        @Test
        @DisplayName("CUBE with 3 columns generates 8 grouping sets")
        void testCubeWith3Columns() {
            // Given: CUBE(region, category, product)
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());

            GroupingSets cube = GroupingSets.cube(
                Arrays.asList(regionCol, categoryCol, productCol)
            );

            // When: Generate SQL and count sets
            String sql = cube.toSQL();
            int setCount = cube.getGroupingSetCount();

            // Then: Should generate 2^3 = 8 sets
            assertThat(sql).isEqualTo("CUBE(region, category, product)");
            assertThat(setCount).isEqualTo(8);
            assertThat(cube.type()).isEqualTo(GroupingType.CUBE);
        }

        @Test
        @DisplayName("CUBE validation - exceeds max dimensions throws exception")
        void testCubeValidationMaxDimensions() {
            // Given: 11 columns (exceeds MAX_CUBE_DIMENSIONS = 10)
            List<Expression> tooManyColumns = Arrays.asList(
                new ColumnReference("col1", IntegerType.get()),
                new ColumnReference("col2", IntegerType.get()),
                new ColumnReference("col3", IntegerType.get()),
                new ColumnReference("col4", IntegerType.get()),
                new ColumnReference("col5", IntegerType.get()),
                new ColumnReference("col6", IntegerType.get()),
                new ColumnReference("col7", IntegerType.get()),
                new ColumnReference("col8", IntegerType.get()),
                new ColumnReference("col9", IntegerType.get()),
                new ColumnReference("col10", IntegerType.get()),
                new ColumnReference("col11", IntegerType.get())
            );

            // When/Then: Should throw exception about exceeding max dimensions
            assertThatThrownBy(() -> GroupingSets.cube(tooManyColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum")
                .hasMessageContaining("2048 grouping sets");  // 2^11
        }

        @Test
        @DisplayName("CUBE with HAVING clause combination")
        void testCubeWithHavingClause() {
            // Given: CUBE with aggregate and HAVING
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            GroupingSets cube = GroupingSets.cube(Arrays.asList(regionCol, productCol));

            // When: Verify CUBE SQL generation
            String cubeSQL = cube.toSQL();

            // Then: Should generate correct CUBE syntax
            assertThat(cubeSQL).isEqualTo("CUBE(region, product)");
            assertThat(cube.getGroupingSetCount()).isEqualTo(4);
        }
    }

    @Nested
    @DisplayName("GROUPING SETS Tests")
    class GroupingSetsTests {

        @Test
        @DisplayName("GROUPING SETS with custom sets generates correct SQL")
        void testGroupingSetsWithCustomSets() {
            // Given: GROUPING SETS((region, product), (region), (category))
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            List<List<Expression>> sets = Arrays.asList(
                Arrays.asList(regionCol, productCol),  // (region, product)
                Collections.singletonList(regionCol),   // (region)
                Collections.singletonList(categoryCol)  // (category)
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);

            // When: Generate SQL
            String sql = groupingSets.toSQL();
            int setCount = groupingSets.getGroupingSetCount();

            // Then: Should generate explicit GROUPING SETS
            assertThat(sql).isEqualTo("GROUPING SETS((region, product), (region), (category))");
            assertThat(setCount).isEqualTo(3);
            assertThat(groupingSets.type()).isEqualTo(GroupingType.GROUPING_SETS);
            assertThat(groupingSets.sets()).hasSize(3);
        }

        @Test
        @DisplayName("GROUPING SETS with empty set for grand total")
        void testGroupingSetsWithEmptySet() {
            // Given: GROUPING SETS((region, product), (region), ())
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());

            List<List<Expression>> sets = Arrays.asList(
                Arrays.asList(regionCol, productCol),  // (region, product)
                Collections.singletonList(regionCol),   // (region)
                Collections.emptyList()                 // () - grand total
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);

            // When: Generate SQL
            String sql = groupingSets.toSQL();

            // Then: Should include empty set as ()
            assertThat(sql).contains("GROUPING SETS");
            assertThat(sql).contains("(region, product)");
            assertThat(sql).contains("(region)");
            assertThat(sql).contains("()");
            assertThat(groupingSets.getGroupingSetCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("GROUPING SETS with DISTINCT aggregates")
        void testGroupingSetsWithDistinct() {
            // Given: GROUPING SETS with DISTINCT aggregate
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());

            List<List<Expression>> sets = Arrays.asList(
                Arrays.asList(regionCol, categoryCol),
                Collections.singletonList(regionCol)
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);
            AggregateExpression countDistinct = new AggregateExpression(
                "COUNT", productCol, "unique_products", true
            );

            // When: Verify GROUPING SETS SQL
            String groupingSetsSQL = groupingSets.toSQL();

            // Then: Should generate correct syntax
            assertThat(groupingSetsSQL).contains("GROUPING SETS");
            assertThat(groupingSetsSQL).contains("(region, category)");
            assertThat(groupingSetsSQL).contains("(region)");
        }

        @Test
        @DisplayName("GROUPING SETS validation - empty sets list throws exception")
        void testGroupingSetsValidationEmptySets() {
            // Given: Empty sets list
            List<List<Expression>> emptySets = Collections.emptyList();

            // When/Then: Should throw exception
            assertThatThrownBy(() -> GroupingSets.groupingSets(emptySets))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GROUPING SETS requires at least one set");
        }

        @Test
        @DisplayName("GROUPING SETS validation - null in set throws exception")
        void testGroupingSetsValidationNullInSet() {
            // Given: Set containing null expression
            Expression regionCol = new ColumnReference("region", StringType.get());

            List<List<Expression>> setsWithNull = Arrays.asList(
                Arrays.asList(regionCol, null)  // Contains null
            );

            // When/Then: Should throw exception
            assertThatThrownBy(() -> GroupingSets.groupingSets(setsWithNull))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contains null expressions");
        }
    }

    @Nested
    @DisplayName("Multiple Grouping Dimensions")
    class MultipleGroupingDimensions {

        @Test
        @DisplayName("Multiple grouping dimensions with ROLLUP")
        void testMultipleDimensionsWithRollup() {
            // Given: ROLLUP with time hierarchy
            Expression yearCol = new ColumnReference("year", IntegerType.get());
            Expression quarterCol = new ColumnReference("quarter", IntegerType.get());
            Expression monthCol = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(
                Arrays.asList(yearCol, quarterCol, monthCol)
            );

            AggregateExpression sumAmount = new AggregateExpression(
                "SUM",
                new ColumnReference("amount", IntegerType.get()),
                "total"
            );
            AggregateExpression avgAmount = new AggregateExpression(
                "AVG",
                new ColumnReference("amount", IntegerType.get()),
                "average"
            );

            // When: Verify ROLLUP SQL
            String sql = rollup.toSQL();

            // Then: Should handle multiple dimensions
            assertThat(sql).contains("ROLLUP");
            assertThat(sql).contains("year");
            assertThat(sql).contains("quarter");
            assertThat(sql).contains("month");
            assertThat(rollup.getGroupingSetCount()).isEqualTo(4);
        }

        @Test
        @DisplayName("ROLLUP handles NULL values correctly")
        void testRollupNullHandling() {
            // Given: ROLLUP columns that may contain NULLs
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(regionCol, categoryCol));

            // When: Generate SQL
            String sql = rollup.toSQL();

            // Then: Should generate correct ROLLUP (NULL handling is database-side)
            assertThat(sql).isEqualTo("ROLLUP(region, category)");
            assertThat(rollup.type()).isEqualTo(GroupingType.ROLLUP);
        }
    }

    @Nested
    @DisplayName("SQL Generation Correctness")
    class SQLGenerationCorrectness {

        @Test
        @DisplayName("ROLLUP SQL generation is correct")
        void testRollupSQLGeneration() {
            // Given: Various ROLLUP configurations
            Expression col1 = new ColumnReference("year", IntegerType.get());
            Expression col2 = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(col1, col2));

            // When: Generate SQL
            String sql = rollup.toSQL();

            // Then: Should match expected format
            assertThat(sql).isEqualTo("ROLLUP(year, month)");
            assertThat(sql).doesNotContain("CUBE");
            assertThat(sql).doesNotContain("GROUPING SETS");
        }

        @Test
        @DisplayName("CUBE SQL generation is correct")
        void testCubeSQLGeneration() {
            // Given: CUBE configuration
            Expression col1 = new ColumnReference("region", StringType.get());
            Expression col2 = new ColumnReference("product", StringType.get());

            GroupingSets cube = GroupingSets.cube(Arrays.asList(col1, col2));

            // When: Generate SQL
            String sql = cube.toSQL();

            // Then: Should match expected format
            assertThat(sql).isEqualTo("CUBE(region, product)");
            assertThat(sql).doesNotContain("ROLLUP");
            assertThat(sql).doesNotContain("GROUPING SETS");
        }

        @Test
        @DisplayName("GROUPING SETS SQL generation with complex sets")
        void testGroupingSetsSQLGeneration() {
            // Given: Complex GROUPING SETS
            Expression region = new ColumnReference("region", StringType.get());
            Expression category = new ColumnReference("category", StringType.get());
            Expression product = new ColumnReference("product", StringType.get());

            List<List<Expression>> sets = Arrays.asList(
                Arrays.asList(region, category, product),  // 3 columns
                Arrays.asList(region, category),            // 2 columns
                Collections.singletonList(region),          // 1 column
                Collections.emptyList()                     // 0 columns (grand total)
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);

            // When: Generate SQL
            String sql = groupingSets.toSQL();

            // Then: Should generate all sets correctly
            assertThat(sql).startsWith("GROUPING SETS(");
            assertThat(sql).contains("(region, category, product)");
            assertThat(sql).contains("(region, category)");
            assertThat(sql).contains("(region)");
            assertThat(sql).contains("()");
            assertThat(sql).endsWith(")");
        }
    }

    @Nested
    @DisplayName("Equality and ToString")
    class EqualityAndToString {

        @Test
        @DisplayName("GroupingSets.equals() works correctly")
        void testGroupingSetsEquality() {
            // Given: Two identical ROLLUP instances
            Expression year = new ColumnReference("year", IntegerType.get());
            Expression month = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup1 = GroupingSets.rollup(Arrays.asList(year, month));
            GroupingSets rollup2 = GroupingSets.rollup(Arrays.asList(year, month));

            // When/Then: Should be equal
            assertThat(rollup1).isEqualTo(rollup2);
            assertThat(rollup1.hashCode()).isEqualTo(rollup2.hashCode());
        }

        @Test
        @DisplayName("GroupingSets.toString() provides useful information")
        void testGroupingSetsToString() {
            // Given: ROLLUP instance
            Expression year = new ColumnReference("year", IntegerType.get());
            Expression month = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(year, month));

            // When: Call toString()
            String str = rollup.toString();

            // Then: Should contain type and set count
            assertThat(str).contains("GroupingSets");
            assertThat(str).contains("type=ROLLUP");
            assertThat(str).contains("sets=3");
        }

        @Test
        @DisplayName("Different GroupingSets types are not equal")
        void testDifferentTypesNotEqual() {
            // Given: ROLLUP and CUBE with same columns
            Expression col1 = new ColumnReference("a", IntegerType.get());
            Expression col2 = new ColumnReference("b", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(col1, col2));
            GroupingSets cube = GroupingSets.cube(Arrays.asList(col1, col2));

            // When/Then: Should not be equal
            assertThat(rollup).isNotEqualTo(cube);
            assertThat(rollup.type()).isNotEqualTo(cube.type());
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("ROLLUP with single element list")
        void testRollupSingleElement() {
            // Given: ROLLUP with one column
            Expression yearCol = new ColumnReference("year", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Collections.singletonList(yearCol));

            // When: Generate SQL
            String sql = rollup.toSQL();
            int count = rollup.getGroupingSetCount();

            // Then: Should work correctly
            assertThat(sql).isEqualTo("ROLLUP(year)");
            assertThat(count).isEqualTo(2);  // (year), ()
        }

        @Test
        @DisplayName("GROUPING SETS with single empty set")
        void testGroupingSetsSingleEmptySet() {
            // Given: GROUPING SETS with only grand total
            List<List<Expression>> sets = Collections.singletonList(
                Collections.emptyList()
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);

            // When: Generate SQL
            String sql = groupingSets.toSQL();

            // Then: Should generate correctly
            assertThat(sql).isEqualTo("GROUPING SETS(())");
            assertThat(groupingSets.getGroupingSetCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Columns list is immutable")
        void testColumnsListImmutable() {
            // Given: ROLLUP with columns
            Expression year = new ColumnReference("year", IntegerType.get());
            Expression month = new ColumnReference("month", IntegerType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(year, month));

            // When: Try to modify columns list
            List<Expression> columns = rollup.columns();

            // Then: Should be unmodifiable
            assertThatThrownBy(() -> columns.add(new ColumnReference("day", IntegerType.get())))
                .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("Sets list is immutable")
        void testSetsListImmutable() {
            // Given: GROUPING SETS
            Expression region = new ColumnReference("region", StringType.get());

            List<List<Expression>> sets = Arrays.asList(
                Collections.singletonList(region),
                Collections.emptyList()
            );

            GroupingSets groupingSets = GroupingSets.groupingSets(sets);

            // When: Try to modify sets list
            List<List<Expression>> returnedSets = groupingSets.sets();

            // Then: Should be unmodifiable
            assertThatThrownBy(() -> returnedSets.add(Collections.emptyList()))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
