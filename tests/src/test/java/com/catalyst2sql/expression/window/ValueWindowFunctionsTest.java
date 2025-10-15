package com.catalyst2sql.expression.window;

import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.expression.WindowFunction;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.logical.Project;
import com.catalyst2sql.logical.Sort;
import com.catalyst2sql.logical.TableScan;
import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import com.catalyst2sql.types.DoubleType;
import com.catalyst2sql.types.IntegerType;
import com.catalyst2sql.types.StringType;
import com.catalyst2sql.types.StructField;
import com.catalyst2sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for value window functions (Week 5 Phase 2 Task W5-7).
 *
 * <p>Tests 14 scenarios covering:
 * - NTH_VALUE: Accessing the Nth value in a window frame
 * - PERCENT_RANK: Relative rank of a row (0-1 scale)
 * - CUME_DIST: Cumulative distribution (0-1 scale)
 * - NTILE: Dividing rows into buckets
 * - Edge cases: NULL handling, frame specifications, ordering
 *
 * <p>These functions are essential for advanced analytical queries.
 */
@DisplayName("Value Window Functions Tests")
@Tag("window")
@Tag("tier1")
@TestCategories.Unit
public class ValueWindowFunctionsTest extends TestBase {

    private static final StructType SALES_SCHEMA = new StructType(Arrays.asList(
        new StructField("sale_id", IntegerType.get(), false),
        new StructField("product_id", IntegerType.get(), false),
        new StructField("customer_id", IntegerType.get(), false),
        new StructField("amount", DoubleType.get(), false),
        new StructField("sale_date", StringType.get(), false),
        new StructField("region", StringType.get(), false)
    ));

    private LogicalPlan createSalesTableScan() {
        return new TableScan("/data/sales.parquet", TableScan.TableFormat.PARQUET, SALES_SCHEMA);
    }

    @Nested
    @DisplayName("NTH_VALUE Function")
    class NthValueFunction {

        @Test
        @DisplayName("NTH_VALUE with FIRST_VALUE equivalent generates correct SQL")
        void testNthValueFirstValue() {
            // Given: NTH_VALUE(amount, 1) - first value in window
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression nthLiteral = new Literal(1, IntegerType.get());

            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());
            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction nthValue = new WindowFunction(
                "NTH_VALUE",
                Arrays.asList(amountCol, nthLiteral),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                null
            );

            // When: Generate SQL
            String sql = nthValue.toSQL();

            // Then: Should generate NTH_VALUE(amount, 1)
            assertThat(sql).contains("NTH_VALUE(amount, 1)");
            assertThat(sql).contains("ORDER BY sale_date ASC");
        }

        @Test
        @DisplayName("NTH_VALUE with Nth position generates correct SQL")
        void testNthValueNthPosition() {
            // Given: NTH_VALUE(amount, 3) OVER (PARTITION BY region ORDER BY sale_date)
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression nthLiteral = new Literal(3, IntegerType.get());
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction nthValue = new WindowFunction(
                "NTH_VALUE",
                Arrays.asList(amountCol, nthLiteral),
                Collections.singletonList(regionCol),
                Collections.singletonList(dateAsc),
                null
            );

            // When: Generate SQL
            String sql = nthValue.toSQL();

            // Then: Should generate full window specification
            assertThat(sql).contains("NTH_VALUE(amount, 3)");
            assertThat(sql).contains("PARTITION BY region");
            assertThat(sql).contains("ORDER BY sale_date ASC");
        }

        @Test
        @DisplayName("NTH_VALUE with frame specification generates correct SQL")
        void testNthValueWithFrame() {
            // Given: NTH_VALUE with explicit frame
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression nthLiteral = new Literal(2, IntegerType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFrame frame = WindowFrame.rowsBetween(0, 5);

            WindowFunction nthValue = new WindowFunction(
                "NTH_VALUE",
                Arrays.asList(amountCol, nthLiteral),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                frame
            );

            // When: Generate SQL
            String sql = nthValue.toSQL();

            // Then: Should include frame specification
            assertThat(sql).contains("NTH_VALUE(amount, 2)");
            assertThat(sql).contains("ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING");
        }

        @Test
        @DisplayName("NTH_VALUE in complete query generates valid SQL")
        void testNthValueInCompleteQuery() {
            // Given: SELECT with NTH_VALUE
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression nthLiteral = new Literal(1, IntegerType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction firstAmount = new WindowFunction(
                "NTH_VALUE",
                Arrays.asList(amountCol, nthLiteral),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                null
            );

            LogicalPlan tableScan = createSalesTableScan();
            Project project = new Project(
                tableScan,
                Arrays.asList(saleDateCol, amountCol, firstAmount)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(project);

            // Then: Should be valid SQL
            assertThat(sql).contains("SELECT");
            assertThat(sql).contains("NTH_VALUE(amount, 1)");
            assertThat(sql).contains("ORDER BY sale_date ASC");
        }
    }

    @Nested
    @DisplayName("PERCENT_RANK Function")
    class PercentRankFunction {

        @Test
        @DisplayName("PERCENT_RANK with simple ordering generates correct SQL")
        void testPercentRankSimple() {
            // Given: PERCENT_RANK() OVER (ORDER BY amount DESC)
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String sql = percentRank.toSQL();

            // Then: Should generate PERCENT_RANK()
            assertThat(sql).isEqualTo("PERCENT_RANK() OVER (ORDER BY amount DESC NULLS LAST)");
        }

        @Test
        @DisplayName("PERCENT_RANK with PARTITION BY generates correct SQL")
        void testPercentRankWithPartition() {
            // Given: PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount DESC)
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.emptyList(),
                Collections.singletonList(regionCol),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String sql = percentRank.toSQL();

            // Then: Should include PARTITION BY
            assertThat(sql).contains("PERCENT_RANK()");
            assertThat(sql).contains("PARTITION BY region");
            assertThat(sql).contains("ORDER BY amount DESC");
        }

        @Test
        @DisplayName("PERCENT_RANK without ORDER BY should still generate valid SQL")
        void testPercentRankWithoutOrderBy() {
            // Given: PERCENT_RANK() without ORDER BY (edge case, may be invalid in some DBs)
            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                null
            );

            // When: Generate SQL
            String sql = percentRank.toSQL();

            // Then: Should generate basic PERCENT_RANK
            assertThat(sql).isEqualTo("PERCENT_RANK() OVER ()");
        }
    }

    @Nested
    @DisplayName("CUME_DIST Function")
    class CumeDistFunction {

        @Test
        @DisplayName("CUME_DIST with simple ordering generates correct SQL")
        void testCumeDistSimple() {
            // Given: CUME_DIST() OVER (ORDER BY amount)
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Sort.SortOrder amountAsc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction cumeDist = new WindowFunction(
                "CUME_DIST",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(amountAsc),
                null
            );

            // When: Generate SQL
            String sql = cumeDist.toSQL();

            // Then: Should generate CUME_DIST()
            assertThat(sql).isEqualTo("CUME_DIST() OVER (ORDER BY amount ASC NULLS FIRST)");
        }

        @Test
        @DisplayName("CUME_DIST with PARTITION BY generates correct SQL")
        void testCumeDistWithPartition() {
            // Given: CUME_DIST() OVER (PARTITION BY product_id ORDER BY sale_date)
            Expression productIdCol = new ColumnReference("product_id", IntegerType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction cumeDist = new WindowFunction(
                "CUME_DIST",
                Collections.emptyList(),
                Collections.singletonList(productIdCol),
                Collections.singletonList(dateAsc),
                null
            );

            // When: Generate SQL
            String sql = cumeDist.toSQL();

            // Then: Should include PARTITION BY
            assertThat(sql).contains("CUME_DIST()");
            assertThat(sql).contains("PARTITION BY product_id");
            assertThat(sql).contains("ORDER BY sale_date ASC");
        }

        @Test
        @DisplayName("CUME_DIST and PERCENT_RANK together in query")
        void testCumeDistWithPercentRank() {
            // Given: Both CUME_DIST and PERCENT_RANK in same window spec
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction cumeDist = new WindowFunction(
                "CUME_DIST",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String cumeDistSQL = cumeDist.toSQL();
            String percentRankSQL = percentRank.toSQL();

            // Then: Both should have same window spec
            assertThat(cumeDistSQL).contains("CUME_DIST() OVER (ORDER BY amount DESC");
            assertThat(percentRankSQL).contains("PERCENT_RANK() OVER (ORDER BY amount DESC");
        }
    }

    @Nested
    @DisplayName("NTILE Function")
    class NtileFunction {

        @Test
        @DisplayName("NTILE with bucket count generates correct SQL")
        void testNtileWithBuckets() {
            // Given: NTILE(4) OVER (ORDER BY amount DESC) - quartiles
            Expression bucketsLiteral = new Literal(4, IntegerType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction ntile = new WindowFunction(
                "NTILE",
                Collections.singletonList(bucketsLiteral),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String sql = ntile.toSQL();

            // Then: Should generate NTILE(4)
            assertThat(sql).contains("NTILE(4)");
            assertThat(sql).contains("ORDER BY amount DESC");
        }

        @Test
        @DisplayName("NTILE with PARTITION BY generates correct SQL")
        void testNtileWithPartition() {
            // Given: NTILE(10) OVER (PARTITION BY region ORDER BY amount) - deciles per region
            Expression bucketsLiteral = new Literal(10, IntegerType.get());
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            Sort.SortOrder amountAsc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFunction ntile = new WindowFunction(
                "NTILE",
                Collections.singletonList(bucketsLiteral),
                Collections.singletonList(regionCol),
                Collections.singletonList(amountAsc),
                null
            );

            // When: Generate SQL
            String sql = ntile.toSQL();

            // Then: Should include PARTITION BY
            assertThat(sql).contains("NTILE(10)");
            assertThat(sql).contains("PARTITION BY region");
            assertThat(sql).contains("ORDER BY amount ASC");
        }

        @Test
        @DisplayName("NTILE for quartiles, deciles, percentiles")
        void testNtileCommonBuckets() {
            // Given: Common NTILE bucket sizes
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction quartiles = new WindowFunction(
                "NTILE",
                Collections.singletonList(new Literal(4, IntegerType.get())),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            WindowFunction deciles = new WindowFunction(
                "NTILE",
                Collections.singletonList(new Literal(10, IntegerType.get())),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            WindowFunction percentiles = new WindowFunction(
                "NTILE",
                Collections.singletonList(new Literal(100, IntegerType.get())),
                Collections.emptyList(),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String quartilesSQL = quartiles.toSQL();
            String decilesSQL = deciles.toSQL();
            String percentilesSQL = percentiles.toSQL();

            // Then: Should generate correct bucket counts
            assertThat(quartilesSQL).contains("NTILE(4)");
            assertThat(decilesSQL).contains("NTILE(10)");
            assertThat(percentilesSQL).contains("NTILE(100)");
        }
    }

    @Nested
    @DisplayName("Edge Cases and Integration")
    class EdgeCasesAndIntegration {

        @Test
        @DisplayName("All value window functions uppercased in SQL")
        void testFunctionNameUppercasing() {
            // Given: Functions with lowercase names
            WindowFunction nthValue = new WindowFunction(
                "nth_value",
                Arrays.asList(
                    new ColumnReference("amount", DoubleType.get()),
                    new Literal(1, IntegerType.get())
                ),
                Collections.emptyList(),
                Collections.singletonList(
                    new Sort.SortOrder(
                        new ColumnReference("sale_date", StringType.get()),
                        Sort.SortDirection.ASCENDING
                    )
                ),
                null
            );

            // When: Generate SQL
            String sql = nthValue.toSQL();

            // Then: Should be uppercased
            assertThat(sql).startsWith("NTH_VALUE(");
            assertThat(sql).doesNotContain("nth_value");
        }

        @Test
        @DisplayName("Value window functions preserve data types")
        void testDataTypePreservation() {
            // Given: Window functions
            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(
                    new Sort.SortOrder(
                        new ColumnReference("amount", DoubleType.get()),
                        Sort.SortDirection.DESCENDING
                    )
                ),
                null
            );

            // When: Check nullable
            // Then: Window functions return nullable values
            assertThat(percentRank.nullable()).isTrue();
        }
    }
}
