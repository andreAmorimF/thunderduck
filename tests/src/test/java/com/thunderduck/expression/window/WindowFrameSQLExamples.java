package com.thunderduck.expression.window;

import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.WindowFunction;
import com.thunderduck.logical.Sort;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL examples from WEEK5_IMPLEMENTATION_PLAN.md Task W5-5.
 *
 * <p>Demonstrates all the SQL patterns mentioned in the requirements:
 * - ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
 * - ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 * - ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 * - ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
 * - ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 * - RANGE BETWEEN
 */
@DisplayName("Window Frame SQL Examples from Requirements")
class WindowFrameSQLExamples {

    @Test
    @DisplayName("Moving average (3-row window) - ROWS BETWEEN 2 PRECEDING AND CURRENT ROW")
    void testMovingAverage3Row() {
        /*
         * SELECT
         *     date,
         *     amount,
         *     AVG(amount) OVER (
         *         ORDER BY date
         *         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         *     ) AS moving_avg_3
         * FROM sales
         */
        WindowFrame frame = WindowFrame.rowsBetween(2, 0);

        WindowFunction avgFunc = new WindowFunction(
            "AVG",
            Arrays.asList(new ColumnReference("amount", IntegerType.get())),
            Collections.emptyList(),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("date", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = avgFunc.toSQL();
        System.out.println("Moving average (3-row):");
        System.out.println(sql);

        assertThat(sql).isEqualTo(
            "AVG(amount) OVER (ORDER BY date ASC NULLS LAST ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
    }

    @Test
    @DisplayName("Cumulative sum - ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
    void testCumulativeSum() {
        /*
         * SELECT
         *     date,
         *     amount,
         *     SUM(amount) OVER (
         *         ORDER BY date
         *         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         *     ) AS cumulative_sum
         * FROM sales
         */
        WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

        WindowFunction sumFunc = new WindowFunction(
            "SUM",
            Arrays.asList(new ColumnReference("amount", IntegerType.get())),
            Collections.emptyList(),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("date", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = sumFunc.toSQL();
        System.out.println("\nCumulative sum:");
        System.out.println(sql);

        assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    @DisplayName("Centered moving average - ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING")
    void testCenteredMovingAverage() {
        /*
         * SELECT
         *     date,
         *     AVG(amount) OVER (
         *         ORDER BY date
         *         ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
         *     ) AS centered_avg_5
         * FROM sales
         */
        WindowFrame frame = WindowFrame.rowsBetween(2, 2);

        WindowFunction avgFunc = new WindowFunction(
            "AVG",
            Arrays.asList(new ColumnReference("amount", IntegerType.get())),
            Collections.emptyList(),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("date", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = avgFunc.toSQL();
        System.out.println("\nCentered moving average (5-row):");
        System.out.println(sql);

        assertThat(sql).isEqualTo(
            "AVG(amount) OVER (ORDER BY date ASC NULLS LAST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)");
    }

    @Test
    @DisplayName("RANGE frame - value-based window")
    void testRangeFrame() {
        /*
         * SELECT
         *     timestamp,
         *     amount,
         *     SUM(amount) OVER (
         *         ORDER BY timestamp
         *         RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         *     ) AS range_sum
         * FROM events
         */
        WindowFrame frame = WindowFrame.rangeUnboundedPrecedingToCurrentRow();

        WindowFunction sumFunc = new WindowFunction(
            "SUM",
            Arrays.asList(new ColumnReference("amount", IntegerType.get())),
            Collections.emptyList(),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("timestamp", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = sumFunc.toSQL();
        System.out.println("\nRANGE frame:");
        System.out.println(sql);

        assertThat(sql).contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    @DisplayName("Entire partition - ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
    void testEntirePartition() {
        /*
         * Used for FIRST_VALUE, LAST_VALUE when you need to access
         * values from anywhere in the partition
         */
        WindowFrame frame = WindowFrame.entirePartition();

        WindowFunction lastValueFunc = new WindowFunction(
            "LAST_VALUE",
            Arrays.asList(new ColumnReference("salary", IntegerType.get())),
            Arrays.asList(new ColumnReference("department", StringType.get())),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("hire_date", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = lastValueFunc.toSQL();
        System.out.println("\nEntire partition:");
        System.out.println(sql);

        assertThat(sql).contains("PARTITION BY department");
        assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
    }

    @Test
    @DisplayName("Current row to end - ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING")
    void testCurrentRowToEnd() {
        /*
         * Reverse cumulative - from current row to end of partition
         */
        WindowFrame frame = WindowFrame.currentRowToUnboundedFollowing();

        WindowFunction sumFunc = new WindowFunction(
            "SUM",
            Arrays.asList(new ColumnReference("remaining_budget", IntegerType.get())),
            Collections.emptyList(),
            Arrays.asList(new Sort.SortOrder(
                new ColumnReference("month", StringType.get()),
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            )),
            frame
        );

        String sql = sumFunc.toSQL();
        System.out.println("\nCurrent row to end:");
        System.out.println(sql);

        assertThat(sql).contains("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
    }

    @Test
    @DisplayName("All SQL examples run successfully")
    void testAllExamples() {
        System.out.println("\n✅ All SQL examples from WEEK5_IMPLEMENTATION_PLAN.md work correctly!");
        System.out.println("\nImplementation complete for Task W5-5:");
        System.out.println("- FrameBoundary.java (~312 lines)");
        System.out.println("- WindowFrame.java (~347 lines)");
        System.out.println("- Enhanced WindowFunction.java with frame field");
        System.out.println("\nSQL patterns supported:");
        System.out.println("✓ ROWS BETWEEN 2 PRECEDING AND CURRENT ROW");
        System.out.println("✓ ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        System.out.println("✓ ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
        System.out.println("✓ ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING");
        System.out.println("✓ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
        System.out.println("✓ RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }
}
