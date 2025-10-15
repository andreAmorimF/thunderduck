package com.catalyst2sql.expression.window;

import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.WindowFunction;
import com.catalyst2sql.logical.Sort;
import com.catalyst2sql.types.IntegerType;
import com.catalyst2sql.types.StringType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple demonstration test for Window Frame implementation (Task W5-5).
 */
@DisplayName("Window Frame Demo")
class SimpleWindowFrameDemo {

    @Test
    @DisplayName("Demonstrate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW")
    void testMovingAverage() {
        // Create window frame: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        WindowFrame frame = WindowFrame.rowsBetween(2, 0);

        // Create window function with frame
        WindowFunction windowFunc = new WindowFunction(
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

        String sql = windowFunc.toSQL();
        System.out.println("Generated SQL: " + sql);

        assertThat(sql).isEqualTo(
            "AVG(amount) OVER (ORDER BY date ASC NULLS LAST ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
    }

    @Test
    @DisplayName("Demonstrate cumulative sum")
    void testCumulativeSum() {
        WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

        WindowFunction windowFunc = new WindowFunction(
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

        String sql = windowFunc.toSQL();
        System.out.println("Cumulative sum SQL: " + sql);

        assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    @DisplayName("Demonstrate frame boundaries")
    void testFrameBoundaries() {
        // Test all boundary types
        FrameBoundary unboundedPreceding = FrameBoundary.UnboundedPreceding.getInstance();
        FrameBoundary unboundedFollowing = FrameBoundary.UnboundedFollowing.getInstance();
        FrameBoundary currentRow = FrameBoundary.CurrentRow.getInstance();
        FrameBoundary preceding5 = new FrameBoundary.Preceding(5);
        FrameBoundary following3 = new FrameBoundary.Following(3);

        assertThat(unboundedPreceding.toSQL()).isEqualTo("UNBOUNDED PRECEDING");
        assertThat(unboundedFollowing.toSQL()).isEqualTo("UNBOUNDED FOLLOWING");
        assertThat(currentRow.toSQL()).isEqualTo("CURRENT ROW");
        assertThat(preceding5.toSQL()).isEqualTo("5 PRECEDING");
        assertThat(following3.toSQL()).isEqualTo("3 FOLLOWING");

        System.out.println("All frame boundaries work correctly!");
    }

    @Test
    @DisplayName("Demonstrate centered moving average")
    void testCenteredWindow() {
        // ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        WindowFrame frame = WindowFrame.rowsBetween(2, 2);

        WindowFunction windowFunc = new WindowFunction(
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

        String sql = windowFunc.toSQL();
        System.out.println("Centered window SQL: " + sql);

        assertThat(sql).contains("ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING");
    }
}
