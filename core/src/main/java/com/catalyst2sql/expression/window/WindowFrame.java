package com.catalyst2sql.expression.window;

import java.util.Objects;

/**
 * Represents a window frame specification in SQL window functions.
 *
 * <p>A window frame defines which rows are included in a window function's
 * computation relative to the current row. It is specified using:
 * <ul>
 *   <li>Frame type: ROWS, RANGE, or GROUPS</li>
 *   <li>Start boundary: where the frame begins</li>
 *   <li>End boundary: where the frame ends</li>
 * </ul>
 *
 * <p><b>Frame Types:</b>
 * <ul>
 *   <li><b>ROWS</b> - Physical rows based on row position</li>
 *   <li><b>RANGE</b> - Logical range based on value equality (requires ORDER BY)</li>
 *   <li><b>GROUPS</b> - Groups of peer rows (rows with same ORDER BY values)</li>
 * </ul>
 *
 * <p><b>Common Frame Patterns:</b>
 * <pre>
 * // Moving average (3 rows: 2 before + current)
 * WindowFrame.rowsBetween(2, 0)
 * SQL: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
 *
 * // Cumulative sum from start
 * WindowFrame.unboundedPrecedingToCurrentRow()
 * SQL: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *
 * // All rows to the end
 * WindowFrame.currentRowToUnboundedFollowing()
 * SQL: ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 *
 * // Centered window (2 before, 2 after)
 * WindowFrame.rowsBetween(2, 2)
 * SQL: ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
 *
 * // Entire partition
 * WindowFrame.entirePartition()
 * SQL: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 * </pre>
 *
 * <p><b>Usage Example:</b>
 * <pre>
 * // Create a 3-day moving average window
 * WindowFrame frame = WindowFrame.rowsBetween(2, 0);
 *
 * // Use with window function
 * WindowFunction avgFunc = new WindowFunction(
 *     "AVG",
 *     Arrays.asList(new ColumnReference("amount")),
 *     partitionBy,
 *     orderBy,
 *     frame  // Add frame specification
 * );
 * </pre>
 *
 * @see FrameBoundary
 */
public final class WindowFrame {

    /**
     * Enumeration of window frame types.
     */
    public enum FrameType {
        /**
         * ROWS - Frame based on physical row positions.
         * Each row is counted individually regardless of value equality.
         */
        ROWS,

        /**
         * RANGE - Frame based on value ranges.
         * Includes all rows with ORDER BY values within the specified range.
         * Requires ORDER BY clause.
         */
        RANGE,

        /**
         * GROUPS - Frame based on peer groups.
         * Groups rows with identical ORDER BY values together.
         * Requires ORDER BY clause.
         */
        GROUPS
    }

    private final FrameType type;
    private final FrameBoundary start;
    private final FrameBoundary end;

    /**
     * Creates a window frame specification.
     *
     * @param type the frame type (ROWS, RANGE, or GROUPS)
     * @param start the start boundary
     * @param end the end boundary
     * @throws IllegalArgumentException if validation fails
     */
    public WindowFrame(FrameType type, FrameBoundary start, FrameBoundary end) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.start = Objects.requireNonNull(start, "start must not be null");
        this.end = Objects.requireNonNull(end, "end must not be null");

        // Validate frame boundaries
        validate();
    }

    /**
     * Validates the frame specification.
     *
     * @throws IllegalArgumentException if the frame is invalid
     */
    private void validate() {
        // Cannot start at UNBOUNDED FOLLOWING
        if (start instanceof FrameBoundary.UnboundedFollowing) {
            throw new IllegalArgumentException(
                "Frame cannot start at UNBOUNDED FOLLOWING");
        }

        // Cannot end at UNBOUNDED PRECEDING
        if (end instanceof FrameBoundary.UnboundedPreceding) {
            throw new IllegalArgumentException(
                "Frame cannot end at UNBOUNDED PRECEDING");
        }

        // Start must come before end (logical ordering)
        validateBoundaryOrder();
    }

    /**
     * Validates that the start boundary comes before the end boundary.
     */
    private void validateBoundaryOrder() {
        int startOrder = getBoundaryOrder(start);
        int endOrder = getBoundaryOrder(end);

        if (startOrder > endOrder) {
            throw new IllegalArgumentException(
                String.format("Invalid frame: start (%s) must come before end (%s)",
                    start.toSQL(), end.toSQL()));
        }
    }

    /**
     * Returns a numeric ordering for boundary comparison.
     * Lower numbers come before higher numbers.
     */
    private int getBoundaryOrder(FrameBoundary boundary) {
        if (boundary instanceof FrameBoundary.UnboundedPreceding) {
            return 0;
        } else if (boundary instanceof FrameBoundary.Preceding) {
            // Larger offset = earlier position (further back)
            return 1000 - (int) ((FrameBoundary.Preceding) boundary).offset();
        } else if (boundary instanceof FrameBoundary.CurrentRow) {
            return 1000;
        } else if (boundary instanceof FrameBoundary.Following) {
            return 1000 + (int) ((FrameBoundary.Following) boundary).offset();
        } else if (boundary instanceof FrameBoundary.UnboundedFollowing) {
            return 2000;
        }
        throw new IllegalArgumentException("Unknown boundary type: " + boundary);
    }

    /**
     * Returns the frame type.
     *
     * @return the frame type
     */
    public FrameType type() {
        return type;
    }

    /**
     * Returns the start boundary.
     *
     * @return the start boundary
     */
    public FrameBoundary start() {
        return start;
    }

    /**
     * Returns the end boundary.
     *
     * @return the end boundary
     */
    public FrameBoundary end() {
        return end;
    }

    /**
     * Converts this window frame to SQL syntax.
     *
     * @return SQL representation of the frame
     */
    public String toSQL() {
        StringBuilder sql = new StringBuilder();

        sql.append(type.name());
        sql.append(" BETWEEN ");
        sql.append(start.toSQL());
        sql.append(" AND ");
        sql.append(end.toSQL());

        return sql.toString();
    }

    // Factory methods for common frame patterns

    /**
     * Creates a ROWS frame from UNBOUNDED PRECEDING to CURRENT ROW.
     * This is useful for cumulative aggregations (running totals, cumulative sums).
     *
     * <p>SQL: {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}
     *
     * @return a frame from the start of the partition to the current row
     */
    public static WindowFrame unboundedPrecedingToCurrentRow() {
        return new WindowFrame(
            FrameType.ROWS,
            FrameBoundary.UnboundedPreceding.getInstance(),
            FrameBoundary.CurrentRow.getInstance()
        );
    }

    /**
     * Creates a ROWS frame from CURRENT ROW to UNBOUNDED FOLLOWING.
     * This includes the current row and all subsequent rows.
     *
     * <p>SQL: {@code ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING}
     *
     * @return a frame from the current row to the end of the partition
     */
    public static WindowFrame currentRowToUnboundedFollowing() {
        return new WindowFrame(
            FrameType.ROWS,
            FrameBoundary.CurrentRow.getInstance(),
            FrameBoundary.UnboundedFollowing.getInstance()
        );
    }

    /**
     * Creates a ROWS frame covering the entire partition.
     * This is useful when you need to compute aggregates over all rows.
     *
     * <p>SQL: {@code ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING}
     *
     * @return a frame covering the entire partition
     */
    public static WindowFrame entirePartition() {
        return new WindowFrame(
            FrameType.ROWS,
            FrameBoundary.UnboundedPreceding.getInstance(),
            FrameBoundary.UnboundedFollowing.getInstance()
        );
    }

    /**
     * Creates a ROWS frame with specified preceding and following offsets.
     *
     * <p>Examples:
     * <pre>
     * // 3-row moving window: 2 PRECEDING to CURRENT ROW
     * rowsBetween(2, 0)
     *
     * // 5-row centered window: 2 PRECEDING to 2 FOLLOWING
     * rowsBetween(2, 2)
     *
     * // 7-day moving window (with ORDER BY date)
     * rowsBetween(6, 0)
     * </pre>
     *
     * @param precedingOffset rows before current (0 for CURRENT ROW start)
     * @param followingOffset rows after current (0 for CURRENT ROW end)
     * @return a ROWS frame with the specified boundaries
     */
    public static WindowFrame rowsBetween(long precedingOffset, long followingOffset) {
        FrameBoundary start = precedingOffset == 0
            ? FrameBoundary.CurrentRow.getInstance()
            : new FrameBoundary.Preceding(precedingOffset);

        FrameBoundary end = followingOffset == 0
            ? FrameBoundary.CurrentRow.getInstance()
            : new FrameBoundary.Following(followingOffset);

        return new WindowFrame(FrameType.ROWS, start, end);
    }

    /**
     * Creates a RANGE frame from UNBOUNDED PRECEDING to CURRENT ROW.
     * This is useful for value-based cumulative aggregations.
     *
     * <p>SQL: {@code RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}
     *
     * @return a RANGE frame from the start of the partition to the current row
     */
    public static WindowFrame rangeUnboundedPrecedingToCurrentRow() {
        return new WindowFrame(
            FrameType.RANGE,
            FrameBoundary.UnboundedPreceding.getInstance(),
            FrameBoundary.CurrentRow.getInstance()
        );
    }

    /**
     * Creates a GROUPS frame from UNBOUNDED PRECEDING to CURRENT ROW.
     * This groups rows with identical ORDER BY values.
     *
     * <p>SQL: {@code GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}
     *
     * @return a GROUPS frame from the start of the partition to the current row
     */
    public static WindowFrame groupsUnboundedPrecedingToCurrentRow() {
        return new WindowFrame(
            FrameType.GROUPS,
            FrameBoundary.UnboundedPreceding.getInstance(),
            FrameBoundary.CurrentRow.getInstance()
        );
    }

    @Override
    public String toString() {
        return String.format("WindowFrame(%s, %s, %s)", type, start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof WindowFrame)) {
            return false;
        }
        WindowFrame other = (WindowFrame) obj;
        return type == other.type
            && Objects.equals(start, other.start)
            && Objects.equals(end, other.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, start, end);
    }
}
