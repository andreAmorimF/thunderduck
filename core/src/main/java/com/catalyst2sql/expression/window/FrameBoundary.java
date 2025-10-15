package com.catalyst2sql.expression.window;

import java.util.Objects;

/**
 * Represents a window frame boundary in window function specifications.
 *
 * <p>Window frame boundaries define the start and end points of a sliding window
 * in window functions. They are used with ROWS BETWEEN, RANGE BETWEEN, and
 * GROUPS BETWEEN clauses to specify which rows are included in the window frame.
 *
 * <p>Common boundary types:
 * <ul>
 *   <li>{@link UnboundedPreceding} - All rows from the start of the partition</li>
 *   <li>{@link UnboundedFollowing} - All rows to the end of the partition</li>
 *   <li>{@link CurrentRow} - The current row being processed</li>
 *   <li>{@link Preceding} - N rows before the current row</li>
 *   <li>{@link Following} - N rows after the current row</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 * // Cumulative sum from start to current row
 * ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *
 * // 3-row moving average
 * ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
 *
 * // Centered window (2 before, 2 after)
 * ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
 *
 * // All rows to the end
 * ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 * </pre>
 *
 * @see WindowFrame
 */
public abstract class FrameBoundary {

    /**
     * Converts this frame boundary to SQL syntax.
     *
     * @return SQL representation of the boundary
     */
    public abstract String toSQL();

    /**
     * Returns whether this boundary represents an unbounded boundary.
     *
     * @return true if unbounded, false otherwise
     */
    public boolean isUnbounded() {
        return this instanceof UnboundedPreceding || this instanceof UnboundedFollowing;
    }

    /**
     * Returns whether this boundary is the current row.
     *
     * @return true if current row, false otherwise
     */
    public boolean isCurrentRow() {
        return this instanceof CurrentRow;
    }

    /**
     * Represents UNBOUNDED PRECEDING boundary.
     * This boundary indicates all rows from the start of the partition.
     */
    public static final class UnboundedPreceding extends FrameBoundary {

        private static final UnboundedPreceding INSTANCE = new UnboundedPreceding();

        /**
         * Returns the singleton instance.
         *
         * @return the unbounded preceding instance
         */
        public static UnboundedPreceding getInstance() {
            return INSTANCE;
        }

        private UnboundedPreceding() {
            // Singleton
        }

        @Override
        public String toSQL() {
            return "UNBOUNDED PRECEDING";
        }

        @Override
        public String toString() {
            return "UnboundedPreceding";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof UnboundedPreceding;
        }

        @Override
        public int hashCode() {
            return UnboundedPreceding.class.hashCode();
        }
    }

    /**
     * Represents UNBOUNDED FOLLOWING boundary.
     * This boundary indicates all rows to the end of the partition.
     */
    public static final class UnboundedFollowing extends FrameBoundary {

        private static final UnboundedFollowing INSTANCE = new UnboundedFollowing();

        /**
         * Returns the singleton instance.
         *
         * @return the unbounded following instance
         */
        public static UnboundedFollowing getInstance() {
            return INSTANCE;
        }

        private UnboundedFollowing() {
            // Singleton
        }

        @Override
        public String toSQL() {
            return "UNBOUNDED FOLLOWING";
        }

        @Override
        public String toString() {
            return "UnboundedFollowing";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof UnboundedFollowing;
        }

        @Override
        public int hashCode() {
            return UnboundedFollowing.class.hashCode();
        }
    }

    /**
     * Represents CURRENT ROW boundary.
     * This boundary indicates the row currently being processed.
     */
    public static final class CurrentRow extends FrameBoundary {

        private static final CurrentRow INSTANCE = new CurrentRow();

        /**
         * Returns the singleton instance.
         *
         * @return the current row instance
         */
        public static CurrentRow getInstance() {
            return INSTANCE;
        }

        private CurrentRow() {
            // Singleton
        }

        @Override
        public String toSQL() {
            return "CURRENT ROW";
        }

        @Override
        public String toString() {
            return "CurrentRow";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CurrentRow;
        }

        @Override
        public int hashCode() {
            return CurrentRow.class.hashCode();
        }
    }

    /**
     * Represents N PRECEDING boundary.
     * This boundary indicates N rows before the current row.
     *
     * <p>Example: {@code ROWS BETWEEN 5 PRECEDING AND CURRENT ROW}
     */
    public static final class Preceding extends FrameBoundary {

        private final long offset;

        /**
         * Creates a PRECEDING boundary with the specified offset.
         *
         * @param offset the number of rows before the current row (must be positive)
         * @throws IllegalArgumentException if offset is not positive
         */
        public Preceding(long offset) {
            if (offset <= 0) {
                throw new IllegalArgumentException(
                    "Offset for PRECEDING must be positive, got: " + offset);
            }
            this.offset = offset;
        }

        /**
         * Returns the offset value.
         *
         * @return the number of rows before the current row
         */
        public long offset() {
            return offset;
        }

        @Override
        public String toSQL() {
            return offset + " PRECEDING";
        }

        @Override
        public String toString() {
            return String.format("Preceding(%d)", offset);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Preceding)) {
                return false;
            }
            Preceding other = (Preceding) obj;
            return offset == other.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Preceding.class, offset);
        }
    }

    /**
     * Represents N FOLLOWING boundary.
     * This boundary indicates N rows after the current row.
     *
     * <p>Example: {@code ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING}
     */
    public static final class Following extends FrameBoundary {

        private final long offset;

        /**
         * Creates a FOLLOWING boundary with the specified offset.
         *
         * @param offset the number of rows after the current row (must be positive)
         * @throws IllegalArgumentException if offset is not positive
         */
        public Following(long offset) {
            if (offset <= 0) {
                throw new IllegalArgumentException(
                    "Offset for FOLLOWING must be positive, got: " + offset);
            }
            this.offset = offset;
        }

        /**
         * Returns the offset value.
         *
         * @return the number of rows after the current row
         */
        public long offset() {
            return offset;
        }

        @Override
        public String toSQL() {
            return offset + " FOLLOWING";
        }

        @Override
        public String toString() {
            return String.format("Following(%d)", offset);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Following)) {
                return false;
            }
            Following other = (Following) obj;
            return offset == other.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Following.class, offset);
        }
    }
}
