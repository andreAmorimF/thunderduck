package com.catalyst2sql.logical;

import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a sort (ORDER BY clause).
 *
 * <p>This node sorts rows from its child based on one or more sort orders.
 *
 * <p>Examples:
 * <pre>
 *   df.orderBy("name")
 *   df.sort(col("age").desc(), col("name").asc())
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT * FROM (child) ORDER BY expr1 ASC, expr2 DESC, ...</pre>
 */
public class Sort extends LogicalPlan {

    private final List<SortOrder> sortOrders;

    /**
     * Creates a sort node.
     *
     * @param child the child node
     * @param sortOrders the sort orders
     */
    public Sort(LogicalPlan child, List<SortOrder> sortOrders) {
        super(child);
        this.sortOrders = new ArrayList<>(Objects.requireNonNull(sortOrders, "sortOrders must not be null"));

        if (this.sortOrders.isEmpty()) {
            throw new IllegalArgumentException("sortOrders must not be empty");
        }
    }

    /**
     * Returns the sort orders.
     *
     * @return an unmodifiable list of sort orders
     */
    public List<SortOrder> sortOrders() {
        return Collections.unmodifiableList(sortOrders);
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // SQL generation will be implemented by the generator
        throw new UnsupportedOperationException("SQL generation not yet implemented");
    }

    @Override
    public StructType inferSchema() {
        // Sort doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        return String.format("Sort(%s)", sortOrders);
    }

    /**
     * Represents a sort order (expression + direction + null handling).
     */
    public static class SortOrder {
        private final Expression expression;
        private final SortDirection direction;
        private final NullOrdering nullOrdering;

        public SortOrder(Expression expression, SortDirection direction, NullOrdering nullOrdering) {
            this.expression = Objects.requireNonNull(expression);
            this.direction = Objects.requireNonNull(direction);
            this.nullOrdering = Objects.requireNonNull(nullOrdering);
        }

        public SortOrder(Expression expression, SortDirection direction) {
            this(expression, direction,
                 direction == SortDirection.ASCENDING ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST);
        }

        public Expression expression() {
            return expression;
        }

        public SortDirection direction() {
            return direction;
        }

        public NullOrdering nullOrdering() {
            return nullOrdering;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s", expression, direction, nullOrdering);
        }
    }

    /**
     * Sort direction.
     */
    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    /**
     * Null ordering.
     */
    public enum NullOrdering {
        NULLS_FIRST,
        NULLS_LAST
    }
}
