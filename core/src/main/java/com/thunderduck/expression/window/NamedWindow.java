package com.thunderduck.expression.window;

import com.thunderduck.expression.Expression;
import com.thunderduck.logical.Sort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a named window specification in the WINDOW clause.
 *
 * <p>Named windows allow defining window specifications once and reusing them
 * across multiple window functions, improving query readability and maintainability.
 *
 * <p><b>SQL Example:</b>
 * <pre>
 * SELECT
 *   employee_id,
 *   salary,
 *   AVG(salary) OVER w AS avg_salary,
 *   RANK() OVER w AS salary_rank
 * FROM employees
 * WINDOW w AS (PARTITION BY department_id ORDER BY salary DESC)
 * </pre>
 *
 * <p><b>Benefits:</b>
 * <ul>
 *   <li>Reduces duplication of complex window specifications</li>
 *   <li>Easier to maintain - change specification in one place</li>
 *   <li>Improves query readability</li>
 *   <li>Database can optimize execution of multiple functions over same window</li>
 * </ul>
 *
 * <p><b>Composition:</b>
 * Named windows can reference other named windows and extend them:
 * <pre>
 * WINDOW
 *   w1 AS (PARTITION BY department_id),
 *   w2 AS (w1 ORDER BY salary DESC),
 *   w3 AS (w2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
 * </pre>
 *
 * @see WindowFunction
 * @see WindowFrame
 * @since 1.0
 */
public class NamedWindow {

    private final String name;
    private final String baseWindow;  // Optional reference to another named window
    private final List<Expression> partitionBy;
    private final List<Sort.SortOrder> orderBy;
    private final WindowFrame frame;

    /**
     * Creates a named window with all specifications.
     *
     * @param name the window name (identifier)
     * @param baseWindow optional base window to extend (can be null)
     * @param partitionBy the PARTITION BY expressions (can be empty)
     * @param orderBy the ORDER BY specifications (can be empty)
     * @param frame the frame specification (can be null)
     */
    public NamedWindow(String name,
                      String baseWindow,
                      List<Expression> partitionBy,
                      List<Sort.SortOrder> orderBy,
                      WindowFrame frame) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.baseWindow = baseWindow;  // Can be null
        this.partitionBy = partitionBy != null
            ? Collections.unmodifiableList(new ArrayList<>(partitionBy))
            : Collections.emptyList();
        this.orderBy = orderBy != null
            ? Collections.unmodifiableList(new ArrayList<>(orderBy))
            : Collections.emptyList();
        this.frame = frame;  // Can be null

        validate();
    }

    /**
     * Creates a named window without base window reference.
     *
     * @param name the window name
     * @param partitionBy the PARTITION BY expressions
     * @param orderBy the ORDER BY specifications
     * @param frame the frame specification
     */
    public NamedWindow(String name,
                      List<Expression> partitionBy,
                      List<Sort.SortOrder> orderBy,
                      WindowFrame frame) {
        this(name, null, partitionBy, orderBy, frame);
    }

    /**
     * Creates a simple named window with only PARTITION BY.
     *
     * @param name the window name
     * @param partitionBy the PARTITION BY expressions
     * @return a new named window instance
     */
    public static NamedWindow partitionBy(String name, List<Expression> partitionBy) {
        return new NamedWindow(name, null, partitionBy, Collections.emptyList(), null);
    }

    /**
     * Creates a named window with PARTITION BY and ORDER BY.
     *
     * @param name the window name
     * @param partitionBy the PARTITION BY expressions
     * @param orderBy the ORDER BY specifications
     * @return a new named window instance
     */
    public static NamedWindow partitionByOrderBy(String name,
                                                 List<Expression> partitionBy,
                                                 List<Sort.SortOrder> orderBy) {
        return new NamedWindow(name, null, partitionBy, orderBy, null);
    }

    /**
     * Creates a named window that extends another named window.
     *
     * @param name the window name
     * @param baseWindow the base window to extend
     * @param orderBy additional ORDER BY specifications
     * @return a new named window instance
     */
    public static NamedWindow extending(String name, String baseWindow,
                                       List<Sort.SortOrder> orderBy) {
        return new NamedWindow(name, baseWindow, Collections.emptyList(), orderBy, null);
    }

    /**
     * Returns the window name.
     *
     * @return the window name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the base window name if this window extends another.
     *
     * @return the base window name, or null if not extending
     */
    public String baseWindow() {
        return baseWindow;
    }

    /**
     * Returns the PARTITION BY expressions.
     *
     * @return an unmodifiable list of partition expressions
     */
    public List<Expression> partitionBy() {
        return partitionBy;
    }

    /**
     * Returns the ORDER BY specifications.
     *
     * @return an unmodifiable list of sort orders
     */
    public List<Sort.SortOrder> orderBy() {
        return orderBy;
    }

    /**
     * Returns the frame specification.
     *
     * @return the window frame, or null if not specified
     */
    public WindowFrame frame() {
        return frame;
    }

    /**
     * Generates SQL for this named window definition.
     *
     * <p>Format: {@code name AS (specification)}
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code w AS (PARTITION BY dept_id)}</li>
     *   <li>{@code w AS (ORDER BY salary DESC)}</li>
     *   <li>{@code w AS (w1 ORDER BY salary)}</li>
     *   <li>{@code w AS (PARTITION BY dept_id ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)}</li>
     * </ul>
     *
     * @return the SQL representation
     */
    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append(name).append(" AS (");

        boolean hasContent = false;

        // Base window reference
        if (baseWindow != null) {
            sql.append(baseWindow);
            hasContent = true;
        }

        // PARTITION BY
        if (!partitionBy.isEmpty()) {
            if (hasContent) sql.append(" ");
            sql.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(partitionBy.get(i).toSQL());
            }
            hasContent = true;
        }

        // ORDER BY
        if (!orderBy.isEmpty()) {
            if (hasContent) sql.append(" ");
            sql.append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) sql.append(", ");
                Sort.SortOrder order = orderBy.get(i);
                sql.append(order.expression().toSQL());

                // Add sort direction
                if (order.direction() == Sort.SortDirection.DESCENDING) {
                    sql.append(" DESC");
                } else {
                    sql.append(" ASC");
                }

                // Add null ordering
                if (order.nullOrdering() == Sort.NullOrdering.NULLS_FIRST) {
                    sql.append(" NULLS FIRST");
                } else if (order.nullOrdering() == Sort.NullOrdering.NULLS_LAST) {
                    sql.append(" NULLS LAST");
                }
            }
            hasContent = true;
        }

        // Frame specification
        if (frame != null) {
            if (hasContent) sql.append(" ");
            sql.append(frame.toSQL());
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Validates the named window configuration.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    private void validate() {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Named window must have a non-empty name");
        }

        // If extending a base window, base window name must be valid
        if (baseWindow != null && baseWindow.trim().isEmpty()) {
            throw new IllegalArgumentException("Base window name must not be empty");
        }

        // Frame specification requires ORDER BY (in most SQL dialects)
        if (frame != null && orderBy.isEmpty() && baseWindow == null) {
            // If there's a frame but no ORDER BY and not extending another window,
            // this might be invalid depending on the SQL dialect
            // For now, we allow it but databases may reject it
        }

        // Cannot have null expressions in partitionBy or orderBy
        if (partitionBy.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("PARTITION BY expressions must not contain nulls");
        }
        if (orderBy.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("ORDER BY specifications must not contain nulls");
        }
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWindow that = (NamedWindow) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(baseWindow, that.baseWindow) &&
               Objects.equals(partitionBy, that.partitionBy) &&
               Objects.equals(orderBy, that.orderBy) &&
               Objects.equals(frame, that.frame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, baseWindow, partitionBy, orderBy, frame);
    }
}
