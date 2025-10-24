package com.thunderduck.expression;

import com.thunderduck.types.DataType;

/**
 * Base class for all expressions in the thunderduck translation layer.
 *
 * <p>Expressions represent computations that produce values, such as:
 * <ul>
 *   <li>Literals (constants)</li>
 *   <li>Column references</li>
 *   <li>Arithmetic operations (a + b, a * b)</li>
 *   <li>Comparison operations (a > b, a == b)</li>
 *   <li>Function calls (upper(name), abs(value))</li>
 * </ul>
 *
 * <p>Expressions are used in:
 * <ul>
 *   <li>SELECT clause (projections)</li>
 *   <li>WHERE clause (filters)</li>
 *   <li>GROUP BY clause</li>
 *   <li>ORDER BY clause</li>
 * </ul>
 */
public abstract class Expression {

    /**
     * Returns the data type of the value produced by this expression.
     *
     * @return the data type
     */
    public abstract DataType dataType();

    /**
     * Returns whether this expression can produce null values.
     *
     * @return true if nullable, false otherwise
     */
    public abstract boolean nullable();

    /**
     * Converts this expression to its SQL string representation.
     *
     * <p>This method generates the SQL text for this expression that can be
     * used in SELECT, WHERE, GROUP BY, ORDER BY, and other SQL clauses.
     *
     * @return the SQL string representation
     */
    public abstract String toSQL();

    /**
     * Returns a human-readable string representation of this expression.
     *
     * <p>By default, this delegates to {@link #toSQL()}.
     *
     * @return a string representation
     */
    @Override
    public String toString() {
        return toSQL();
    }
}
