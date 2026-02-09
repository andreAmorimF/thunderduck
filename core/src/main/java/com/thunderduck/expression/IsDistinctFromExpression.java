package com.thunderduck.expression;

import com.thunderduck.types.BooleanType;
import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression representing IS [NOT] DISTINCT FROM comparison.
 *
 * <p>This is null-safe equality: unlike regular {@code =} which returns NULL
 * when either operand is NULL, IS DISTINCT FROM always returns a non-null boolean.
 *
 * <p>Examples:
 * <pre>
 *   a IS DISTINCT FROM b        -- true if a != b (treating NULLs as equal)
 *   a IS NOT DISTINCT FROM b    -- true if a == b (treating NULLs as equal)
 * </pre>
 */
public final class IsDistinctFromExpression implements Expression {

    private final Expression left;
    private final Expression right;
    private final boolean negated;

    /**
     * Creates an IS [NOT] DISTINCT FROM expression.
     *
     * @param left the left operand
     * @param right the right operand
     * @param negated true for IS NOT DISTINCT FROM, false for IS DISTINCT FROM
     */
    public IsDistinctFromExpression(Expression left, Expression right, boolean negated) {
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.negated = negated;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public boolean negated() {
        return negated;
    }

    @Override
    public DataType dataType() {
        return BooleanType.get();
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String toSQL() {
        String notStr = negated ? "NOT " : "";
        return left.toSQL() + " IS " + notStr + "DISTINCT FROM " + right.toSQL();
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof IsDistinctFromExpression)) return false;
        IsDistinctFromExpression that = (IsDistinctFromExpression) obj;
        return negated == that.negated &&
               Objects.equals(left, that.left) &&
               Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, negated);
    }
}
