package com.thunderduck.expression;

import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;

/**
 * Expression representing a field dereference: base.fieldName.
 *
 * <p>Used for complex dereferences like subquery results or function return
 * values where the base is not a simple column reference.
 *
 * <p>Examples:
 * <pre>
 *   (SELECT struct_col FROM t).field
 *   func().field
 * </pre>
 */
public final class FieldAccessExpression implements Expression {

    private final Expression base;
    private final String fieldName;

    public FieldAccessExpression(Expression base, String fieldName) {
        this.base = Objects.requireNonNull(base, "base must not be null");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
    }

    public Expression base() {
        return base;
    }

    public String fieldName() {
        return fieldName;
    }

    @Override
    public DataType dataType() {
        // Cannot resolve without schema information
        return StringType.get();
    }

    @Override
    public boolean nullable() {
        return true;
    }

    @Override
    public String toSQL() {
        return base.toSQL() + "." + SQLQuoting.quoteIdentifierIfNeeded(fieldName);
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof FieldAccessExpression)) return false;
        FieldAccessExpression that = (FieldAccessExpression) obj;
        return Objects.equals(base, that.base) &&
               Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, fieldName);
    }
}
