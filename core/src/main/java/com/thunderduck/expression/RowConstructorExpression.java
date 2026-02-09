package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing a row/tuple constructor: (a, b, c).
 *
 * <p>Used for tuple comparisons in IN clauses and WHERE (a,b) = (1,2).
 *
 * <p>Examples:
 * <pre>
 *   (1, 2, 3)
 *   (a, b) IN (SELECT x, y FROM t)
 * </pre>
 */
public final class RowConstructorExpression implements Expression {

    private final List<Expression> elements;

    public RowConstructorExpression(List<Expression> elements) {
        Objects.requireNonNull(elements, "elements must not be null");
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Row constructor requires at least one element");
        }
        this.elements = new ArrayList<>(elements);
    }

    public List<Expression> elements() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public DataType dataType() {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < elements.size(); i++) {
            DataType elemType = elements.get(i).dataType();
            fields.add(new StructField("col" + (i + 1), elemType, elements.get(i).nullable()));
        }
        return new StructType(fields);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder("(");
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(elements.get(i).toSQL());
        }
        sql.append(")");
        return sql.toString();
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RowConstructorExpression)) return false;
        RowConstructorExpression that = (RowConstructorExpression) obj;
        return Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }
}
