package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * Represents a sort order expression (column with direction and null ordering).
 */
public class SortOrder extends UnaryExpression {
    private final boolean ascending;
    private final boolean nullsFirst;

    public SortOrder(Expression child, boolean ascending, boolean nullsFirst) {
        super(child);
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
    }

    public static SortOrder asc(Expression expr) {
        return new SortOrder(expr, true, false);
    }

    public static SortOrder desc(Expression expr) {
        return new SortOrder(expr, false, true);
    }

    public boolean isAscending() {
        return ascending;
    }

    public boolean isNullsFirst() {
        return nullsFirst;
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public String toString() {
        String direction = ascending ? "ASC" : "DESC";
        String nulls = nullsFirst ? "NULLS FIRST" : "NULLS LAST";
        return String.format("%s %s %s", child, direction, nulls);
    }
}