package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * Represents an aliased expression with a name.
 */
public class Alias extends UnaryExpression {
    private final String name;

    public Alias(Expression child, String name) {
        super(child);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public String toString() {
        return String.format("%s AS %s", child, name);
    }

    @Override
    public String name() {
        return name;
    }
}