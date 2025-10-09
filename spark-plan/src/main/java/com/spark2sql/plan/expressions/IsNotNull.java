package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Check if expression is not null.
 */
public class IsNotNull extends UnaryExpression {
    public IsNotNull(Expression child) {
        super(child);
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }

    @Override
    public boolean nullable() {
        return false; // IsNotNull always returns a boolean, never null
    }

    @Override
    public String toString() {
        return String.format("(%s IS NOT NULL)", child);
    }
}