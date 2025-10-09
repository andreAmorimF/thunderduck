package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

public class Cast extends UnaryExpression {
    private final DataType targetType;

    public Cast(Expression child, DataType targetType) {
        super(child);
        this.targetType = targetType;
    }

    public DataType getTargetType() {
        return targetType;
    }

    @Override
    public DataType dataType() {
        return targetType;
    }

    @Override
    public String toString() {
        return String.format("CAST(%s AS %s)", child, targetType);
    }
}
