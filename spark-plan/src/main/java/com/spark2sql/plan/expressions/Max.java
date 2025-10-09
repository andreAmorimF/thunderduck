package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

public class Max extends UnaryExpression {
    public Max(Expression child) {
        super(child);
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public String toString() {
        return String.format("MAX(%s)", child);
    }
}
