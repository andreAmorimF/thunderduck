package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class Not extends UnaryExpression {
    public Not(Expression child) {
        super(child);
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }

    @Override
    public String toString() {
        return String.format("NOT (%s)", child);
    }
}
