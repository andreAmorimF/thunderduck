package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class Average extends UnaryExpression {
    public Average(Expression child) {
        super(child);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public String toString() {
        return String.format("AVG(%s)", child);
    }
}
