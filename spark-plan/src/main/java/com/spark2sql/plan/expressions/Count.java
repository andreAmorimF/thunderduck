package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class Count extends UnaryExpression {
    public Count(Expression child) {
        super(child);
    }

    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("COUNT(%s)", child);
    }
}
