package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class StartsWith extends BinaryExpression {
    public StartsWith(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "STARTS WITH";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}
