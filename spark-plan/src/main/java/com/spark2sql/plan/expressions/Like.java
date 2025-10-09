package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class Like extends BinaryExpression {
    public Like(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "LIKE";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}
