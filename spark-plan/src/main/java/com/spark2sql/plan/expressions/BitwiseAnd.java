package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

public class BitwiseAnd extends BinaryExpression {
    public BitwiseAnd(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "&";
    }

    @Override
    public DataType dataType() {
        return left.dataType();
    }
}
