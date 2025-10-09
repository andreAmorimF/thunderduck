package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * Remainder (modulo) expression.
 */
public class Remainder extends BinaryExpression {
    public Remainder(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "%";
    }

    @Override
    public DataType dataType() {
        return left.dataType();
    }
}