package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Greater than or equal comparison expression.
 */
public class GreaterThanOrEqual extends BinaryExpression {
    public GreaterThanOrEqual(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return ">=";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}