package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Equality comparison expression.
 */
public class EqualTo extends BinaryExpression {
    public EqualTo(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "=";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}