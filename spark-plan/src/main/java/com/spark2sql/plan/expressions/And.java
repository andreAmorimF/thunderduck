package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Logical AND expression.
 */
public class And extends BinaryExpression {
    public And(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "AND";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}