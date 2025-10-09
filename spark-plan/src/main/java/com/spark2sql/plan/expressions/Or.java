package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Logical OR expression.
 */
public class Or extends BinaryExpression {
    public Or(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "OR";
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }
}