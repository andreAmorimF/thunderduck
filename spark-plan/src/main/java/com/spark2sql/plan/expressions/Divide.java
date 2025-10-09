package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Division expression.
 * Note: Spark uses Java semantics for integer division (truncation towards zero).
 */
public class Divide extends BinaryExpression {
    public Divide(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "/";
    }

    @Override
    public DataType dataType() {
        // Division may produce doubles
        if (left.dataType() == DataTypes.DoubleType || right.dataType() == DataTypes.DoubleType) {
            return DataTypes.DoubleType;
        }
        return left.dataType();
    }
}