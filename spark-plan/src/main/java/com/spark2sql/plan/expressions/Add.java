package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * Addition expression.
 */
public class Add extends BinaryExpression {
    public Add(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected String symbol() {
        return "+";
    }

    @Override
    public DataType dataType() {
        // Simplified type coercion - in reality would be more complex
        return left.dataType();
    }
}