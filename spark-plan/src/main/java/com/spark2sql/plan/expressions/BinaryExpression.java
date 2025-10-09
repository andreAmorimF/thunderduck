package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * Abstract base class for binary expressions.
 */
public abstract class BinaryExpression extends Expression {
    protected final Expression left;
    protected final Expression right;

    protected BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public boolean nullable() {
        return left.nullable() || right.nullable();
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s)", left, symbol(), right);
    }

    protected abstract String symbol();
}