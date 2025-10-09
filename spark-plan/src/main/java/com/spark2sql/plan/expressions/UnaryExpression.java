package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;

/**
 * Abstract base class for unary expressions.
 */
public abstract class UnaryExpression extends Expression {
    protected final Expression child;

    protected UnaryExpression(Expression child) {
        this.child = child;
    }

    public Expression getChild() {
        return child;
    }

    @Override
    public boolean nullable() {
        return child.nullable();
    }
}