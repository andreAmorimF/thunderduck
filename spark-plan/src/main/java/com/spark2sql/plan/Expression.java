package com.spark2sql.plan;

import org.apache.spark.sql.types.DataType;

/**
 * Base class for all expressions in the plan.
 */
public abstract class Expression {
    public abstract DataType dataType();
    public abstract boolean nullable();
    public abstract String toString();

    public String name() {
        return toString();
    }
}