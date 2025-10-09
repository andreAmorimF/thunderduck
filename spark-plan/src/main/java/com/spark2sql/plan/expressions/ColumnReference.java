package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Reference to a column by name.
 */
public class ColumnReference extends Expression {
    private final String name;
    private final DataType dataType;

    public ColumnReference(String name) {
        this(name, DataTypes.StringType); // Default type, will be resolved during analysis
    }

    public ColumnReference(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return true; // Column references are nullable by default
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnReference)) return false;
        ColumnReference that = (ColumnReference) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}