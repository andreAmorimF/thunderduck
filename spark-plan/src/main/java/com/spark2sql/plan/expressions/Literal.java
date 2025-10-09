package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Represents a literal value in an expression.
 */
public class Literal extends Expression {
    private final Object value;
    private final DataType dataType;

    public Literal(Object value, DataType dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    public static Literal create(Object value) {
        if (value == null) {
            return new Literal(null, DataTypes.NullType);
        } else if (value instanceof Boolean) {
            return new Literal(value, DataTypes.BooleanType);
        } else if (value instanceof Byte) {
            return new Literal(value, DataTypes.ByteType);
        } else if (value instanceof Short) {
            return new Literal(value, DataTypes.ShortType);
        } else if (value instanceof Integer) {
            return new Literal(value, DataTypes.IntegerType);
        } else if (value instanceof Long) {
            return new Literal(value, DataTypes.LongType);
        } else if (value instanceof Float) {
            return new Literal(value, DataTypes.FloatType);
        } else if (value instanceof Double) {
            return new Literal(value, DataTypes.DoubleType);
        } else if (value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;
            return new Literal(value, DataTypes.createDecimalType(bd.precision(), bd.scale()));
        } else if (value instanceof String) {
            return new Literal(value, DataTypes.StringType);
        } else if (value instanceof byte[]) {
            return new Literal(value, DataTypes.BinaryType);
        } else if (value instanceof Date) {
            return new Literal(value, DataTypes.DateType);
        } else if (value instanceof Timestamp) {
            return new Literal(value, DataTypes.TimestampType);
        } else {
            throw new IllegalArgumentException("Unsupported literal type: " + value.getClass());
        }
    }

    public Object getValue() {
        return value;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return value == null;
    }

    @Override
    public String toString() {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "'" + value + "'";
        } else {
            return value.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Literal)) return false;
        Literal literal = (Literal) o;
        return (value == null ? literal.value == null : value.equals(literal.value)) &&
               dataType.equals(literal.dataType);
    }

    @Override
    public int hashCode() {
        return (value != null ? value.hashCode() : 0) * 31 + dataType.hashCode();
    }
}