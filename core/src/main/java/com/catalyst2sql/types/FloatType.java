package com.catalyst2sql.types;

/**
 * Data type representing a single-precision 32-bit floating point number.
 * Maps to DuckDB FLOAT and Spark FloatType.
 */
public class FloatType extends DataType {

    private static final FloatType INSTANCE = new FloatType();

    private FloatType() {}

    public static FloatType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "float";
    }

    @Override
    public int defaultSize() {
        return 4;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FloatType;
    }

    @Override
    public int hashCode() {
        return typeName().hashCode();
    }
}
