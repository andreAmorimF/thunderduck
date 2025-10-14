package com.catalyst2sql.types;

/**
 * Data type representing arbitrary binary data (byte arrays).
 * Maps to DuckDB BLOB and Spark BinaryType.
 */
public class BinaryType extends DataType {

    private static final BinaryType INSTANCE = new BinaryType();

    private BinaryType() {}

    public static BinaryType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "binary";
    }

    @Override
    public int defaultSize() {
        return -1; // Variable length
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BinaryType;
    }

    @Override
    public int hashCode() {
        return typeName().hashCode();
    }
}
