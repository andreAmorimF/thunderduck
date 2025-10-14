package com.catalyst2sql.types;

/**
 * Data type representing a variable-length string.
 */
public class StringType extends DataType {

    private static final StringType INSTANCE = new StringType();

    private StringType() {}

    public static StringType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "string";
    }

    @Override
    public int defaultSize() {
        return -1; // Variable length
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringType;
    }

    @Override
    public int hashCode() {
        return typeName().hashCode();
    }
}
