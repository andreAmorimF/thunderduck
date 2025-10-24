package com.thunderduck.types;

import java.util.Objects;

/**
 * Represents a field in a StructType.
 *
 * <p>Each field has a name, data type, and nullability flag.
 */
public class StructField {

    private final String name;
    private final DataType dataType;
    private final boolean nullable;

    /**
     * Creates a struct field.
     *
     * @param name the field name
     * @param dataType the field data type
     * @param nullable whether the field can contain null values
     */
    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
        this.nullable = nullable;
    }

    /**
     * Creates a nullable struct field.
     *
     * @param name the field name
     * @param dataType the field data type
     */
    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    /**
     * Returns the field name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the field data type.
     *
     * @return the data type
     */
    public DataType dataType() {
        return dataType;
    }

    /**
     * Returns whether this field is nullable.
     *
     * @return true if nullable, false otherwise
     */
    public boolean nullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructField that = (StructField) o;
        return nullable == that.nullable &&
               Objects.equals(name, that.name) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, nullable);
    }

    @Override
    public String toString() {
        return name + ": " + dataType + (nullable ? "" : " NOT NULL");
    }
}
