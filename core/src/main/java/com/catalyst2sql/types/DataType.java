package com.catalyst2sql.types;

/**
 * Base class for all data types in the catalyst2sql type system.
 *
 * <p>This represents the data type of a column or expression. The type system
 * is designed to be compatible with Spark's DataType hierarchy while mapping
 * cleanly to DuckDB's type system.
 *
 * <p>Common data types include:
 * <ul>
 *   <li>Primitive types: IntegerType, LongType, DoubleType, StringType, etc.</li>
 *   <li>Temporal types: DateType, TimestampType</li>
 *   <li>Complex types: ArrayType, MapType, StructType</li>
 * </ul>
 */
public abstract class DataType {

    /**
     * Returns a human-readable name for this data type.
     *
     * @return the type name
     */
    public abstract String typeName();

    /**
     * Returns the default size in bytes for values of this type.
     *
     * <p>Returns -1 for variable-length types (e.g., String, Array).
     *
     * @return the default size in bytes, or -1 for variable-length types
     */
    public int defaultSize() {
        return -1;
    }

    @Override
    public abstract boolean equals(Object other);

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
        return typeName();
    }
}
