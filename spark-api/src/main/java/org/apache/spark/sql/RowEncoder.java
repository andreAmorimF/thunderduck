package org.apache.spark.sql;

import org.apache.spark.sql.types.StructType;

/**
 * Encoder for Row objects.
 * Simplified version for Phase 1.
 */
public class RowEncoder extends Encoder<Row> {

    private final StructType schema;

    private RowEncoder(StructType schema) {
        this.schema = schema;
    }

    /**
     * Create a RowEncoder for the given schema.
     */
    public static RowEncoder apply(StructType schema) {
        return new RowEncoder(schema);
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Class<Row> clsTag() {
        return Row.class;
    }
}