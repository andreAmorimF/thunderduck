package com.thunderduck.runtime;

import com.thunderduck.types.ArrayType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapping iterator that corrects nullable flags in Arrow schema metadata.
 *
 * <p>DuckDB returns all columns as nullable=true in Arrow output, but Spark has
 * specific nullable semantics:
 * <ul>
 *   <li>COUNT(*) and COUNT(col) return non-nullable BIGINT</li>
 *   <li>Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) return non-nullable INT</li>
 *   <li>Column references inherit nullable from source schema</li>
 * </ul>
 *
 * <p>This wrapper uses zero-copy: it reuses the source batch's Arrow vectors directly,
 * only correcting the schema metadata (nullable flags). No data is copied.
 */
public class SchemaCorrectedBatchIterator implements ArrowBatchIterator {

    private static final Logger logger = LoggerFactory.getLogger(SchemaCorrectedBatchIterator.class);

    private final ArrowBatchIterator source;
    private final StructType logicalSchema;
    private final BufferAllocator allocator;

    private Schema correctedSchema;
    private long totalRowCount = 0;
    private int batchCount = 0;
    private boolean closed = false;

    /**
     * Creates a schema-correcting wrapper around a source iterator.
     *
     * @param source the source Arrow batch iterator (typically from DuckDB)
     * @param logicalSchema the logical plan schema with correct nullable flags
     * @param allocator the Arrow allocator (retained for API compatibility but not used for copying)
     */
    public SchemaCorrectedBatchIterator(ArrowBatchIterator source, StructType logicalSchema,
                                        BufferAllocator allocator) {
        this.source = source;
        this.logicalSchema = logicalSchema;
        this.allocator = allocator;
        logger.debug("SchemaCorrectedBatchIterator created with {} fields in logical schema",
            logicalSchema != null ? logicalSchema.size() : 0);
    }

    @Override
    public Schema getSchema() {
        if (correctedSchema != null) {
            return correctedSchema;
        }
        // Build corrected schema from source schema
        return buildCorrectedSchema(source.getSchema());
    }

    @Override
    public boolean hasNext() {
        return !closed && source.hasNext();
    }

    @Override
    public VectorSchemaRoot next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        VectorSchemaRoot duckdbRoot = source.next();

        // Build corrected schema on first batch
        if (correctedSchema == null) {
            correctedSchema = buildCorrectedSchema(duckdbRoot.getSchema());
            logger.debug("Built corrected schema with {} fields", correctedSchema.getFields().size());
        }

        // Zero-copy: wrap existing vectors with corrected schema metadata
        VectorSchemaRoot corrected = wrapWithCorrectedSchema(duckdbRoot);
        totalRowCount += corrected.getRowCount();
        batchCount++;

        return corrected;
    }

    /**
     * Builds a corrected Arrow schema using nullable flags from the logical schema.
     *
     * @param duckdbSchema the schema from DuckDB (all nullable=true)
     * @return the corrected schema with proper nullable flags
     */
    private Schema buildCorrectedSchema(Schema duckdbSchema) {
        List<Field> correctedFields = new ArrayList<>();

        for (int i = 0; i < duckdbSchema.getFields().size(); i++) {
            Field duckField = duckdbSchema.getFields().get(i);

            // Get nullable and dataType from logical schema if available
            boolean nullable = true;  // Default to nullable
            DataType logicalType = null;
            if (logicalSchema != null && i < logicalSchema.size()) {
                StructField field = logicalSchema.fields().get(i);
                nullable = field.nullable();
                logicalType = field.dataType();
            }

            // Correct the field including children for complex types
            Field correctedField = correctField(duckField, nullable, logicalType);
            correctedFields.add(correctedField);
        }

        return new Schema(correctedFields, duckdbSchema.getCustomMetadata());
    }

    /**
     * Recursively corrects a single field's nullable flags.
     *
     * <p>Keeps DuckDB's Arrow types unchanged — only nullable flags are corrected
     * based on the logical schema. For complex types, recursively corrects child
     * nullable flags (e.g., containsNull for arrays, valueContainsNull for maps).
     *
     * @param arrowField the Arrow field from DuckDB
     * @param nullable the correct nullable flag for this field
     * @param logicalType the logical type with correct nullable info (may be null)
     * @return the corrected Arrow field
     */
    private Field correctField(Field arrowField, boolean nullable, DataType logicalType) {
        List<Field> correctedChildren = new ArrayList<>();
        List<Field> originalChildren = arrowField.getChildren();

        // Handle complex types that have children — correct their nullable flags recursively
        if (arrowField.getType() instanceof ArrowType.List && logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            if (originalChildren != null && !originalChildren.isEmpty()) {
                Field elementField = originalChildren.get(0);
                Field correctedElement = correctField(
                    elementField,
                    arrayType.containsNull(),
                    arrayType.elementType()
                );
                correctedChildren.add(correctedElement);
            }
        } else if (arrowField.getType() instanceof ArrowType.Map && logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            if (originalChildren != null && !originalChildren.isEmpty()) {
                Field entriesField = originalChildren.get(0);
                List<Field> entryChildren = entriesField.getChildren();
                if (entryChildren != null && entryChildren.size() >= 2) {
                    Field correctedKey = correctField(entryChildren.get(0), false, mapType.keyType());
                    Field correctedValue = correctField(entryChildren.get(1), mapType.valueContainsNull(), mapType.valueType());

                    List<Field> correctedEntryChildren = new ArrayList<>();
                    correctedEntryChildren.add(correctedKey);
                    correctedEntryChildren.add(correctedValue);

                    FieldType entriesType = new FieldType(
                        entriesField.isNullable(),
                        entriesField.getType(),
                        entriesField.getDictionary(),
                        entriesField.getMetadata()
                    );
                    correctedChildren.add(new Field(entriesField.getName(), entriesType, correctedEntryChildren));
                } else {
                    correctedChildren.add(entriesField);
                }
            }
        } else if (arrowField.getType() instanceof ArrowType.Struct && logicalType instanceof StructType) {
            StructType structType = (StructType) logicalType;
            if (originalChildren != null) {
                for (int i = 0; i < originalChildren.size(); i++) {
                    Field child = originalChildren.get(i);
                    if (i < structType.size()) {
                        StructField logicalField = structType.fields().get(i);
                        Field correctedChild = correctField(child, logicalField.nullable(), logicalField.dataType());
                        correctedChildren.add(correctedChild);
                    } else {
                        correctedChildren.add(child);
                    }
                }
            }
        } else {
            // Not a complex type or no logical type info — keep original children
            correctedChildren = originalChildren;
        }

        FieldType fieldType = new FieldType(
            nullable,
            arrowField.getType(),
            arrowField.getDictionary(),
            arrowField.getMetadata()
        );
        return new Field(arrowField.getName(), fieldType, correctedChildren);
    }

    /**
     * Zero-copy wrapping: creates a new VectorSchemaRoot that references the same
     * underlying Arrow buffers but with the corrected schema metadata.
     *
     * @param source the source batch from DuckDB
     * @return a new VectorSchemaRoot with corrected schema, sharing the same data buffers
     */
    private VectorSchemaRoot wrapWithCorrectedSchema(VectorSchemaRoot source) {
        List<FieldVector> vectors = source.getFieldVectors();
        return new VectorSchemaRoot(correctedSchema.getFields(), vectors, source.getRowCount());
    }

    @Override
    public long getTotalRowCount() {
        return totalRowCount;
    }

    @Override
    public int getBatchCount() {
        return batchCount;
    }

    @Override
    public boolean hasError() {
        return source.hasError();
    }

    @Override
    public Exception getError() {
        return source.getError();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            source.close();
        } catch (Exception e) {
            logger.warn("Error closing source iterator", e);
        }

        logger.debug("SchemaCorrectedBatchIterator closed: {} batches, {} rows",
            batchCount, totalRowCount);
    }
}
