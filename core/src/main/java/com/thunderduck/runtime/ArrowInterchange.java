package com.thunderduck.runtime;

import com.thunderduck.types.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.duckdb.DuckDBConnection;
import java.sql.*;
import java.util.*;

/**
 * Arrow data interchange utilities for DuckDB.
 *
 * <p>This class provides utilities for importing Arrow data into DuckDB tables
 * and type conversions between Arrow and SQL types.
 *
 * <p>For query execution with Arrow streaming, use {@link ArrowStreamingExecutor}
 * which provides zero-copy batch iteration via DuckDB's arrowExportStream().
 *
 * <p>Example usage:
 * <pre>
 *   // Import Arrow data to DuckDB
 *   ArrowInterchange.toTable(root, "temp_table", connection);
 * </pre>
 *
 * @see ArrowStreamingExecutor
 * @see QueryExecutor
 */
public class ArrowInterchange {

    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    private ArrowInterchange() {} // Utility class

    /**
     * Converts Arrow VectorSchemaRoot to DuckDB table.
     *
     * <p>This method creates a temporary table in DuckDB and imports the
     * Arrow data into it.
     *
     * @param root the Arrow VectorSchemaRoot
     * @param tableName the target table name
     * @param conn the DuckDB connection
     * @throws SQLException if import fails
     */
    public static void toTable(VectorSchemaRoot root, String tableName,
                               DuckDBConnection conn) throws SQLException {
        Objects.requireNonNull(root, "root must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(conn, "conn must not be null");

        // Create table from schema
        String createSQL = generateCreateTable(tableName, root.getSchema());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
        }

        // Insert data
        String insertSQL = generateInsertStatement(tableName, root.getSchema());
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            for (int row = 0; row < root.getRowCount(); row++) {
                for (int col = 0; col < root.getFieldVectors().size(); col++) {
                    FieldVector vector = root.getVector(col);
                    Object value = getVectorValue(vector, row);
                    pstmt.setObject(col + 1, value);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    /**
     * Gets a value from an Arrow vector at the specified index.
     *
     * @param vector the vector to read from
     * @param index the row index
     * @return the value (may be null)
     */
    private static Object getVectorValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }

        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index) != 0;
        } else if (vector instanceof TinyIntVector) {
            return ((TinyIntVector) vector).get(index);
        } else if (vector instanceof SmallIntVector) {
            return ((SmallIntVector) vector).get(index);
        } else if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        } else if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof DecimalVector) {
            // Handle DECIMAL/NUMERIC types - convert to double for simplicity
            java.math.BigDecimal decimal = ((DecimalVector) vector).getObject(index);
            return decimal != null ? decimal.doubleValue() : null;
        } else if (vector instanceof Decimal256Vector) {
            // Handle 256-bit decimals
            java.math.BigDecimal decimal = ((Decimal256Vector) vector).getObject(index);
            return decimal != null ? decimal.doubleValue() : null;
        } else if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        } else if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(index);
        } else if (vector instanceof DateDayVector) {
            int days = ((DateDayVector) vector).get(index);
            return java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(days));
        } else if (vector instanceof TimeStampMicroVector) {
            long micros = ((TimeStampMicroVector) vector).get(index);
            return new java.sql.Timestamp(micros / 1000);
        }

        return null;
    }

    /**
     * Generates a CREATE TABLE statement from Arrow schema.
     *
     * @param tableName the table name
     * @param schema the Arrow schema
     * @return the CREATE TABLE SQL
     */
    private static String generateCreateTable(String tableName, Schema schema) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(tableName).append(" (");

        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            Field field = fields.get(i);
            sql.append(field.getName()).append(" ");
            sql.append(arrowTypeToSQLType(field.getType()));
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Generates an INSERT statement for Arrow schema.
     *
     * @param tableName the table name
     * @param schema the Arrow schema
     * @return the INSERT SQL with placeholders
     */
    private static String generateInsertStatement(String tableName, Schema schema) {
        int fieldCount = schema.getFields().size();
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" VALUES (");

        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Converts Arrow type to SQL type string.
     *
     * @param type the Arrow type
     * @return the SQL type name
     */
    public static String arrowTypeToSQLType(ArrowType type) {
        switch (type.getTypeID()) {
            case Bool: return "BOOLEAN";
            case Int:
                ArrowType.Int intType = (ArrowType.Int) type;
                switch (intType.getBitWidth()) {
                    case 8: return "TINYINT";
                    case 16: return "SMALLINT";
                    case 32: return "INTEGER";
                    case 64: return "BIGINT";
                    default: return "INTEGER";
                }
            case FloatingPoint:
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) type;
                return fpType.getPrecision() == FloatingPointPrecision.SINGLE ? "FLOAT" : "DOUBLE";
            case Utf8: return "VARCHAR";
            case Binary: return "VARBINARY";
            case Date: return "DATE";
            case Time: return "TIME";
            case Timestamp: return "TIMESTAMP";
            case Decimal: return "DECIMAL";
            default: return "VARCHAR";
        }
    }

    /**
     * Converts Arrow Schema to thunderduck StructType, preserving nullable flags.
     *
     * <p>This method correctly extracts the nullable flag from each Arrow Field,
     * which is important for schema fidelity when PySpark sends DataFrames with
     * explicit nullable=False fields.
     *
     * @param arrowSchema the Arrow schema
     * @return the thunderduck StructType with correct nullable flags
     */
    public static StructType arrowSchemaToStructType(Schema arrowSchema) {
        List<StructField> fields = new ArrayList<>();
        for (Field arrowField : arrowSchema.getFields()) {
            DataType fieldType = arrowFieldToDataType(arrowField);
            fields.add(new StructField(
                arrowField.getName(),
                fieldType,
                arrowField.isNullable()
            ));
        }
        return new StructType(fields);
    }

    /**
     * Converts Arrow Field to thunderduck DataType.
     *
     * <p>Handles complex types (List, Map, Struct) by recursively converting child fields.
     *
     * @param arrowField the Arrow field
     * @return the thunderduck DataType
     */
    public static DataType arrowFieldToDataType(Field arrowField) {
        ArrowType arrowType = arrowField.getType();
        List<Field> children = arrowField.getChildren();

        // Handle complex types first (they need access to children)
        if (arrowType instanceof ArrowType.List) {
            // Arrow List has one child field (the element type)
            if (children != null && !children.isEmpty()) {
                Field elementField = children.get(0);
                DataType elementType = arrowFieldToDataType(elementField);
                boolean containsNull = elementField.isNullable();
                return new ArrayType(elementType, containsNull);
            }
            // Fallback for empty list - default to string element
            return new ArrayType(StringType.get(), true);
        } else if (arrowType instanceof ArrowType.Map) {
            // Arrow Map has one child field (entries struct with key and value fields)
            if (children != null && !children.isEmpty()) {
                Field entriesField = children.get(0);
                List<Field> entryChildren = entriesField.getChildren();
                if (entryChildren != null && entryChildren.size() >= 2) {
                    DataType keyType = arrowFieldToDataType(entryChildren.get(0));
                    DataType valueType = arrowFieldToDataType(entryChildren.get(1));
                    boolean valueContainsNull = entryChildren.get(1).isNullable();
                    return new MapType(keyType, valueType, valueContainsNull);
                }
            }
            // Fallback
            return new MapType(StringType.get(), StringType.get(), true);
        } else if (arrowType instanceof ArrowType.Struct) {
            // Arrow Struct has child fields for each struct field
            List<StructField> structFields = new ArrayList<>();
            if (children != null) {
                for (Field child : children) {
                    DataType childType = arrowFieldToDataType(child);
                    structFields.add(new StructField(
                        child.getName(),
                        childType,
                        child.isNullable()
                    ));
                }
            }
            return new StructType(structFields);
        }

        // Handle primitive types
        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 32) {
                return IntegerType.get();
            } else if (intType.getBitWidth() == 64) {
                return LongType.get();
            } else if (intType.getBitWidth() == 16) {
                return ShortType.get();
            } else if (intType.getBitWidth() == 8) {
                return ByteType.get();
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return DoubleType.get();
            } else if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return FloatType.get();
            }
        } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return StringType.get();
        } else if (arrowType instanceof ArrowType.Bool) {
            return BooleanType.get();
        } else if (arrowType instanceof ArrowType.Date) {
            return DateType.get();
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return TimestampType.get();
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
        } else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary) {
            return BinaryType.get();
        }

        // Default to string for unknown types
        return StringType.get();
    }

    /**
     * Returns the allocator used for Arrow memory management.
     *
     * @return the root allocator
     */
    public static RootAllocator getAllocator() {
        return allocator;
    }
}
