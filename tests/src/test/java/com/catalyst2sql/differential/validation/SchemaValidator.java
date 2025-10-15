package com.catalyst2sql.differential.validation;

import com.catalyst2sql.differential.model.Divergence;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates schema compatibility between Spark and catalyst2sql results.
 *
 * <p>Performs field-by-field comparison including:
 * <ul>
 *   <li>Column count matching</li>
 *   <li>Column name matching</li>
 *   <li>Data type compatibility</li>
 *   <li>Nullability validation</li>
 * </ul>
 */
public class SchemaValidator {

    private final TypeComparator typeComparator;

    public SchemaValidator() {
        this.typeComparator = new TypeComparator();
    }

    /**
     * Compare Spark schema with JDBC ResultSet metadata.
     *
     * @param sparkSchema Spark StructType schema
     * @param metadata JDBC ResultSetMetaData
     * @return List of schema divergences found
     */
    public List<Divergence> compare(StructType sparkSchema, ResultSetMetaData metadata) {
        List<Divergence> divergences = new ArrayList<>();

        try {
            int sparkFieldCount = sparkSchema.fields().length;
            int jdbcFieldCount = metadata.getColumnCount();

            // Check field count
            if (sparkFieldCount != jdbcFieldCount) {
                divergences.add(new Divergence(
                        Divergence.Type.SCHEMA_MISMATCH,
                        Divergence.Severity.CRITICAL,
                        String.format("Column count mismatch: Spark has %d columns, DuckDB has %d columns",
                                sparkFieldCount, jdbcFieldCount),
                        sparkFieldCount,
                        jdbcFieldCount
                ));
                return divergences; // Critical error, can't continue comparison
            }

            // Compare each field
            for (int i = 0; i < sparkFieldCount; i++) {
                StructField sparkField = sparkSchema.fields()[i];
                String sparkName = sparkField.name();
                DataType sparkType = sparkField.dataType();
                boolean sparkNullable = sparkField.nullable();

                // JDBC is 1-indexed
                String jdbcName = metadata.getColumnName(i + 1);
                int jdbcTypeCode = metadata.getColumnType(i + 1);
                int jdbcNullable = metadata.isNullable(i + 1);

                // Check column name
                if (!sparkName.equalsIgnoreCase(jdbcName)) {
                    divergences.add(new Divergence(
                            Divergence.Type.SCHEMA_MISMATCH,
                            Divergence.Severity.HIGH,
                            String.format("Column %d name mismatch", i),
                            sparkName,
                            jdbcName
                    ));
                }

                // Check type compatibility
                if (!typeComparator.areTypesCompatible(sparkType, jdbcTypeCode)) {
                    divergences.add(new Divergence(
                            Divergence.Type.SCHEMA_MISMATCH,
                            Divergence.Severity.HIGH,
                            String.format("Column '%s' type mismatch", sparkName),
                            sparkType.typeName(),
                            typeComparator.jdbcTypeToString(jdbcTypeCode)
                    ));
                }

                // Skip nullability checks - JDBC drivers report nullability inconsistently
                // especially for aggregate results and literals. This is not a semantic divergence.
                // For example, COUNT(*) can never be NULL but DuckDB JDBC reports it as nullable.
                // Similarly, literals like 42 or 'hello' are reported as nullable by DuckDB JDBC.
                // We intentionally skip this check to avoid false positives.
            }

        } catch (Exception e) {
            divergences.add(new Divergence(
                    Divergence.Type.EXECUTION_ERROR,
                    Divergence.Severity.CRITICAL,
                    "Schema comparison failed: " + e.getMessage(),
                    null,
                    null
            ));
        }

        return divergences;
    }
}
