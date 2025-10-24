package com.thunderduck.io;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.logical.TableScan;
import com.thunderduck.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Reader for Delta Lake tables using DuckDB Delta extension.
 *
 * <p>Provides support for:
 * <ul>
 *   <li>Time travel queries (version and timestamp)</li>
 *   <li>Transaction log parsing</li>
 *   <li>Schema evolution tracking</li>
 *   <li>Partition pruning</li>
 * </ul>
 *
 * <p>Delta Lake is an open-source storage framework that brings ACID transactions
 * to Apache Spark and big data workloads. It maintains a transaction log that
 * enables time travel and other advanced features.
 *
 * <p><b>Requirements:</b>
 * <p>DuckDB Delta extension must be installed and loaded before using this reader:
 * <pre>
 *   INSTALL delta;
 *   LOAD delta;
 * </pre>
 *
 * <p><b>Time Travel:</b>
 * <p>Delta Lake supports querying historical versions of data using either:
 * <ul>
 *   <li>Version-based: {@code atVersion(42)} - reads version 42 of the table</li>
 *   <li>Timestamp-based: {@code atTimestamp("2025-10-14T10:00:00Z")} - reads the version
 *       that was current at the specified timestamp</li>
 * </ul>
 *
 * <p><b>Example usage:</b>
 * <pre>
 *   // Read current version of Delta table
 *   DeltaLakeReader reader = new DeltaLakeReader("path/to/delta-table");
 *   LogicalPlan plan = reader.toTableScan();
 *   String sql = reader.toSQL();
 *   // Generates: delta_scan('path/to/delta-table')
 *
 *   // Read specific version (version-based time travel)
 *   DeltaLakeReader reader = new DeltaLakeReader("path/to/delta-table")
 *       .atVersion(42);
 *   String sql = reader.toSQL();
 *   // Generates: delta_scan('path/to/delta-table', version => 42)
 *
 *   // Read at specific timestamp (timestamp-based time travel)
 *   DeltaLakeReader reader = new DeltaLakeReader("path/to/delta-table")
 *       .atTimestamp("2025-10-14T10:00:00Z");
 *   String sql = reader.toSQL();
 *   // Generates: delta_scan('path/to/delta-table', timestamp => '2025-10-14T10:00:00Z')
 *
 *   // Method chaining for fluent API
 *   LogicalPlan plan = new DeltaLakeReader("path/to/delta-table")
 *       .atVersion(10)
 *       .toTableScan();
 * </pre>
 *
 * <p><b>SQL Generation:</b>
 * <p>The reader generates DuckDB SQL using the {@code delta_scan} table function:
 * <ul>
 *   <li>Current version: {@code delta_scan('path/to/table')}</li>
 *   <li>Version-based: {@code delta_scan('path/to/table', version => 42)}</li>
 *   <li>Timestamp-based: {@code delta_scan('path/to/table', timestamp => '2025-10-14T10:00:00Z')}</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b>
 * <p>This class is <b>not</b> thread-safe. Instances should not be shared across threads
 * without external synchronization.
 *
 * @see TableScan
 * @see ParquetReader
 */
public class DeltaLakeReader {

    private final String tablePath;
    private Long version;
    private String timestamp;
    private StructType schema;

    /**
     * Creates a Delta Lake reader for the specified table path.
     *
     * <p>The table path should point to a directory containing Delta Lake metadata
     * and data files. The directory structure typically includes:
     * <ul>
     *   <li>{@code _delta_log/} - transaction log directory</li>
     *   <li>Parquet data files</li>
     * </ul>
     *
     * @param tablePath path to Delta Lake table directory (must not be null)
     * @throws NullPointerException if tablePath is null
     */
    public DeltaLakeReader(String tablePath) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
    }

    /**
     * Sets the version for version-based time travel.
     *
     * <p>Delta Lake versions start at 0 and increment with each transaction.
     * Each version represents a consistent snapshot of the table at a point in time.
     *
     * <p>If a timestamp was previously set, it will be cleared since only one
     * time travel mode can be active at a time.
     *
     * <p>This method returns the reader instance for method chaining.
     *
     * @param version the version number (must be >= 0)
     * @return this reader for method chaining
     * @throws IllegalArgumentException if version is negative
     *
     * @see #atTimestamp(String)
     */
    public DeltaLakeReader atVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0, got: " + version);
        }
        this.version = version;
        this.timestamp = null;  // Clear timestamp if version is set
        return this;
    }

    /**
     * Sets the timestamp for timestamp-based time travel.
     *
     * <p>The timestamp should be in ISO-8601 format, such as:
     * <ul>
     *   <li>{@code 2025-10-14T10:00:00Z} - UTC timestamp</li>
     *   <li>{@code 2025-10-14T10:00:00-05:00} - with timezone offset</li>
     * </ul>
     *
     * <p>Delta Lake will return the version of the table that was current
     * at the specified timestamp.
     *
     * <p>If a version was previously set, it will be cleared since only one
     * time travel mode can be active at a time.
     *
     * <p>This method returns the reader instance for method chaining.
     *
     * @param timestamp ISO-8601 timestamp string (must not be null)
     * @return this reader for method chaining
     * @throws NullPointerException if timestamp is null
     * @throws IllegalArgumentException if timestamp is empty or blank
     *
     * @see #atVersion(long)
     */
    public DeltaLakeReader atTimestamp(String timestamp) {
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        if (timestamp.trim().isEmpty()) {
            throw new IllegalArgumentException("timestamp must not be empty");
        }
        this.timestamp = timestamp;
        this.version = null;  // Clear version if timestamp is set
        return this;
    }

    /**
     * Sets the expected schema for the Delta Lake table.
     *
     * <p>If not set, DuckDB will infer the schema automatically when the query
     * is executed. Setting an explicit schema can be useful for:
     * <ul>
     *   <li>Type validation</li>
     *   <li>Column selection optimization</li>
     *   <li>Schema evolution handling</li>
     * </ul>
     *
     * @param schema the expected schema (must not be null)
     * @return this reader for method chaining
     * @throws NullPointerException if schema is null
     */
    public DeltaLakeReader withSchema(StructType schema) {
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        return this;
    }

    /**
     * Creates a TableScan logical plan node for this Delta Lake table.
     *
     * <p>The TableScan node can be used in logical plan construction and will
     * generate the appropriate DuckDB SQL when converted to SQL.
     *
     * <p>Any time travel settings (version or timestamp) are preserved in the
     * TableScan's options map.
     *
     * @return a TableScan logical plan node
     */
    public LogicalPlan toTableScan() {
        Map<String, String> options = new HashMap<>();

        if (version != null) {
            options.put("version", String.valueOf(version));
        } else if (timestamp != null) {
            options.put("timestamp", timestamp);
        }

        return new TableScan(tablePath, TableScan.TableFormat.DELTA, options, schema);
    }

    /**
     * Generates DuckDB SQL for reading this Delta Lake table.
     *
     * <p>The generated SQL uses DuckDB's {@code delta_scan} table function with
     * appropriate parameters based on the configured time travel settings.
     *
     * <p><b>Generated SQL Examples:</b>
     * <pre>
     *   // Current version
     *   delta_scan('path/to/table')
     *
     *   // Specific version
     *   delta_scan('path/to/table', version => 42)
     *
     *   // Specific timestamp
     *   delta_scan('path/to/table', timestamp => '2025-10-14T10:00:00Z')
     * </pre>
     *
     * <p><b>Path Escaping:</b>
     * <p>Single quotes in the table path are automatically escaped by doubling them
     * to prevent SQL injection and syntax errors. For example:
     * <pre>
     *   path/to/table's/data → delta_scan('path/to/table''s/data')
     * </pre>
     *
     * <p><b>Validation:</b>
     * <p>This method validates that only one of version or timestamp is set.
     * If both are set (which shouldn't happen due to mutual exclusion in
     * {@code atVersion} and {@code atTimestamp}), an {@code IllegalStateException}
     * is thrown.
     *
     * @return DuckDB SQL expression for reading this Delta Lake table
     * @throws IllegalStateException if both version and timestamp are set
     */
    public String toSQL() {
        // Validate that only one time travel mode is active
        if (version != null && timestamp != null) {
            throw new IllegalStateException(
                "Cannot set both version and timestamp. Use either atVersion() or atTimestamp(), not both.");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("delta_scan('").append(escapePath(tablePath)).append("'");

        if (version != null) {
            sql.append(", version => ").append(version);
        } else if (timestamp != null) {
            sql.append(", timestamp => '").append(escapeTimestamp(timestamp)).append("'");
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Escapes single quotes in file paths for SQL safety.
     *
     * <p>SQL standard requires single quotes to be escaped by doubling them.
     * This prevents SQL injection and syntax errors when paths contain quotes.
     *
     * <p>Example: {@code path/to/table's/data} becomes {@code path/to/table''s/data}
     *
     * @param path the file path
     * @return escaped path safe for use in SQL string literals
     */
    private String escapePath(String path) {
        return path.replace("'", "''");
    }

    /**
     * Escapes single quotes in timestamp strings for SQL safety.
     *
     * <p>Same as {@link #escapePath(String)} but semantically distinct for
     * timestamp values.
     *
     * @param ts the timestamp string
     * @return escaped timestamp safe for use in SQL string literals
     */
    private String escapeTimestamp(String ts) {
        return ts.replace("'", "''");
    }

    /**
     * Returns the table path.
     *
     * @return the table path
     */
    public String tablePath() {
        return tablePath;
    }

    /**
     * Returns the version for time travel, or null if not set.
     *
     * <p>A null value indicates either:
     * <ul>
     *   <li>No time travel is configured (reading current version)</li>
     *   <li>Timestamp-based time travel is configured instead</li>
     * </ul>
     *
     * @return the version, or null
     */
    public Long version() {
        return version;
    }

    /**
     * Returns the timestamp for time travel, or null if not set.
     *
     * <p>A null value indicates either:
     * <ul>
     *   <li>No time travel is configured (reading current version)</li>
     *   <li>Version-based time travel is configured instead</li>
     * </ul>
     *
     * @return the timestamp, or null
     */
    public String timestamp() {
        return timestamp;
    }

    /**
     * Returns the expected schema, or null if not set.
     *
     * <p>If null, DuckDB will infer the schema automatically.
     *
     * @return the schema, or null
     */
    public StructType schema() {
        return schema;
    }

    /**
     * Returns a string representation of this reader.
     *
     * <p>The string includes the table path and any time travel settings.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DeltaLakeReader(");
        sb.append("path=").append(tablePath);

        if (version != null) {
            sb.append(", version=").append(version);
        } else if (timestamp != null) {
            sb.append(", timestamp=").append(timestamp);
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Checks equality based on table path and time travel settings.
     *
     * @param obj the object to compare
     * @return true if equal
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        DeltaLakeReader that = (DeltaLakeReader) obj;
        return Objects.equals(tablePath, that.tablePath) &&
               Objects.equals(version, that.version) &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(schema, that.schema);
    }

    /**
     * Returns hash code based on table path and time travel settings.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(tablePath, version, timestamp, schema);
    }
}
