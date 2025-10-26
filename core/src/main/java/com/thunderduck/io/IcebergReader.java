package com.thunderduck.io;

import com.thunderduck.logical.TableScan;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Reader for Apache Iceberg tables using DuckDB Iceberg extension.
 *
 * <p>Provides support for:
 * <ul>
 *   <li>Snapshot isolation (read from specific snapshots)</li>
 *   <li>Time travel queries (read table state at specific timestamps)</li>
 *   <li>Metadata table queries</li>
 *   <li>Schema evolution tracking</li>
 *   <li>Partition evolution support</li>
 * </ul>
 *
 * <p>Requires DuckDB Iceberg extension to be installed and loaded:
 * <pre>
 *   INSTALL iceberg;
 *   LOAD iceberg;
 * </pre>
 *
 * <p>Example usage:
 * <pre>
 *   // Read current snapshot
 *   IcebergReader reader = new IcebergReader("path/to/iceberg/table");
 *   LogicalPlan plan = reader.toTableScan();
 *   String sql = reader.toSQL();  // iceberg_scan('path/to/iceberg/table')
 *
 *   // Read specific snapshot (snapshot isolation)
 *   IcebergReader reader = new IcebergReader("path/to/iceberg/table")
 *       .atSnapshot(1234567890L);
 *   String sql = reader.toSQL();  // iceberg_scan('path/to/iceberg/table', snapshot_id => 1234567890)
 *
 *   // Time travel to specific timestamp
 *   IcebergReader reader = new IcebergReader("path/to/iceberg/table")
 *       .asOf("2025-10-14T10:00:00Z");
 *   String sql = reader.toSQL();  // iceberg_scan('path/to/iceberg/table', as_of => '2025-10-14T10:00:00Z')
 * </pre>
 *
 * <p><b>Note on Snapshot Isolation vs Time Travel:</b>
 * Only one of {@code snapshotId} or {@code asOfTimestamp} can be set at a time.
 * Setting one will automatically clear the other.
 *
 * @see TableScan
 * @see ParquetReader
 */
public class IcebergReader {

    private final String tablePath;
    private Long snapshotId;
    private String asOfTimestamp;

    /**
     * Creates an Iceberg reader for the specified table path.
     *
     * <p>The table path should point to the Iceberg table directory containing
     * the metadata directory. DuckDB will automatically locate and read the
     * table metadata.
     *
     * @param tablePath path to Iceberg table directory or metadata file
     * @throws NullPointerException if tablePath is null
     */
    public IcebergReader(String tablePath) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.snapshotId = null;
        this.asOfTimestamp = null;
    }

    /**
     * Sets the snapshot ID for snapshot isolation.
     *
     * <p>Snapshot isolation allows reading the table state at a specific snapshot.
     * This is useful for:
     * <ul>
     *   <li>Reproducible reads across multiple queries</li>
     *   <li>Auditing and compliance (read historical state)</li>
     *   <li>A/B testing with different data versions</li>
     * </ul>
     *
     * <p>Setting a snapshot ID will clear any previously set timestamp.
     * You cannot use both snapshot isolation and time travel simultaneously.
     *
     * @param snapshotId the snapshot ID (must be a valid Iceberg snapshot ID)
     * @return this reader for method chaining
     * @throws IllegalArgumentException if snapshotId is negative
     */
    public IcebergReader atSnapshot(long snapshotId) {
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must be non-negative, got: " + snapshotId);
        }
        this.snapshotId = snapshotId;
        this.asOfTimestamp = null;  // Clear asOf if snapshot is set
        return this;
    }

    /**
     * Sets the as-of timestamp for time travel queries.
     *
     * <p>Time travel allows reading the table state as it existed at a specific
     * point in time. The timestamp should be in ISO-8601 format.
     *
     * <p>Supported timestamp formats:
     * <ul>
     *   <li>ISO-8601 with timezone: {@code 2025-10-14T10:00:00Z}</li>
     *   <li>ISO-8601 with offset: {@code 2025-10-14T10:00:00-07:00}</li>
     *   <li>ISO-8601 date only: {@code 2025-10-14}</li>
     * </ul>
     *
     * <p>Setting a timestamp will clear any previously set snapshot ID.
     * You cannot use both snapshot isolation and time travel simultaneously.
     *
     * @param timestamp ISO-8601 timestamp string
     * @return this reader for method chaining
     * @throws NullPointerException if timestamp is null
     */
    public IcebergReader asOf(String timestamp) {
        this.asOfTimestamp = Objects.requireNonNull(timestamp, "asOf timestamp must not be null");
        this.snapshotId = null;  // Clear snapshot if asOf is set
        return this;
    }

    /**
     * Creates a TableScan logical plan node for this Iceberg table.
     *
     * <p>The returned TableScan can be used as part of a larger logical plan
     * and will generate the appropriate DuckDB SQL when converted to SQL.
     *
     * <p>The TableScan will include options for snapshot isolation or time travel
     * if they have been set.
     *
     * @return a TableScan logical plan node
     */
    public TableScan toTableScan() {
        Map<String, String> options = new HashMap<>();

        if (snapshotId != null) {
            options.put("snapshot_id", String.valueOf(snapshotId));
        } else if (asOfTimestamp != null) {
            options.put("as_of", asOfTimestamp);
        }

        return new TableScan(tablePath, TableScan.TableFormat.ICEBERG, options, null);
    }

    /**
     * Generates DuckDB SQL for reading this Iceberg table.
     *
     * <p>The generated SQL uses DuckDB's {@code iceberg_scan()} function with
     * appropriate parameters for snapshot isolation or time travel.
     *
     * <p>Examples:
     * <pre>
     *   // Current snapshot
     *   iceberg_scan('path/to/table')
     *
     *   // Specific snapshot (snapshot isolation)
     *   iceberg_scan('path/to/table', snapshot_id => 1234567890)
     *
     *   // Specific timestamp (time travel)
     *   iceberg_scan('path/to/table', as_of => '2025-10-14T10:00:00Z')
     * </pre>
     *
     * <p>Paths containing single quotes are properly escaped by doubling them.
     *
     * @return DuckDB SQL expression for iceberg_scan()
     */
    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("iceberg_scan('").append(escapePath(tablePath)).append("'");

        if (snapshotId != null) {
            sql.append(", snapshot_id => ").append(snapshotId);
        } else if (asOfTimestamp != null) {
            // Escape the timestamp string as well
            sql.append(", as_of => '").append(escapeString(asOfTimestamp)).append("'");
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Escapes single quotes in file paths for SQL safety.
     *
     * <p>In SQL, single quotes are escaped by doubling them.
     * For example: {@code path/to/file's/table} becomes {@code path/to/file''s/table}
     *
     * @param path the file path to escape
     * @return escaped path safe for SQL string literals
     */
    private String escapePath(String path) {
        return path.replace("'", "''");
    }

    /**
     * Escapes single quotes in string values for SQL safety.
     *
     * <p>This is used for timestamp strings and other string parameters.
     *
     * @param value the string value to escape
     * @return escaped string safe for SQL string literals
     */
    private String escapeString(String value) {
        return value.replace("'", "''");
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
     * Returns the snapshot ID for snapshot isolation, or null for current snapshot.
     *
     * @return the snapshot ID, or null if not set
     */
    public Long snapshotId() {
        return snapshotId;
    }

    /**
     * Returns the as-of timestamp for time travel, or null for current snapshot.
     *
     * @return the as-of timestamp, or null if not set
     */
    public String asOf() {
        return asOfTimestamp;
    }

    /**
     * Returns a string representation of this reader configuration.
     *
     * @return string representation including path and time travel settings
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IcebergReader(");
        sb.append("tablePath='").append(tablePath).append("'");

        if (snapshotId != null) {
            sb.append(", snapshotId=").append(snapshotId);
        } else if (asOfTimestamp != null) {
            sb.append(", asOf='").append(asOfTimestamp).append("'");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Checks equality based on table path and time travel settings.
     *
     * @param obj object to compare with
     * @return true if equal
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IcebergReader that = (IcebergReader) obj;
        return Objects.equals(tablePath, that.tablePath)
            && Objects.equals(snapshotId, that.snapshotId)
            && Objects.equals(asOfTimestamp, that.asOfTimestamp);
    }

    /**
     * Computes hash code based on table path and time travel settings.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(tablePath, snapshotId, asOfTimestamp);
    }
}
