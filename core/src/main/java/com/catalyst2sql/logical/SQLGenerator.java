package com.catalyst2sql.logical;

/**
 * Marker interface for SQL generation.
 *
 * <p>The actual SQL generator implementation will be provided in the sql package.
 * This interface is used to maintain a clean separation between the logical plan
 * representation and the SQL generation logic.
 *
 * <p>Implementations should provide methods to translate logical plan nodes to
 * DuckDB SQL strings.
 */
public interface SQLGenerator {
    // Marker interface - actual methods will be added in sql package
}
