package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Collections;
import java.util.Objects;

/**
 * A logical plan node that represents a DDL/DML statement as pre-generated SQL.
 *
 * <p>DDL statements (CREATE TABLE, DROP TABLE, TRUNCATE TABLE, etc.) and DML statements
 * (INSERT INTO) don't return data, so this node has an empty schema. The SQL is generated
 * by the SparkSQL parser's visitor methods with type name mapping (Spark STRING -> DuckDB VARCHAR)
 * and identifier quoting applied.
 *
 * <p>This node is a pass-through: it holds the final DuckDB SQL string and returns it directly
 * from {@link #toSQL(SQLGenerator)}. The SQLGenerator simply emits the SQL as-is.
 *
 * @see com.thunderduck.parser.SparkSQLAstBuilder
 */
public final class RawDDLStatement extends LogicalPlan {

    private final String sql;

    /**
     * Creates a new RawDDLStatement with the given DuckDB SQL.
     *
     * @param sql the DuckDB-compatible DDL/DML SQL string
     */
    public RawDDLStatement(String sql) {
        super(); // No children
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    /**
     * Returns the DuckDB SQL string for this DDL/DML statement.
     *
     * @return the SQL string
     */
    public String sql() {
        return sql;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        return sql;
    }

    @Override
    public StructType inferSchema() {
        return new StructType(Collections.emptyList());
    }

    @Override
    public String toString() {
        String preview = sql.length() > 50 ? sql.substring(0, 50) + "..." : sql;
        return "RawDDLStatement(" + preview + ")";
    }
}
