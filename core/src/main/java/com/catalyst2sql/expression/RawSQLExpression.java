package com.catalyst2sql.expression;

import com.catalyst2sql.types.DataType;
import com.catalyst2sql.types.StringType;
import java.util.Objects;

/**
 * Expression that represents a raw SQL expression string.
 *
 * <p>This is used when expressions are provided as SQL strings that
 * should be passed through directly without parsing.
 *
 * <p>Examples:
 * <pre>
 *   CASE WHEN status = 'active' THEN 1 ELSE 0 END
 *   COALESCE(first_name, last_name, 'Unknown')
 * </pre>
 */
public class RawSQLExpression extends Expression {

    private final String sql;

    /**
     * Creates a raw SQL expression.
     *
     * @param sql the SQL expression string
     */
    public RawSQLExpression(String sql) {
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    /**
     * Returns the SQL expression string.
     *
     * @return the SQL string
     */
    public String sql() {
        return sql;
    }

    @Override
    public DataType dataType() {
        // We can't determine the type without parsing the SQL
        // Return a generic type
        return StringType.INSTANCE;
    }

    @Override
    public boolean nullable() {
        // We can't determine nullability without parsing
        // Assume nullable to be safe
        return true;
    }

    /**
     * Returns the raw SQL string.
     *
     * @return the SQL string
     */
    public String toSQL() {
        return sql;
    }

    @Override
    public String toString() {
        return "RawSQL(" + sql + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RawSQLExpression)) return false;
        RawSQLExpression that = (RawSQLExpression) obj;
        return Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql);
    }
}