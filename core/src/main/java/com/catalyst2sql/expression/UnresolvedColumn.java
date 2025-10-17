package com.catalyst2sql.expression;

import com.catalyst2sql.types.DataType;
import com.catalyst2sql.types.StringType;
import com.catalyst2sql.generator.SQLQuoting;
import java.util.Objects;

/**
 * Expression representing an unresolved column reference.
 *
 * <p>This is used during plan conversion when the data type is not yet known.
 * The column will be resolved during query planning/optimization.
 */
public class UnresolvedColumn extends Expression {

    private final String columnName;
    private final String qualifier; // Optional table/alias qualifier

    /**
     * Creates an unresolved column reference with a qualifier.
     *
     * @param columnName the column name
     * @param qualifier the table or alias qualifier (may be null)
     */
    public UnresolvedColumn(String columnName, String qualifier) {
        this.columnName = Objects.requireNonNull(columnName, "columnName must not be null");
        this.qualifier = qualifier;
    }

    /**
     * Creates a simple unresolved column reference without a qualifier.
     *
     * @param columnName the column name
     */
    public UnresolvedColumn(String columnName) {
        this(columnName, null);
    }

    /**
     * Returns the column name.
     *
     * @return the column name
     */
    public String columnName() {
        return columnName;
    }

    /**
     * Returns the qualifier (table or alias).
     *
     * @return the qualifier, or null if not qualified
     */
    public String qualifier() {
        return qualifier;
    }

    /**
     * Returns whether this column reference is qualified.
     *
     * @return true if qualified, false otherwise
     */
    public boolean isQualified() {
        return qualifier != null;
    }

    @Override
    public DataType dataType() {
        // Type is unknown until resolution
        return StringType.get(); // Use a default type
    }

    @Override
    public boolean nullable() {
        // Assume nullable until resolved
        return true;
    }

    /**
     * Converts this column reference to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (qualifier != null) {
            return SQLQuoting.quoteIdentifierIfNeeded(qualifier) + "." +
                   SQLQuoting.quoteIdentifierIfNeeded(columnName);
        }
        return SQLQuoting.quoteIdentifierIfNeeded(columnName);
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UnresolvedColumn)) return false;
        UnresolvedColumn that = (UnresolvedColumn) obj;
        return Objects.equals(columnName, that.columnName) &&
               Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, qualifier);
    }
}