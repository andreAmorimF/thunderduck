package com.thunderduck.logical;

import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a relation with a user-provided alias.
 *
 * <p>This is used for DataFrame.alias() operations. Unlike SQLRelation which
 * wraps everything in SQL text, AliasedRelation preserves the alias information
 * so that joins can reference it directly in the join condition.
 *
 * <p>Example:
 * <pre>
 *   date_dim.alias("d1").join(date_dim.alias("d2"), col("d1.d_date_sk") == col("d2.d_date_sk"))
 * </pre>
 *
 * <p>The aliases "d1" and "d2" must be accessible in the join condition, so we can't
 * wrap them in another subquery with a generated alias. This class preserves the
 * user alias so Join.toSQL() can use it directly.
 */
public final class AliasedRelation extends LogicalPlan {

    private final LogicalPlan child;
    private final String alias;
    private final List<String> columnAliases;

    /**
     * Creates an aliased relation.
     *
     * @param child the underlying relation
     * @param alias the user-provided alias
     */
    public AliasedRelation(LogicalPlan child, String alias) {
        this(child, alias, Collections.emptyList());
    }

    /**
     * Creates an aliased relation with column aliases.
     * Column aliases rename the output columns of the child relation.
     * e.g., {@code (subquery) AS alias (col1, col2)}
     *
     * @param child the underlying relation
     * @param alias the user-provided alias
     * @param columnAliases optional column aliases
     */
    public AliasedRelation(LogicalPlan child, String alias, List<String> columnAliases) {
        super(Collections.singletonList(child));
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.alias = Objects.requireNonNull(alias, "alias must not be null");
        this.columnAliases = columnAliases != null ? columnAliases : Collections.emptyList();
        if (alias.isEmpty()) {
            throw new IllegalArgumentException("alias must not be empty");
        }
    }

    /**
     * Returns the child relation.
     *
     * @return the child plan
     */
    public LogicalPlan child() {
        return child;
    }

    /**
     * Returns the user-provided alias.
     *
     * @return the alias
     */
    public String alias() {
        return alias;
    }

    /**
     * Returns the optional column aliases.
     *
     * @return the column aliases (empty list if none)
     */
    public List<String> columnAliases() {
        return columnAliases;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // Generate SQL with explicit alias
        String childSql = generator.generate(child);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (").append(childSql).append(") AS ");
        sb.append(SQLQuoting.quoteIdentifier(alias));
        if (!columnAliases.isEmpty()) {
            sb.append(" (");
            for (int i = 0; i < columnAliases.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(SQLQuoting.quoteIdentifier(columnAliases.get(i)));
            }
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public StructType inferSchema() {
        StructType childSchema = child.schema();

        // If there are column aliases and the child schema has matching column count,
        // rename the columns in the schema. This handles e.g. VALUES ... AS t(id, name)
        // where the child schema has col1, col2 but the aliases rename them.
        if (childSchema != null && !columnAliases.isEmpty()
                && childSchema.size() == columnAliases.size()) {
            java.util.List<com.thunderduck.types.StructField> renamedFields = new java.util.ArrayList<>();
            for (int i = 0; i < childSchema.size(); i++) {
                com.thunderduck.types.StructField original = childSchema.fields().get(i);
                renamedFields.add(new com.thunderduck.types.StructField(
                    columnAliases.get(i), original.dataType(), original.nullable()));
            }
            return new StructType(renamedFields);
        }

        return childSchema;
    }

    @Override
    public String toString() {
        return String.format("AliasedRelation[%s]", alias);
    }
}
