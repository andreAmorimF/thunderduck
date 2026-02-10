package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a Common Table Expression (WITH clause).
 *
 * <p>Holds a list of named CTE definitions and a body query that can reference them.
 * SQL generation produces: {@code WITH name1 AS (...), name2 AS (...) <body>}
 *
 * <p>Example:
 * <pre>
 * WITH cte1 AS (SELECT ...), cte2 AS (SELECT ...)
 * SELECT ... FROM cte1 JOIN cte2 ...
 * </pre>
 */
public final class WithCTE extends LogicalPlan {

    private final List<CTEDefinition> definitions;
    private final LogicalPlan body;

    /**
     * A single CTE definition: a name and its corresponding query plan.
     */
    public record CTEDefinition(String name, LogicalPlan plan, List<String> columnAliases) {
        public CTEDefinition {
            Objects.requireNonNull(name, "CTE name must not be null");
            Objects.requireNonNull(plan, "CTE plan must not be null");
            if (columnAliases == null) {
                columnAliases = Collections.emptyList();
            }
        }

        public CTEDefinition(String name, LogicalPlan plan) {
            this(name, plan, Collections.emptyList());
        }
    }

    /**
     * Creates a WithCTE node.
     *
     * @param definitions the CTE definitions (name + plan pairs)
     * @param body the main query that references the CTEs
     */
    public WithCTE(List<CTEDefinition> definitions, LogicalPlan body) {
        super(body);
        this.definitions = new ArrayList<>(
            Objects.requireNonNull(definitions, "definitions must not be null"));
        this.body = Objects.requireNonNull(body, "body must not be null");
        if (definitions.isEmpty()) {
            throw new IllegalArgumentException("WITH clause requires at least one CTE definition");
        }
    }

    public List<CTEDefinition> definitions() {
        return Collections.unmodifiableList(definitions);
    }

    public LogicalPlan body() {
        return body;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        StringBuilder sql = new StringBuilder("WITH ");

        for (int i = 0; i < definitions.size(); i++) {
            if (i > 0) sql.append(", ");
            CTEDefinition def = definitions.get(i);
            sql.append(com.thunderduck.generator.SQLQuoting.quoteIdentifierIfNeeded(def.name()));

            if (!def.columnAliases().isEmpty()) {
                sql.append("(");
                sql.append(String.join(", ", def.columnAliases().stream()
                    .map(com.thunderduck.generator.SQLQuoting::quoteIdentifierIfNeeded)
                    .toList()));
                sql.append(")");
            }

            sql.append(" AS (");
            sql.append(generator.generate(def.plan()));
            sql.append(")");
        }

        sql.append(" ");
        sql.append(generator.generate(body));

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        return body.inferSchema();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WithCTE(");
        for (int i = 0; i < definitions.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(definitions.get(i).name());
        }
        sb.append(" -> body=").append(body);
        sb.append(")");
        return sb.toString();
    }
}
