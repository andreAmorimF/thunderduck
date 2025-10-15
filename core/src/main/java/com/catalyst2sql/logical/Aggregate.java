package com.catalyst2sql.logical;

import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.types.StructField;
import com.catalyst2sql.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing an aggregation (GROUP BY clause with aggregates).
 *
 * <p>This node groups rows from its child and computes aggregate functions.
 * Optionally supports HAVING clause to filter aggregated results.
 *
 * <p>Examples:
 * <pre>
 *   df.groupBy("category").agg(sum("amount"), avg("price"))
 *   df.groupBy("year", "month").count()
 *   df.groupBy("customer_id").agg(count("*")).having(col("count") > 5)
 * </pre>
 *
 * <p>SQL generation:
 * <pre>
 * SELECT groupingExpr1, groupingExpr2, aggFunc1, aggFunc2
 * FROM (child)
 * GROUP BY groupingExpr1, groupingExpr2
 * HAVING condition
 * </pre>
 */
public class Aggregate extends LogicalPlan {

    private final List<Expression> groupingExpressions;
    private final List<AggregateExpression> aggregateExpressions;
    private final Expression havingCondition;

    /**
     * Creates an aggregate node with optional HAVING clause.
     *
     * @param child the child node
     * @param groupingExpressions the grouping expressions (empty for global aggregation)
     * @param aggregateExpressions the aggregate expressions (sum, avg, count, etc.)
     * @param havingCondition the HAVING condition (can be null)
     */
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions,
                    Expression havingCondition) {
        super(child);
        this.groupingExpressions = new ArrayList<>(
            Objects.requireNonNull(groupingExpressions, "groupingExpressions must not be null"));
        this.aggregateExpressions = new ArrayList<>(
            Objects.requireNonNull(aggregateExpressions, "aggregateExpressions must not be null"));
        this.havingCondition = havingCondition;  // Can be null
    }

    /**
     * Creates an aggregate node without HAVING clause (backward compatibility).
     *
     * @param child the child node
     * @param groupingExpressions the grouping expressions (empty for global aggregation)
     * @param aggregateExpressions the aggregate expressions (sum, avg, count, etc.)
     */
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions) {
        this(child, groupingExpressions, aggregateExpressions, null);
    }

    /**
     * Returns the grouping expressions.
     *
     * @return an unmodifiable list of grouping expressions
     */
    public List<Expression> groupingExpressions() {
        return Collections.unmodifiableList(groupingExpressions);
    }

    /**
     * Returns the aggregate expressions.
     *
     * @return an unmodifiable list of aggregate expressions
     */
    public List<AggregateExpression> aggregateExpressions() {
        return Collections.unmodifiableList(aggregateExpressions);
    }

    /**
     * Returns the HAVING condition.
     *
     * @return the HAVING condition expression, or null if no HAVING clause
     */
    public Expression havingCondition() {
        return havingCondition;
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        if (aggregateExpressions.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate SQL for aggregation with no aggregate expressions");
        }

        StringBuilder sql = new StringBuilder();

        // SELECT clause with grouping expressions and aggregates
        sql.append("SELECT ");

        List<String> selectExprs = new ArrayList<>();

        // Add grouping columns
        for (Expression expr : groupingExpressions) {
            selectExprs.add(expr.toSQL());
        }

        // Add aggregate expressions
        for (AggregateExpression aggExpr : aggregateExpressions) {
            String aggSQL = aggExpr.toSQL();
            // Add alias if provided
            if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
                aggSQL += " AS " + com.catalyst2sql.generator.SQLQuoting.quoteIdentifier(aggExpr.alias());
            }
            selectExprs.add(aggSQL);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause
        sql.append(" FROM (");
        sql.append(generator.generate(child()));
        sql.append(") AS ").append(generator.generateSubqueryAlias());

        // GROUP BY clause
        if (!groupingExpressions.isEmpty()) {
            sql.append(" GROUP BY ");
            List<String> groupExprs = new ArrayList<>();
            for (Expression expr : groupingExpressions) {
                groupExprs.add(expr.toSQL());
            }
            sql.append(String.join(", ", groupExprs));
        }

        // HAVING clause
        if (havingCondition != null) {
            sql.append(" HAVING ");
            sql.append(havingCondition.toSQL());
        }

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        List<StructField> fields = new ArrayList<>();

        // Add grouping fields
        for (int i = 0; i < groupingExpressions.size(); i++) {
            Expression expr = groupingExpressions.get(i);
            fields.add(new StructField("group_" + i, expr.dataType(), expr.nullable()));
        }

        // Add aggregate fields
        for (int i = 0; i < aggregateExpressions.size(); i++) {
            AggregateExpression aggExpr = aggregateExpressions.get(i);
            String name = aggExpr.alias != null ? aggExpr.alias : ("agg_" + i);
            fields.add(new StructField(name, aggExpr.dataType(), aggExpr.nullable()));
        }

        return new StructType(fields);
    }

    @Override
    public String toString() {
        if (havingCondition != null) {
            return String.format("Aggregate(groupBy=%s, agg=%s, having=%s)",
                               groupingExpressions, aggregateExpressions, havingCondition);
        }
        return String.format("Aggregate(groupBy=%s, agg=%s)",
                           groupingExpressions, aggregateExpressions);
    }

    /**
     * Represents an aggregate expression (e.g., SUM(amount), AVG(price)).
     *
     * <p>Supports DISTINCT keyword for unique value aggregation:
     * <pre>
     *   COUNT(DISTINCT customer_id)
     *   SUM(DISTINCT price)
     *   AVG(DISTINCT amount)
     * </pre>
     */
    public static class AggregateExpression extends Expression {
        private final String function;
        private final Expression argument;
        private final String alias;
        private final boolean distinct;

        /**
         * Creates an aggregate expression with optional DISTINCT modifier.
         *
         * @param function the aggregate function name (COUNT, SUM, AVG, MIN, MAX, etc.)
         * @param argument the expression to aggregate (can be null for COUNT(*))
         * @param alias the result column alias (can be null)
         * @param distinct whether to aggregate only distinct values
         */
        public AggregateExpression(String function, Expression argument, String alias, boolean distinct) {
            this.function = Objects.requireNonNull(function, "function must not be null");
            this.argument = argument;
            this.alias = alias;
            this.distinct = distinct;
        }

        /**
         * Creates an aggregate expression without DISTINCT (backward compatibility).
         *
         * @param function the aggregate function name
         * @param argument the expression to aggregate (can be null for COUNT(*))
         * @param alias the result column alias (can be null)
         */
        public AggregateExpression(String function, Expression argument, String alias) {
            this(function, argument, alias, false);
        }

        /**
         * Returns the aggregate function name.
         *
         * @return the function name (e.g., "COUNT", "SUM", "AVG")
         */
        public String function() {
            return function;
        }

        /**
         * Returns the expression being aggregated.
         *
         * @return the argument expression, or null for COUNT(*)
         */
        public Expression argument() {
            return argument;
        }

        /**
         * Returns the result column alias.
         *
         * @return the alias, or null if not specified
         */
        public String alias() {
            return alias;
        }

        /**
         * Returns whether this aggregate uses DISTINCT.
         *
         * @return true if aggregating distinct values only
         */
        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public com.catalyst2sql.types.DataType dataType() {
            // Type inference will depend on the function
            // For now, return the argument type (will be improved later)
            return argument != null ? argument.dataType() : null;
        }

        @Override
        public boolean nullable() {
            // Aggregates can generally produce nulls (e.g., AVG of empty set)
            return true;
        }

        @Override
        public String toSQL() {
            StringBuilder sql = new StringBuilder();
            sql.append(function.toUpperCase());
            sql.append("(");

            // Add DISTINCT keyword if specified
            if (distinct) {
                sql.append("DISTINCT ");
            }

            // Add argument or * for COUNT(*)
            if (argument != null) {
                sql.append(argument.toSQL());
            } else {
                // COUNT(*) case - DISTINCT not allowed with *
                if (!distinct) {
                    sql.append("*");
                }
                // If distinct is true and argument is null, we don't add *
                // This should be validated elsewhere as an error case
            }

            sql.append(")");
            return sql.toString();
        }

        @Override
        public String toString() {
            return toSQL();
        }
    }
}
