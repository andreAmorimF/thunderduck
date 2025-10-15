package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.logical.Aggregate.AggregateExpression;
import com.catalyst2sql.types.StructField;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Optimization rule that pushes filters down towards data sources.
 *
 * <p>Moving filters closer to table scans can significantly reduce the amount
 * of data processed by subsequent operations. This rule attempts to push
 * filters through other operators when safe to do so.
 *
 * <h2>Transformation Examples</h2>
 *
 * <h3>Push through Project</h3>
 * <pre>
 * Before: Filter(age > 25, Project([name, age], Scan))
 * After:  Project([name, age], Filter(age > 25, Scan))
 * </pre>
 *
 * <h3>Push into Join</h3>
 * <pre>
 * Before: Filter(t1.age > 25 AND t2.active = true, Join(t1, t2))
 * After:  Join(Filter(age > 25, t1), Filter(active = true, t2))
 * </pre>
 *
 * <h3>Push through Aggregate</h3>
 * <pre>
 * Before: Filter(category = 'electronics', Aggregate(groupBy=[category], ...))
 * After:  Aggregate(groupBy=[category], Filter(category = 'electronics', child))
 * </pre>
 *
 * <h3>Push through Union</h3>
 * <pre>
 * Before: Filter(active = true, Union(t1, t2))
 * After:  Union(Filter(active = true, t1), Filter(active = true, t2))
 * </pre>
 *
 * <h2>Safety Conditions</h2>
 *
 * <ul>
 *   <li><b>Project</b>: Filter only references columns that are projected</li>
 *   <li><b>Join</b>: Split predicates based on column origin (left/right/both)</li>
 *   <li><b>Aggregate</b>: Filter only references grouping keys (not aggregates)</li>
 *   <li><b>Union</b>: Always safe to push through both branches</li>
 * </ul>
 *
 * <p>The rule only applies transformations that preserve query semantics and
 * improve performance. Filters that reference computed columns cannot be pushed
 * below the computation.
 *
 * @see OptimizationRule
 */
public class FilterPushdownRule implements OptimizationRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }

        // Apply recursively to children first (bottom-up traversal)
        LogicalPlan transformed = applyToChildren(plan);

        // If this is a Filter node, try to push it down
        if (transformed instanceof Filter) {
            Filter filter = (Filter) transformed;
            LogicalPlan child = filter.child();

            // Case 1: Push through Project
            if (child instanceof Project) {
                return pushThroughProject(filter, (Project) child);
            }

            // Case 2: Push into Join
            if (child instanceof Join) {
                return pushIntoJoin(filter, (Join) child);
            }

            // Case 3: Push through Aggregate (if safe)
            if (child instanceof Aggregate) {
                return pushThroughAggregate(filter, (Aggregate) child);
            }

            // Case 4: Push through Union
            if (child instanceof Union) {
                return pushThroughUnion(filter, (Union) child);
            }
        }

        return transformed;
    }

    /**
     * Applies the rule recursively to all children of a plan node.
     *
     * @param plan the plan node
     * @return the plan with rule applied to all children
     */
    private LogicalPlan applyToChildren(LogicalPlan plan) {
        List<LogicalPlan> children = plan.children();

        if (children.isEmpty()) {
            return plan;
        }

        // Apply rule to each child
        List<LogicalPlan> transformedChildren = new ArrayList<>();
        boolean changed = false;

        for (LogicalPlan child : children) {
            LogicalPlan transformedChild = apply(child);
            transformedChildren.add(transformedChild);
            if (transformedChild != child) {
                changed = true;
            }
        }

        // If no children changed, return original plan
        if (!changed) {
            return plan;
        }

        // Reconstruct plan with transformed children
        return reconstructPlan(plan, transformedChildren);
    }

    /**
     * Reconstructs a plan node with new children.
     *
     * @param plan the original plan
     * @param newChildren the new children
     * @return a new plan with the updated children
     */
    private LogicalPlan reconstructPlan(LogicalPlan plan, List<LogicalPlan> newChildren) {
        // Handle each plan type
        if (plan instanceof Filter) {
            Filter filter = (Filter) plan;
            return new Filter(newChildren.get(0), filter.condition());
        } else if (plan instanceof Project) {
            Project project = (Project) plan;
            return new Project(newChildren.get(0), project.projections(), project.aliases());
        } else if (plan instanceof Join) {
            Join join = (Join) plan;
            return new Join(newChildren.get(0), newChildren.get(1), join.joinType(), join.condition());
        } else if (plan instanceof Aggregate) {
            Aggregate agg = (Aggregate) plan;
            return new Aggregate(newChildren.get(0), agg.groupingExpressions(), agg.aggregateExpressions());
        } else if (plan instanceof Union) {
            Union union = (Union) plan;
            return new Union(newChildren.get(0), newChildren.get(1), union.all());
        } else if (plan instanceof Sort) {
            Sort sort = (Sort) plan;
            return new Sort(newChildren.get(0), sort.sortOrders());
        } else if (plan instanceof Limit) {
            Limit limit = (Limit) plan;
            return new Limit(newChildren.get(0), limit.limit(), limit.offset());
        }

        // For unknown plan types, return as is
        return plan;
    }

    /**
     * Pushes a filter through a Project node.
     *
     * <p>Safe only if the filter condition references only columns that are
     * projected (or available from the project's input).
     *
     * <p>Example:
     * <pre>
     * Filter(age > 25, Project([name, age], Scan))
     * → Project([name, age], Filter(age > 25, Scan))
     * </pre>
     *
     * @param filter the filter node
     * @param project the project node
     * @return the transformed plan, or original if unsafe
     */
    private LogicalPlan pushThroughProject(Filter filter, Project project) {
        // Get columns referenced in filter condition
        Set<String> filterColumns = getReferencedColumns(filter.condition());

        // Get columns available from project's input
        Set<String> availableColumns = getAvailableColumns(project.child());

        // Can only push if all filter columns are available in the input
        if (availableColumns.containsAll(filterColumns)) {
            // Safe to push: Project(Filter(child))
            Filter pushedFilter = new Filter(project.child(), filter.condition());
            return new Project(pushedFilter, project.projections(), project.aliases());
        }

        // Not safe to push (filter references computed columns), return unchanged
        return filter;
    }

    /**
     * Pushes a filter into a Join node.
     *
     * <p>Splits the filter condition into predicates that can be pushed to:
     * <ul>
     *   <li>Left input only (references only left columns)</li>
     *   <li>Right input only (references only right columns)</li>
     *   <li>Remaining (references both sides or complex expressions)</li>
     * </ul>
     *
     * <p>Example:
     * <pre>
     * Filter(t1.age > 25 AND t2.active = true AND t1.id = t2.id, Join(t1, t2))
     * → Join(Filter(age > 25, t1), Filter(active = true, t2), condition: t1.id = t2.id)
     * </pre>
     *
     * @param filter the filter node
     * @param join the join node
     * @return the transformed plan
     */
    private LogicalPlan pushIntoJoin(Filter filter, Join join) {
        // Get available columns from left and right inputs
        Set<String> leftColumns = getAvailableColumns(join.left());
        Set<String> rightColumns = getAvailableColumns(join.right());

        // Split filter condition into conjuncts (AND-separated predicates)
        List<Expression> conjuncts = splitConjunction(filter.condition());

        // Categorize predicates
        List<Expression> leftFilters = new ArrayList<>();
        List<Expression> rightFilters = new ArrayList<>();
        List<Expression> remainingFilters = new ArrayList<>();

        for (Expression conjunct : conjuncts) {
            Set<String> referencedCols = getReferencedColumns(conjunct);

            if (leftColumns.containsAll(referencedCols)) {
                // References only left columns
                leftFilters.add(conjunct);
            } else if (rightColumns.containsAll(referencedCols)) {
                // References only right columns
                rightFilters.add(conjunct);
            } else {
                // References both sides or is complex
                remainingFilters.add(conjunct);
            }
        }

        // Apply filters to inputs
        LogicalPlan newLeft = join.left();
        LogicalPlan newRight = join.right();

        if (!leftFilters.isEmpty()) {
            Expression leftCondition = combineWithAnd(leftFilters);
            newLeft = new Filter(newLeft, leftCondition);
        }

        if (!rightFilters.isEmpty()) {
            Expression rightCondition = combineWithAnd(rightFilters);
            newRight = new Filter(newRight, rightCondition);
        }

        // Recreate join with filtered inputs
        LogicalPlan newJoin = new Join(newLeft, newRight, join.joinType(), join.condition());

        // If there are remaining filters, wrap the join
        if (!remainingFilters.isEmpty()) {
            Expression remainingCondition = combineWithAnd(remainingFilters);
            return new Filter(newJoin, remainingCondition);
        }

        return newJoin;
    }

    /**
     * Pushes a filter through an Aggregate node.
     *
     * <p>Safe only if the filter condition references only grouping keys,
     * not aggregate functions.
     *
     * <p>Example (safe):
     * <pre>
     * Filter(category = 'electronics', Aggregate(groupBy=[category], agg=[sum(price)]))
     * → Aggregate(groupBy=[category], agg=[sum(price)], Filter(category = 'electronics', child))
     * </pre>
     *
     * <p>Example (unsafe - filter references aggregate):
     * <pre>
     * Filter(sum_price > 1000, Aggregate(...))
     * → Cannot push (returns unchanged)
     * </pre>
     *
     * @param filter the filter node
     * @param aggregate the aggregate node
     * @return the transformed plan, or original if unsafe
     */
    private LogicalPlan pushThroughAggregate(Filter filter, Aggregate aggregate) {
        // Get grouping column names
        Set<String> groupingColumns = getGroupingColumns(aggregate);

        // Get columns referenced in filter
        Set<String> filterColumns = getReferencedColumns(filter.condition());

        // Can only push if filter only references grouping keys
        if (groupingColumns.containsAll(filterColumns)) {
            // Safe to push: Aggregate(Filter(child))
            Filter pushedFilter = new Filter(aggregate.child(), filter.condition());
            return new Aggregate(pushedFilter, aggregate.groupingExpressions(),
                               aggregate.aggregateExpressions());
        }

        // Not safe (filter references aggregates), return unchanged
        return filter;
    }

    /**
     * Pushes a filter through a Union node.
     *
     * <p>Always safe to push filters through union - apply to both branches.
     *
     * <p>Example:
     * <pre>
     * Filter(active = true, Union(t1, t2))
     * → Union(Filter(active = true, t1), Filter(active = true, t2))
     * </pre>
     *
     * @param filter the filter node
     * @param union the union node
     * @return the transformed plan
     */
    private LogicalPlan pushThroughUnion(Filter filter, Union union) {
        // Always safe to push through union - apply to both sides
        Filter leftFilter = new Filter(union.left(), filter.condition());
        Filter rightFilter = new Filter(union.right(), filter.condition());
        return new Union(leftFilter, rightFilter, union.all());
    }

    // ==================== Helper Methods ====================

    /**
     * Extracts all column names referenced in an expression.
     *
     * <p>Recursively traverses the expression tree to find all ColumnReference nodes.
     *
     * @param expr the expression to analyze
     * @return set of referenced column names
     */
    private Set<String> getReferencedColumns(Expression expr) {
        Set<String> columns = new HashSet<>();
        collectReferencedColumns(expr, columns);
        return columns;
    }

    /**
     * Recursively collects column references from an expression.
     *
     * @param expr the expression
     * @param columns the set to accumulate column names
     */
    private void collectReferencedColumns(Expression expr, Set<String> columns) {
        if (expr == null) {
            return;
        }

        if (expr instanceof ColumnReference) {
            ColumnReference colRef = (ColumnReference) expr;
            columns.add(colRef.columnName());
        } else if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            collectReferencedColumns(binExpr.left(), columns);
            collectReferencedColumns(binExpr.right(), columns);
        } else if (expr instanceof AggregateExpression) {
            AggregateExpression aggExpr = (AggregateExpression) expr;
            if (aggExpr.argument() != null) {
                collectReferencedColumns(aggExpr.argument(), columns);
            }
        }
        // Add more expression types as needed (UnaryExpression, FunctionCall, etc.)
    }

    /**
     * Gets all column names available in a plan's output schema.
     *
     * @param plan the plan node
     * @return set of available column names
     */
    private Set<String> getAvailableColumns(LogicalPlan plan) {
        if (plan == null) {
            return Collections.emptySet();
        }

        try {
            return plan.schema().fields().stream()
                .map(StructField::name)
                .collect(Collectors.toSet());
        } catch (Exception e) {
            // If schema inference fails, return empty set
            return Collections.emptySet();
        }
    }

    /**
     * Extracts grouping column names from an Aggregate node.
     *
     * @param aggregate the aggregate node
     * @return set of grouping column names
     */
    private Set<String> getGroupingColumns(Aggregate aggregate) {
        Set<String> groupingCols = new HashSet<>();

        for (Expression groupExpr : aggregate.groupingExpressions()) {
            collectReferencedColumns(groupExpr, groupingCols);
        }

        return groupingCols;
    }

    /**
     * Splits an AND expression into its individual conjuncts.
     *
     * <p>Example:
     * <pre>
     * (a > 5 AND b < 10 AND c = 'x') → [a > 5, b < 10, c = 'x']
     * </pre>
     *
     * @param expr the expression to split
     * @return list of conjuncts
     */
    private List<Expression> splitConjunction(Expression expr) {
        List<Expression> conjuncts = new ArrayList<>();
        splitConjunctionHelper(expr, conjuncts);
        return conjuncts;
    }

    /**
     * Helper method to recursively split AND expressions.
     *
     * @param expr the expression
     * @param conjuncts the list to accumulate conjuncts
     */
    private void splitConjunctionHelper(Expression expr, List<Expression> conjuncts) {
        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            if (binExpr.operator() == BinaryExpression.Operator.AND) {
                // Recursively split left and right
                splitConjunctionHelper(binExpr.left(), conjuncts);
                splitConjunctionHelper(binExpr.right(), conjuncts);
                return;
            }
        }

        // Not an AND expression, treat as single conjunct
        conjuncts.add(expr);
    }

    /**
     * Combines a list of expressions with AND.
     *
     * <p>Example:
     * <pre>
     * [a > 5, b < 10, c = 'x'] → (a > 5 AND b < 10 AND c = 'x')
     * </pre>
     *
     * @param expressions the expressions to combine
     * @return the combined expression, or null if list is empty
     */
    private Expression combineWithAnd(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }

        if (expressions.size() == 1) {
            return expressions.get(0);
        }

        // Build left-associative AND tree: ((e1 AND e2) AND e3) AND e4
        Expression result = expressions.get(0);
        for (int i = 1; i < expressions.size(); i++) {
            result = BinaryExpression.and(result, expressions.get(i));
        }

        return result;
    }
}
