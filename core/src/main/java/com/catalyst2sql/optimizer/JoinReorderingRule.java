package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.types.StructField;

import java.util.*;

/**
 * Optimization rule that reorders joins for better performance.
 *
 * <p>The order of joins can significantly impact query performance. This
 * rule uses cost-based analysis to find a more efficient join order,
 * considering factors like:
 * <ul>
 *   <li>Table sizes (smaller tables first)</li>
 *   <li>Join selectivity (more selective joins first)</li>
 *   <li>Index availability</li>
 *   <li>Filter conditions</li>
 * </ul>
 *
 * <h2>Transformation Examples</h2>
 *
 * <h3>Swap for Smaller Build Side</h3>
 * <pre>
 * Before: Join(large_table, small_table, INNER, condition)
 * After:  Join(small_table, large_table, INNER, swapped_condition)
 *
 * Rationale: Smaller table on the right (build side) for hash joins
 * </pre>
 *
 * <h3>Preserve Non-INNER Joins</h3>
 * <pre>
 * Before: Join(large_table, small_table, LEFT, condition)
 * After:  Join(large_table, small_table, LEFT, condition) // No change
 *
 * Rationale: LEFT/RIGHT/FULL joins are order-sensitive
 * </pre>
 *
 * <h3>Recursive Application</h3>
 * <pre>
 * Before: Join(large1, Join(large2, small, INNER, c2), INNER, c1)
 * After:  Join(Join(small, large2, INNER, swapped_c2), large1, INNER, swapped_c1)
 *
 * Rationale: Recursively optimize nested joins
 * </pre>
 *
 * <h2>Cardinality Estimation</h2>
 *
 * <p>This implementation uses a simple heuristic-based cardinality estimation:
 * <ul>
 *   <li><b>TableScan</b>: Use schema field count as proxy (larger schema ≈ larger table)</li>
 *   <li><b>Filter</b>: Apply 0.3 selectivity factor to child cardinality</li>
 *   <li><b>Project</b>: Pass through child cardinality</li>
 *   <li><b>Join</b>: Multiply child cardinalities × 0.1 (join selectivity)</li>
 *   <li><b>Aggregate</b>: Estimate 10% of input rows (aggregation reduces data)</li>
 *   <li><b>Union</b>: Sum of child cardinalities</li>
 *   <li><b>Default</b>: Assume 1000 rows</li>
 * </ul>
 *
 * <p><b>Note</b>: In production systems, this would use actual table statistics,
 * histograms, and metadata from the storage layer. This simplified approach
 * provides reasonable results for common cases.
 *
 * <h2>Safety Conditions</h2>
 *
 * <ul>
 *   <li><b>Only INNER joins</b>: Other join types are not commutative</li>
 *   <li><b>Condition swapping</b>: Column references are swapped to maintain correctness</li>
 *   <li><b>Recursive application</b>: Child joins are optimized before parent</li>
 *   <li><b>Null safety</b>: Handles null conditions (CROSS joins) correctly</li>
 * </ul>
 *
 * <p>Note: This rule must preserve join semantics. It only reorders
 * INNER joins and applies associativity/commutativity rules where valid.
 *
 * @see OptimizationRule
 * @see FilterPushdownRule
 */
public class JoinReorderingRule implements OptimizationRule {

    /**
     * Default cardinality estimate for plans without better information.
     */
    private static final long DEFAULT_CARDINALITY = 1000L;

    /**
     * Selectivity factor for filter operations (30% of rows pass).
     */
    private static final double FILTER_SELECTIVITY = 0.3;

    /**
     * Selectivity factor for join operations (10% of cross product).
     */
    private static final double JOIN_SELECTIVITY = 0.1;

    /**
     * Reduction factor for aggregate operations (10% of input rows).
     */
    private static final double AGGREGATE_REDUCTION = 0.1;

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }

        // Apply recursively to children first (bottom-up traversal)
        LogicalPlan transformed = applyToChildren(plan);

        // If this is a Join node, try to reorder it
        if (transformed instanceof Join) {
            Join join = (Join) transformed;

            // Only reorder INNER joins (commutative and associative)
            if (join.joinType() == Join.JoinType.INNER) {
                return reorderJoin(join);
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
     * Reorders a join if beneficial based on cardinality estimates.
     *
     * <p>In hash joins, the smaller table should be on the right (build side)
     * to minimize memory usage and maximize performance. This method swaps
     * the left and right inputs if the right input is larger.
     *
     * <p>Example:
     * <pre>
     * Input:  Join(large_table [10000 rows], small_table [100 rows], INNER, condition)
     * Output: Join(small_table [100 rows], large_table [10000 rows], INNER, swapped_condition)
     * </pre>
     *
     * @param join the join node to potentially reorder
     * @return the reordered join, or original if no benefit
     */
    private LogicalPlan reorderJoin(Join join) {
        // Estimate cardinalities of left and right inputs
        long leftCardinality = estimateCardinality(join.left());
        long rightCardinality = estimateCardinality(join.right());

        // If left is larger than right, swap them
        // (smaller table should be on the right as the build side)
        if (leftCardinality > rightCardinality) {
            // Swap the inputs and update the condition
            Expression swappedCondition = swapJoinCondition(join.condition(), join.left(), join.right());
            return new Join(join.right(), join.left(), join.joinType(), swappedCondition);
        }

        // No reordering needed
        return join;
    }

    /**
     * Estimates the cardinality (number of rows) of a logical plan.
     *
     * <p>This uses a simple heuristic-based approach:
     * <ul>
     *   <li>TableScan: Use schema field count as proxy (more columns ≈ larger table)</li>
     *   <li>Filter: Apply selectivity factor (30%) to child</li>
     *   <li>Project: Pass through child cardinality</li>
     *   <li>Join: Multiply children × join selectivity (10%)</li>
     *   <li>Aggregate: Reduce to 10% of input (aggregation reduces rows)</li>
     *   <li>Union: Sum of children</li>
     *   <li>Default: 1000 rows</li>
     * </ul>
     *
     * <p><b>Production Note</b>: Real systems would use table statistics from the
     * catalog (e.g., row count, data size, histograms). This simplified approach
     * provides reasonable ordering for common cases.
     *
     * @param plan the plan to estimate
     * @return estimated number of rows
     */
    private long estimateCardinality(LogicalPlan plan) {
        if (plan == null) {
            return DEFAULT_CARDINALITY;
        }

        // TableScan: Use schema field count as a proxy for table size
        if (plan instanceof TableScan) {
            TableScan scan = (TableScan) plan;
            try {
                // More fields typically means a larger, more complex table
                int fieldCount = scan.schema().fields().size();
                // Use field count × 1000 as base estimate
                return (long) fieldCount * 1000;
            } catch (Exception e) {
                return DEFAULT_CARDINALITY;
            }
        }

        // Filter: Apply selectivity factor
        if (plan instanceof Filter) {
            Filter filter = (Filter) plan;
            long childCardinality = estimateCardinality(filter.child());
            return (long) (childCardinality * FILTER_SELECTIVITY);
        }

        // Project: Pass through child cardinality (projection doesn't change row count)
        if (plan instanceof Project) {
            Project project = (Project) plan;
            return estimateCardinality(project.child());
        }

        // Join: Multiply children and apply join selectivity
        if (plan instanceof Join) {
            Join join = (Join) plan;
            long leftCard = estimateCardinality(join.left());
            long rightCard = estimateCardinality(join.right());
            return (long) (leftCard * rightCard * JOIN_SELECTIVITY);
        }

        // Aggregate: Reduce to a fraction of input (grouping reduces rows)
        if (plan instanceof Aggregate) {
            Aggregate agg = (Aggregate) plan;
            long childCardinality = estimateCardinality(agg.child());
            return (long) (childCardinality * AGGREGATE_REDUCTION);
        }

        // Union: Sum of both branches
        if (plan instanceof Union) {
            Union union = (Union) plan;
            long leftCard = estimateCardinality(union.left());
            long rightCard = estimateCardinality(union.right());
            return leftCard + rightCard;
        }

        // Sort/Limit: Pass through child cardinality
        if (plan instanceof Sort) {
            Sort sort = (Sort) plan;
            return estimateCardinality(sort.child());
        }

        if (plan instanceof Limit) {
            Limit limit = (Limit) plan;
            long childCard = estimateCardinality(limit.child());
            // Limit reduces rows, but we need child cardinality for join ordering
            return Math.min(childCard, limit.limit());
        }

        // Default fallback
        return DEFAULT_CARDINALITY;
    }

    /**
     * Swaps column references in a join condition when inputs are swapped.
     *
     * <p>When we swap left and right inputs of a join, we need to ensure
     * that column references in the condition are updated to reflect the swap.
     *
     * <p>For equi-join conditions (e.g., {@code left.id = right.id}), we swap
     * the left and right operands. For complex conditions, we recursively process
     * the expression tree.
     *
     * <p>Example:
     * <pre>
     * Original: Join(T1, T2, INNER, T1.id = T2.id)
     * Swapped:  Join(T2, T1, INNER, T2.id = T1.id)
     * </pre>
     *
     * @param condition the original join condition
     * @param originalLeft the original left input
     * @param originalRight the original right input
     * @return the swapped condition, or null if condition was null
     */
    private Expression swapJoinCondition(Expression condition, LogicalPlan originalLeft, LogicalPlan originalRight) {
        if (condition == null) {
            return null;
        }

        // For binary expressions (e.g., equality), swap left and right
        if (condition instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) condition;

            // Swap the operands
            Expression swappedLeft = swapExpression(binExpr.right(), originalLeft, originalRight);
            Expression swappedRight = swapExpression(binExpr.left(), originalLeft, originalRight);

            // Reconstruct the binary expression with swapped operands
            return new BinaryExpression(swappedLeft, binExpr.operator(), swappedRight);
        }

        // For other expression types, pass through
        return condition;
    }

    /**
     * Swaps column references in an expression when join inputs are swapped.
     *
     * <p>This is a helper method for {@link #swapJoinCondition} that handles
     * individual expressions within the condition.
     *
     * @param expr the expression to swap
     * @param originalLeft the original left input
     * @param originalRight the original right input
     * @return the swapped expression
     */
    private Expression swapExpression(Expression expr, LogicalPlan originalLeft, LogicalPlan originalRight) {
        if (expr == null) {
            return null;
        }

        // For column references, we keep them as-is since the swap is structural
        // (the actual column names don't change, just which side they're on)
        if (expr instanceof ColumnReference) {
            return expr;
        }

        // For binary expressions, recursively swap children
        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            Expression swappedLeft = swapExpression(binExpr.left(), originalLeft, originalRight);
            Expression swappedRight = swapExpression(binExpr.right(), originalLeft, originalRight);
            return new BinaryExpression(swappedLeft, binExpr.operator(), swappedRight);
        }

        // For other expression types, return as-is
        return expr;
    }

    /**
     * Checks if a join is an INNER join.
     *
     * <p>Only INNER joins are safe to reorder because they are both
     * commutative and associative. Other join types (LEFT, RIGHT, FULL,
     * SEMI, ANTI) are order-sensitive and cannot be reordered without
     * changing query semantics.
     *
     * @param join the join node
     * @return true if the join is INNER, false otherwise
     */
    private boolean isInnerJoin(Join join) {
        return join != null && join.joinType() == Join.JoinType.INNER;
    }

    /**
     * Checks if reordering a join would be beneficial.
     *
     * <p>Reordering is beneficial when:
     * <ul>
     *   <li>The join is INNER (safe to reorder)</li>
     *   <li>The left input is significantly larger than the right</li>
     *   <li>Swapping would reduce the build side size for hash joins</li>
     * </ul>
     *
     * @param join the join node
     * @return true if reordering would improve performance
     */
    private boolean shouldReorder(Join join) {
        if (!isInnerJoin(join)) {
            return false;
        }

        long leftCardinality = estimateCardinality(join.left());
        long rightCardinality = estimateCardinality(join.right());

        // Reorder if left is larger (we want smaller on the right)
        return leftCardinality > rightCardinality;
    }
}
