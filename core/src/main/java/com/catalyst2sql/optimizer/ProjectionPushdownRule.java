package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.FunctionCall;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.expression.UnaryExpression;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.types.StructField;
import com.catalyst2sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Optimization rule that pushes projections down into table scans for column pruning.
 *
 * <p>When using columnar storage formats (Parquet, Delta, Iceberg), reading
 * only the required columns can significantly reduce I/O. This rule identifies
 * projection operations on table scans and pushes the column selection down to
 * the scan level, allowing the storage layer to skip reading unnecessary columns.
 *
 * <h2>Transformation Examples</h2>
 *
 * <h3>Push Simple Projection into Scan</h3>
 * <pre>
 * Before: Project([a, b], TableScan(a, b, c, d))
 * After:  TableScan(a, b)
 * </pre>
 *
 * <h3>Push Projection with Aliases</h3>
 * <pre>
 * Before: Project([a AS x, b AS y], TableScan(a, b, c, d))
 * After:  Project([a AS x, b AS y], TableScan(a, b))
 * </pre>
 *
 * <h3>Cannot Push Computed Projections</h3>
 * <pre>
 * Before: Project([a + b AS sum], TableScan(a, b, c, d))
 * After:  Project([a + b AS sum], TableScan(a, b, c, d))  // Cannot push
 *         Note: ColumnPruningRule will handle this case
 * </pre>
 *
 * <h3>Combine Multiple Projections</h3>
 * <pre>
 * Before: Project([a], Project([a, b], TableScan(a, b, c, d)))
 * After:  TableScan(a)
 * </pre>
 *
 * <h2>Pushdown Conditions</h2>
 *
 * <p>A projection can be pushed into a table scan if:
 * <ul>
 *   <li>The child is a TableScan node</li>
 *   <li>All projection expressions are simple ColumnReferences (not computed)</li>
 *   <li>No aliases are present (or handled separately by keeping the projection)</li>
 * </ul>
 *
 * <p>If a projection contains computed expressions (e.g., a + b, upper(name)),
 * the pushdown is not performed. The ColumnPruningRule will still optimize
 * these cases by pruning the scan to only read columns used in the computation.
 *
 * <h2>Performance Impact</h2>
 *
 * <p>For columnar storage formats like Parquet:
 * <ul>
 *   <li><b>5-10x faster</b> scans when selecting 2-3 columns from a 20+ column table</li>
 *   <li><b>50-90% I/O reduction</b> by skipping unused column data</li>
 *   <li><b>Reduced memory usage</b> by not loading unnecessary columns into memory</li>
 * </ul>
 *
 * <p>This optimization works in conjunction with:
 * <ul>
 *   <li>{@link ColumnPruningRule} - Prunes columns throughout the plan tree</li>
 *   <li>{@link FilterPushdownRule} - Pushes filters closer to table scans</li>
 * </ul>
 *
 * @see OptimizationRule
 * @see ColumnPruningRule
 * @see FilterPushdownRule
 */
public class ProjectionPushdownRule implements OptimizationRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }

        // Apply recursively to children first (bottom-up traversal)
        LogicalPlan transformed = applyToChildren(plan);

        // If this is a Project node, try to push it down
        if (transformed instanceof Project) {
            Project project = (Project) transformed;
            LogicalPlan child = project.child();

            // Case 1: Push into TableScan
            if (child instanceof TableScan) {
                return pushIntoTableScan(project, (TableScan) child);
            }

            // Case 2: Combine consecutive Projects
            if (child instanceof Project) {
                return combineProjects(project, (Project) child);
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
     * Pushes a projection into a TableScan node.
     *
     * <p>This is the main optimization: when a Project directly operates on a
     * TableScan, we can create a new TableScan that only reads the projected columns.
     *
     * <p>Strategy:
     * <ol>
     *   <li>Check if all projections are simple column references (no computed expressions)</li>
     *   <li>If yes and no aliases, replace with pruned TableScan</li>
     *   <li>If yes but has aliases, keep Project over pruned TableScan</li>
     *   <li>If no (has computed expressions), return unchanged (ColumnPruningRule handles it)</li>
     * </ol>
     *
     * <p>Example (no aliases):
     * <pre>
     * Project([a, b], TableScan(a, b, c, d))
     * → TableScan(a, b)
     * </pre>
     *
     * <p>Example (with aliases):
     * <pre>
     * Project([a AS x, b AS y], TableScan(a, b, c, d))
     * → Project([a AS x, b AS y], TableScan(a, b))
     * </pre>
     *
     * <p>Example (computed - no pushdown):
     * <pre>
     * Project([a + b AS sum], TableScan(a, b, c, d))
     * → Project([a + b AS sum], TableScan(a, b, c, d))  // Unchanged
     * </pre>
     *
     * @param project the project node
     * @param tableScan the table scan node
     * @return the optimized plan
     */
    private LogicalPlan pushIntoTableScan(Project project, TableScan tableScan) {
        // Step 1: Check if all projections are simple column references
        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        ProjectionAnalysis analysis = analyzeProjections(projections);

        if (!analysis.allSimpleColumns) {
            // Contains computed expressions, cannot push down
            // ColumnPruningRule will optimize this case instead
            return project;
        }

        // Step 2: Extract column names from the simple column references
        List<String> projectedColumns = analysis.columnNames;

        // Step 3: Create pruned schema for TableScan
        StructType originalSchema = tableScan.schema();
        StructType prunedSchema = createPrunedSchema(originalSchema, projectedColumns);

        // If no columns were pruned, return original (optimization not beneficial)
        if (prunedSchema.fields().size() == originalSchema.fields().size()) {
            return project;
        }

        // Step 4: Create new TableScan with pruned schema
        TableScan prunedScan = new TableScan(
            tableScan.source(),
            tableScan.format(),
            tableScan.options(),
            prunedSchema
        );

        // Step 5: Decide whether to keep the Project node
        if (hasAnyAliases(aliases)) {
            // Keep Project for aliases: Project(aliases, PrunedTableScan)
            return new Project(prunedScan, projections, aliases);
        } else {
            // No aliases, can eliminate Project entirely: PrunedTableScan
            return prunedScan;
        }
    }

    /**
     * Combines two consecutive Project nodes into one.
     *
     * <p>When we have Project(outer) → Project(inner) → child, we can combine
     * them into a single Project that directly projects from the child.
     *
     * <p>Example:
     * <pre>
     * Project([a], Project([a, b], TableScan(a, b, c, d)))
     * → Project([a], TableScan(a, b, c, d))
     * </pre>
     *
     * <p>This creates more opportunities for projection pushdown on the next
     * iteration of the optimizer.
     *
     * @param outerProject the outer project node
     * @param innerProject the inner project node
     * @return the combined project plan
     */
    private LogicalPlan combineProjects(Project outerProject, Project innerProject) {
        // For simplicity, we only combine if the outer project references
        // columns from the inner project without further computation.
        // A full implementation would substitute inner projections into outer expressions.

        List<Expression> outerProjections = outerProject.projections();
        List<String> outerAliases = outerProject.aliases();

        ProjectionAnalysis analysis = analyzeProjections(outerProjections);

        if (!analysis.allSimpleColumns) {
            // Outer project has computed expressions, cannot combine simply
            return outerProject;
        }

        // Get the columns that outer project needs from inner project
        Set<String> requiredColumns = new HashSet<>(analysis.columnNames);

        // Build new projection list from inner project
        List<Expression> innerProjections = innerProject.projections();
        List<String> innerAliases = innerProject.aliases();

        List<Expression> combinedProjections = new ArrayList<>();
        List<String> combinedAliases = new ArrayList<>();

        for (int i = 0; i < innerProjections.size(); i++) {
            String innerAlias = innerAliases.get(i);
            String outputName = (innerAlias != null) ? innerAlias : getColumnName(innerProjections.get(i));

            if (requiredColumns.contains(outputName)) {
                combinedProjections.add(innerProjections.get(i));
                // Use outer alias if present, otherwise keep inner alias
                String outerAlias = findAlias(outerAliases, outerProjections, outputName);
                combinedAliases.add((outerAlias != null) ? outerAlias : innerAlias);
            }
        }

        // If no projections remain, keep at least one
        if (combinedProjections.isEmpty()) {
            combinedProjections.add(innerProjections.get(0));
            combinedAliases.add(innerAliases.get(0));
        }

        // Create combined project directly on inner's child
        return new Project(innerProject.child(), combinedProjections, combinedAliases);
    }

    // ==================== Helper Methods ====================

    /**
     * Container for projection analysis results.
     */
    private static class ProjectionAnalysis {
        final boolean allSimpleColumns;
        final List<String> columnNames;

        ProjectionAnalysis(boolean allSimpleColumns, List<String> columnNames) {
            this.allSimpleColumns = allSimpleColumns;
            this.columnNames = columnNames;
        }
    }

    /**
     * Analyzes projection expressions to determine if they are all simple column references.
     *
     * @param projections the projection expressions
     * @return the analysis result
     */
    private ProjectionAnalysis analyzeProjections(List<Expression> projections) {
        List<String> columnNames = new ArrayList<>();
        boolean allSimpleColumns = true;

        for (Expression expr : projections) {
            if (expr instanceof ColumnReference) {
                columnNames.add(((ColumnReference) expr).columnName());
            } else {
                // Not a simple column reference (computed expression)
                allSimpleColumns = false;
            }
        }

        return new ProjectionAnalysis(allSimpleColumns, columnNames);
    }

    /**
     * Creates a pruned schema containing only the specified columns.
     *
     * <p>The pruned schema maintains the original column order from the table.
     *
     * @param originalSchema the original table schema
     * @param projectedColumns the columns to include in the pruned schema
     * @return the pruned schema
     */
    private StructType createPrunedSchema(StructType originalSchema, List<String> projectedColumns) {
        if (originalSchema == null || projectedColumns == null || projectedColumns.isEmpty()) {
            return originalSchema;
        }

        Set<String> requiredColumns = new HashSet<>(projectedColumns);
        List<StructField> prunedFields = new ArrayList<>();

        // Maintain original column order
        for (StructField field : originalSchema.fields()) {
            if (requiredColumns.contains(field.name())) {
                prunedFields.add(field);
            }
        }

        // If no fields matched (shouldn't happen in valid query), keep at least one
        if (prunedFields.isEmpty()) {
            prunedFields.add(originalSchema.fields().get(0));
        }

        return new StructType(prunedFields);
    }

    /**
     * Checks if any of the aliases are non-null and non-empty.
     *
     * @param aliases the alias list
     * @return true if any alias is present
     */
    private boolean hasAnyAliases(List<String> aliases) {
        if (aliases == null) {
            return false;
        }

        for (String alias : aliases) {
            if (alias != null && !alias.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Gets the column name from a simple ColumnReference expression.
     *
     * <p>If the expression is not a ColumnReference, returns a generated name.
     *
     * @param expr the expression
     * @return the column name
     */
    private String getColumnName(Expression expr) {
        if (expr instanceof ColumnReference) {
            return ((ColumnReference) expr).columnName();
        }
        return "col";
    }

    /**
     * Finds the alias for a column in the outer projection.
     *
     * @param aliases the alias list
     * @param projections the projection expressions
     * @param columnName the column name to find
     * @return the alias, or null if not found
     */
    private String findAlias(List<String> aliases, List<Expression> projections, String columnName) {
        for (int i = 0; i < projections.size(); i++) {
            Expression expr = projections.get(i);
            if (expr instanceof ColumnReference) {
                ColumnReference colRef = (ColumnReference) expr;
                if (colRef.columnName().equals(columnName)) {
                    return aliases.get(i);
                }
            }
        }
        return null;
    }
}
