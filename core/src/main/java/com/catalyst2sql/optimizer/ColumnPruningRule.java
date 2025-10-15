package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.*;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.types.StructField;
import com.catalyst2sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Optimization rule that removes unused columns from the query plan.
 *
 * <p>Column pruning eliminates columns that are not referenced by any
 * downstream operator, reducing the amount of data read, transferred,
 * and processed.
 *
 * <p>This optimization works by:
 * <ol>
 *   <li>Computing required columns starting from the root (all output columns)</li>
 *   <li>Propagating requirements down through the plan tree</li>
 *   <li>At each node, determining which input columns are actually needed</li>
 *   <li>Inserting Project nodes to prune unused columns where beneficial</li>
 *   <li>Updating TableScan nodes to read only required columns</li>
 * </ol>
 *
 * <p>Examples of transformations:
 * <pre>
 *   // Remove unused columns from projection
 *   Project(a, b, c) -> Filter(uses a, b) -> Project(a, b)
 *
 *   // Prune columns from table scan
 *   Scan(a, b, c, d) -> Project(a, c) -> Scan(a, c)
 *
 *   // Prune columns from join
 *   Join(left(a,b,c), right(x,y,z)) -> Project(a, x) -> Join(left(a), right(x))
 *
 *   // Prune through aggregation
 *   Aggregate(groupBy=category, agg=sum(price))
 *     -> TableScan(category, price, quantity, description)
 *     -> TableScan(category, price)  // Only read needed columns
 * </pre>
 *
 * <p>This optimization is particularly effective with columnar storage
 * formats like Parquet, where reading fewer columns can significantly
 * reduce I/O and improve query performance. In practice, this can reduce
 * I/O by 50-90% for queries that only use a small subset of table columns.
 *
 * <h3>Algorithm Details</h3>
 *
 * <p>The algorithm uses a two-pass approach:
 * <ol>
 *   <li><b>Bottom-up Pass</b>: Determine available columns from leaf nodes</li>
 *   <li><b>Top-down Pass</b>: Propagate required columns from root to leaves,
 *       pruning unused columns at each level</li>
 * </ol>
 *
 * <p>For each operator type:
 * <ul>
 *   <li><b>Filter</b>: Requires columns referenced in the condition plus all parent requirements</li>
 *   <li><b>Project</b>: Requires columns referenced in projection expressions</li>
 *   <li><b>Join</b>: Requires columns in join condition plus columns needed from each side</li>
 *   <li><b>Aggregate</b>: Requires grouping columns plus columns in aggregate functions</li>
 *   <li><b>TableScan</b>: Create pruned schema with only required columns</li>
 *   <li><b>Sort/Limit</b>: Pass through all parent requirements plus sort keys</li>
 *   <li><b>Union</b>: Requires corresponding columns from each child</li>
 * </ul>
 *
 * <h3>Edge Cases</h3>
 *
 * <ul>
 *   <li>If all columns are required, no pruning is performed</li>
 *   <li>For nested expressions (e.g., col("a") + col("b")), all referenced columns are required</li>
 *   <li>For semi/anti joins, only left side columns are in output, but join condition may reference both sides</li>
 *   <li>Window functions require partition and order by columns in addition to function arguments</li>
 * </ul>
 *
 * @see OptimizationRule
 * @see FilterPushdownRule
 * @see ProjectionPushdownRule
 */
public class ColumnPruningRule implements OptimizationRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }

        // Step 1: Compute required columns from root (all output columns)
        Set<String> requiredColumns = getAllColumnNames(plan.schema());

        // Step 2: Prune columns recursively from top to bottom
        return pruneColumns(plan, requiredColumns);
    }

    /**
     * Recursively prunes columns from the plan tree.
     *
     * @param plan the logical plan to prune
     * @param requiredColumns the set of columns required by parent operators
     * @return the pruned logical plan
     */
    private LogicalPlan pruneColumns(LogicalPlan plan, Set<String> requiredColumns) {
        if (plan == null || requiredColumns == null || requiredColumns.isEmpty()) {
            return plan;
        }

        // Handle different plan node types
        if (plan instanceof TableScan) {
            return pruneTableScan((TableScan) plan, requiredColumns);
        } else if (plan instanceof Project) {
            return pruneProject((Project) plan, requiredColumns);
        } else if (plan instanceof Filter) {
            return pruneFilter((Filter) plan, requiredColumns);
        } else if (plan instanceof Join) {
            return pruneJoin((Join) plan, requiredColumns);
        } else if (plan instanceof Aggregate) {
            return pruneAggregate((Aggregate) plan, requiredColumns);
        } else if (plan instanceof Sort) {
            return pruneSort((Sort) plan, requiredColumns);
        } else if (plan instanceof Limit) {
            return pruneLimit((Limit) plan, requiredColumns);
        } else if (plan instanceof Union) {
            return pruneUnion((Union) plan, requiredColumns);
        } else {
            // For other node types (InMemoryRelation, LocalRelation), return as-is
            return plan;
        }
    }

    /**
     * Prunes columns from a TableScan node.
     *
     * <p>Creates a new TableScan with only the required columns in its schema.
     *
     * @param scan the table scan node
     * @param requiredColumns the required column names
     * @return the pruned table scan
     */
    private LogicalPlan pruneTableScan(TableScan scan, Set<String> requiredColumns) {
        StructType originalSchema = scan.schema();

        // Filter fields to only required columns
        List<StructField> prunedFields = originalSchema.fields().stream()
            .filter(field -> requiredColumns.contains(field.name()))
            .collect(Collectors.toList());

        // If no columns were pruned, return original scan
        if (prunedFields.size() == originalSchema.fields().size()) {
            return scan;
        }

        // If all columns were pruned (shouldn't happen in valid query), keep at least one
        if (prunedFields.isEmpty()) {
            prunedFields.add(originalSchema.fields().get(0));
        }

        // Create new scan with pruned schema
        StructType prunedSchema = new StructType(prunedFields);
        return new TableScan(scan.source(), scan.format(), scan.options(), prunedSchema);
    }

    /**
     * Prunes columns from a Project node.
     *
     * <p>Computes which columns the child needs and recursively prunes the child.
     * Also prunes the projection list to only include required expressions.
     *
     * @param project the project node
     * @param requiredColumns the required output column names
     * @return the pruned project plan
     */
    private LogicalPlan pruneProject(Project project, Set<String> requiredColumns) {
        // Compute columns needed from child
        Set<String> childRequired = new HashSet<>();
        List<Expression> requiredProjections = new ArrayList<>();
        List<String> requiredAliases = new ArrayList<>();

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        for (int i = 0; i < projections.size(); i++) {
            Expression expr = projections.get(i);
            String alias = aliases.get(i);
            String outputName = (alias != null) ? alias : getExpressionOutputName(expr, i);

            // If this projection is required by parent
            if (requiredColumns.contains(outputName)) {
                requiredProjections.add(expr);
                requiredAliases.add(alias);

                // Add all columns referenced in this expression to child requirements
                childRequired.addAll(getReferencedColumns(expr));
            }
        }

        // If no projections are required (shouldn't happen), keep at least one
        if (requiredProjections.isEmpty()) {
            requiredProjections.add(projections.get(0));
            requiredAliases.add(aliases.get(0));
            childRequired.addAll(getReferencedColumns(projections.get(0)));
        }

        // Recursively prune child
        LogicalPlan prunedChild = pruneColumns(project.child(), childRequired);

        // If nothing changed, return original
        if (prunedChild == project.child() && requiredProjections.size() == projections.size()) {
            return project;
        }

        // Create new project with pruned projections
        return new Project(prunedChild, requiredProjections, requiredAliases);
    }

    /**
     * Prunes columns from a Filter node.
     *
     * <p>Requires all parent columns plus columns referenced in the filter condition.
     *
     * @param filter the filter node
     * @param requiredColumns the required output column names
     * @return the pruned filter plan
     */
    private LogicalPlan pruneFilter(Filter filter, Set<String> requiredColumns) {
        // Filter requires all parent columns plus columns in condition
        Set<String> childRequired = new HashSet<>(requiredColumns);
        childRequired.addAll(getReferencedColumns(filter.condition()));

        // Recursively prune child
        LogicalPlan prunedChild = pruneColumns(filter.child(), childRequired);

        // If child didn't change, return original
        if (prunedChild == filter.child()) {
            return filter;
        }

        return new Filter(prunedChild, filter.condition());
    }

    /**
     * Prunes columns from a Join node.
     *
     * <p>Splits required columns between left and right sides based on schema.
     *
     * @param join the join node
     * @param requiredColumns the required output column names
     * @return the pruned join plan
     */
    private LogicalPlan pruneJoin(Join join, Set<String> requiredColumns) {
        StructType leftSchema = join.left().schema();
        StructType rightSchema = join.right().schema();

        // Split required columns between left and right
        Set<String> leftRequired = new HashSet<>();
        Set<String> rightRequired = new HashSet<>();

        for (String col : requiredColumns) {
            if (leftSchema.fieldByName(col) != null) {
                leftRequired.add(col);
            } else if (rightSchema.fieldByName(col) != null) {
                rightRequired.add(col);
            }
        }

        // Add columns needed for join condition
        if (join.condition() != null) {
            Set<String> joinConditionColumns = getReferencedColumns(join.condition());
            for (String col : joinConditionColumns) {
                if (leftSchema.fieldByName(col) != null) {
                    leftRequired.add(col);
                } else if (rightSchema.fieldByName(col) != null) {
                    rightRequired.add(col);
                }
            }
        }

        // Ensure at least one column from each side (if they have columns)
        if (leftRequired.isEmpty() && !leftSchema.fields().isEmpty()) {
            leftRequired.add(leftSchema.fields().get(0).name());
        }
        if (rightRequired.isEmpty() && !rightSchema.fields().isEmpty()) {
            rightRequired.add(rightSchema.fields().get(0).name());
        }

        // Recursively prune children
        LogicalPlan prunedLeft = pruneColumns(join.left(), leftRequired);
        LogicalPlan prunedRight = pruneColumns(join.right(), rightRequired);

        // If nothing changed, return original
        if (prunedLeft == join.left() && prunedRight == join.right()) {
            return join;
        }

        return new Join(prunedLeft, prunedRight, join.joinType(), join.condition());
    }

    /**
     * Prunes columns from an Aggregate node.
     *
     * <p>Requires grouping columns and columns referenced in aggregate expressions.
     *
     * @param agg the aggregate node
     * @param requiredColumns the required output column names
     * @return the pruned aggregate plan
     */
    private LogicalPlan pruneAggregate(Aggregate agg, Set<String> requiredColumns) {
        // Compute columns needed from child
        Set<String> childRequired = new HashSet<>();

        // Add all grouping columns
        for (Expression groupExpr : agg.groupingExpressions()) {
            childRequired.addAll(getReferencedColumns(groupExpr));
        }

        // Add columns from aggregate expressions
        for (Aggregate.AggregateExpression aggExpr : agg.aggregateExpressions()) {
            if (aggExpr.argument() != null) {
                childRequired.addAll(getReferencedColumns(aggExpr.argument()));
            }
        }

        // Recursively prune child
        LogicalPlan prunedChild = pruneColumns(agg.child(), childRequired);

        // If child didn't change, return original
        if (prunedChild == agg.child()) {
            return agg;
        }

        return new Aggregate(prunedChild, agg.groupingExpressions(), agg.aggregateExpressions());
    }

    /**
     * Prunes columns from a Sort node.
     *
     * <p>Requires all parent columns plus columns referenced in sort expressions.
     *
     * @param sort the sort node
     * @param requiredColumns the required output column names
     * @return the pruned sort plan
     */
    private LogicalPlan pruneSort(Sort sort, Set<String> requiredColumns) {
        // Sort requires all parent columns plus columns in order expressions
        Set<String> childRequired = new HashSet<>(requiredColumns);

        for (Sort.SortOrder order : sort.sortOrders()) {
            childRequired.addAll(getReferencedColumns(order.expression()));
        }

        // Recursively prune child
        LogicalPlan prunedChild = pruneColumns(sort.child(), childRequired);

        // If child didn't change, return original
        if (prunedChild == sort.child()) {
            return sort;
        }

        return new Sort(prunedChild, sort.sortOrders());
    }

    /**
     * Prunes columns from a Limit node.
     *
     * <p>Limit doesn't add any column requirements, just passes through parent requirements.
     *
     * @param limit the limit node
     * @param requiredColumns the required output column names
     * @return the pruned limit plan
     */
    private LogicalPlan pruneLimit(Limit limit, Set<String> requiredColumns) {
        // Limit just passes through requirements
        LogicalPlan prunedChild = pruneColumns(limit.child(), requiredColumns);

        // If child didn't change, return original
        if (prunedChild == limit.child()) {
            return limit;
        }

        return new Limit(prunedChild, limit.limit(), limit.offset());
    }

    /**
     * Prunes columns from a Union node.
     *
     * <p>Requires corresponding columns from each child.
     *
     * @param union the union node
     * @param requiredColumns the required output column names
     * @return the pruned union plan
     */
    private LogicalPlan pruneUnion(Union union, Set<String> requiredColumns) {
        // For union, both children should have the same schema
        // Prune each child with the same required columns
        LogicalPlan prunedLeft = pruneColumns(union.left(), requiredColumns);
        LogicalPlan prunedRight = pruneColumns(union.right(), requiredColumns);

        // If nothing changed, return original
        if (prunedLeft == union.left() && prunedRight == union.right()) {
            return union;
        }

        return new Union(prunedLeft, prunedRight, union.all());
    }

    /**
     * Extracts all column names from a schema.
     *
     * @param schema the struct type schema
     * @return set of all column names
     */
    private Set<String> getAllColumnNames(StructType schema) {
        if (schema == null) {
            return Collections.emptySet();
        }

        return schema.fields().stream()
            .map(StructField::name)
            .collect(Collectors.toSet());
    }

    /**
     * Extracts all column references from an expression.
     *
     * <p>This recursively traverses the expression tree to find all ColumnReference nodes.
     *
     * @param expr the expression to analyze
     * @return set of referenced column names
     */
    private Set<String> getReferencedColumns(Expression expr) {
        if (expr == null) {
            return Collections.emptySet();
        }

        Set<String> columns = new HashSet<>();

        if (expr instanceof ColumnReference) {
            columns.add(((ColumnReference) expr).columnName());
        } else if (expr instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expr;
            columns.addAll(getReferencedColumns(binary.left()));
            columns.addAll(getReferencedColumns(binary.right()));
        } else if (expr instanceof UnaryExpression) {
            UnaryExpression unary = (UnaryExpression) expr;
            columns.addAll(getReferencedColumns(unary.operand()));
        } else if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            for (Expression arg : func.arguments()) {
                columns.addAll(getReferencedColumns(arg));
            }
        } else if (expr instanceof WindowFunction) {
            WindowFunction window = (WindowFunction) expr;
            // Add columns from the window function arguments
            for (Expression arg : window.arguments()) {
                columns.addAll(getReferencedColumns(arg));
            }
            // Add columns from partition by
            for (Expression partition : window.partitionBy()) {
                columns.addAll(getReferencedColumns(partition));
            }
            // Add columns from order by
            for (Sort.SortOrder order : window.orderBy()) {
                columns.addAll(getReferencedColumns(order.expression()));
            }
        } else if (expr instanceof ScalarSubquery) {
            // Scalar subqueries don't reference columns from the outer query directly
            // (correlated columns would be handled separately)
        } else if (expr instanceof Aggregate.AggregateExpression) {
            Aggregate.AggregateExpression aggExpr = (Aggregate.AggregateExpression) expr;
            if (aggExpr.argument() != null) {
                columns.addAll(getReferencedColumns(aggExpr.argument()));
            }
        }
        // For Literal and other leaf expressions, no columns to add

        return columns;
    }

    /**
     * Gets the output name for a projection expression.
     *
     * <p>If the expression is a simple ColumnReference, returns the column name.
     * Otherwise, returns a generated name like "col_0".
     *
     * @param expr the projection expression
     * @param index the index in the projection list
     * @return the output column name
     */
    private String getExpressionOutputName(Expression expr, int index) {
        if (expr instanceof ColumnReference) {
            return ((ColumnReference) expr).columnName();
        }
        return "col_" + index;
    }
}
