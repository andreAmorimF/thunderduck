package com.spark2sql.plan;

import com.spark2sql.plan.nodes.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Builder for creating logical plans.
 * Provides factory methods for various plan nodes.
 */
public class PlanBuilder {

    /**
     * Create a table scan plan.
     */
    public LogicalPlan tableScan(String tableName) {
        return new TableScan(tableName);
    }

    /**
     * Create a local relation plan from data.
     */
    public LogicalPlan localRelation(List<Row> rows, StructType schema) {
        return new LocalRelation(rows, schema);
    }

    /**
     * Create a filter plan.
     */
    public LogicalPlan filter(LogicalPlan child, Expression condition) {
        return new Filter(child, condition);
    }

    /**
     * Create a project plan.
     */
    public LogicalPlan project(LogicalPlan child, List<?> columns) {
        return new Project(child, columns);
    }

    /**
     * Create a limit plan.
     */
    public LogicalPlan limit(LogicalPlan child, int n) {
        return new Limit(child, n);
    }

    /**
     * Create a distinct plan.
     */
    public LogicalPlan distinct(LogicalPlan child) {
        return new Distinct(child);
    }

    /**
     * Create a sort plan.
     */
    public LogicalPlan sort(LogicalPlan child, List<?> sortExpressions, boolean global) {
        return new Sort(child, sortExpressions, global);
    }

    /**
     * Create a join plan.
     */
    public LogicalPlan join(LogicalPlan left, LogicalPlan right, Expression condition, String joinType) {
        return new Join(left, right, condition, joinType);
    }

    /**
     * Create an aggregate plan.
     */
    public LogicalPlan aggregate(LogicalPlan child, List<?> groupingExpressions, List<?> aggregateExpressions) {
        return new Aggregate(child, groupingExpressions, aggregateExpressions);
    }
}