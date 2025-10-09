package org.apache.spark.sql;

import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.plan.nodes.Aggregate;
import com.spark2sql.plan.Expression;
import com.spark2sql.plan.expressions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set of methods for aggregations on a DataFrame, created by groupBy.
 * Simplified version for Phase 1.
 */
public class RelationalGroupedDataset {
    private final SparkSession sparkSession;
    private final LogicalPlan logicalPlan;
    private final List<?> groupingExpressions;

    RelationalGroupedDataset(SparkSession session, LogicalPlan plan, List<?> groupingExprs) {
        this.sparkSession = session;
        this.logicalPlan = plan;
        this.groupingExpressions = groupingExprs;
    }

    /**
     * Compute aggregates by specifying the column names and aggregate methods.
     */
    public Dataset<Row> agg(Column... exprs) {
        List<Column> aggExprs = Arrays.asList(exprs);
        LogicalPlan aggregatePlan = new Aggregate(logicalPlan, groupingExpressions, aggExprs);
        return new Dataset<>(sparkSession, aggregatePlan, RowEncoder.apply(aggregatePlan.schema()));
    }

    /**
     * Count the number of rows for each group.
     */
    public Dataset<Row> count() {
        return agg(functions.count("*").as("count"));
    }

    /**
     * Compute the sum for each group.
     */
    public Dataset<Row> sum(String... colNames) {
        Column[] sumExprs = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            sumExprs[i] = functions.sum(colNames[i]);
        }
        return agg(sumExprs);
    }

    /**
     * Compute the average for each group.
     */
    public Dataset<Row> avg(String... colNames) {
        Column[] avgExprs = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            avgExprs[i] = functions.avg(colNames[i]);
        }
        return agg(avgExprs);
    }

    /**
     * Compute the mean for each group.
     */
    public Dataset<Row> mean(String... colNames) {
        return avg(colNames);
    }

    /**
     * Compute the max for each group.
     */
    public Dataset<Row> max(String... colNames) {
        Column[] maxExprs = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            maxExprs[i] = functions.max(colNames[i]);
        }
        return agg(maxExprs);
    }

    /**
     * Compute the min for each group.
     */
    public Dataset<Row> min(String... colNames) {
        Column[] minExprs = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            minExprs[i] = functions.min(colNames[i]);
        }
        return agg(minExprs);
    }
}