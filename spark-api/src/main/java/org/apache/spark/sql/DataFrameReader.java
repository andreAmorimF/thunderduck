package org.apache.spark.sql;

import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.plan.PlanBuilder;
import com.spark2sql.plan.nodes.TableScan;

import java.util.HashMap;
import java.util.Map;

/**
 * Interface for reading data from various sources.
 * Simplified version for Phase 1.
 */
public class DataFrameReader {
    private final SparkSession sparkSession;
    private final PlanBuilder planBuilder;
    private final Map<String, String> options = new HashMap<>();
    private String format = "parquet"; // Default format

    DataFrameReader(SparkSession session, PlanBuilder planBuilder) {
        this.sparkSession = session;
        this.planBuilder = planBuilder;
    }

    /**
     * Specifies the input data source format.
     */
    public DataFrameReader format(String source) {
        this.format = source;
        return this;
    }

    /**
     * Adds an input option for the data source.
     */
    public DataFrameReader option(String key, String value) {
        this.options.put(key, value);
        return this;
    }

    /**
     * Adds input options for the data source.
     */
    public DataFrameReader options(Map<String, String> options) {
        this.options.putAll(options);
        return this;
    }

    /**
     * Loads data from a data source and returns it as a DataFrame.
     */
    public Dataset<Row> load(String path) {
        // For now, treat all loads as table scans
        LogicalPlan plan = new TableScan(path);
        return new Dataset<>(sparkSession, plan, RowEncoder.apply(plan.schema()));
    }

    /**
     * Loads a table from the catalog.
     */
    public Dataset<Row> table(String tableName) {
        LogicalPlan plan = new TableScan(tableName);
        return new Dataset<>(sparkSession, plan, RowEncoder.apply(plan.schema()));
    }

    /**
     * Loads a JSON file and returns the result as a DataFrame.
     */
    public Dataset<Row> json(String path) {
        return format("json").load(path);
    }

    /**
     * Loads a Parquet file and returns the result as a DataFrame.
     */
    public Dataset<Row> parquet(String... paths) {
        if (paths.length == 1) {
            return format("parquet").load(paths[0]);
        }
        throw new UnsupportedOperationException("Multiple paths not yet supported in Phase 1");
    }

    /**
     * Loads a CSV file and returns the result as a DataFrame.
     */
    public Dataset<Row> csv(String path) {
        return format("csv").load(path);
    }

    /**
     * Loads an ORC file and returns the result as a DataFrame.
     */
    public Dataset<Row> orc(String path) {
        return format("orc").load(path);
    }

    /**
     * Loads text files and returns the result as a DataFrame.
     */
    public Dataset<Row> text(String path) {
        return format("text").load(path);
    }

    /**
     * Loads text files and returns the result as a DataFrame.
     */
    public Dataset<Row> textFile(String path) {
        return text(path);
    }
}