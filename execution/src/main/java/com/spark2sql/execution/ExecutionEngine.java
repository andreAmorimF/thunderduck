package com.spark2sql.execution;

import com.spark2sql.plan.LogicalPlan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Interface for execution engines that can execute logical plans.
 */
public interface ExecutionEngine extends AutoCloseable {

    /**
     * Execute a logical plan and return results.
     *
     * @param plan The logical plan to execute
     * @param encoder The encoder for result rows
     * @return List of results
     */
    <T> List<T> execute(LogicalPlan plan, Encoder<T> encoder);

    /**
     * Execute a SQL query string directly.
     *
     * @param sql The SQL query to execute
     * @param session The Spark session context
     * @return Dataset containing the results
     */
    Dataset<Row> sql(String sql, SparkSession session);

    /**
     * Create a temporary view from a logical plan.
     *
     * @param viewName Name of the temporary view
     * @param plan The logical plan for the view
     */
    void createTempView(String viewName, LogicalPlan plan);

    /**
     * Drop a temporary view.
     *
     * @param viewName Name of the view to drop
     */
    void dropTempView(String viewName);

    /**
     * Check if a table exists.
     *
     * @param tableName Name of the table
     * @return true if the table exists
     */
    boolean tableExists(String tableName);

    /**
     * Get a table as a Dataset.
     *
     * @param tableName Name of the table
     * @param session The Spark session context
     * @return Dataset representing the table
     */
    Dataset<Row> getTable(String tableName, SparkSession session);

    /**
     * List all tables in the current database.
     *
     * @return List of table names
     */
    List<String> listTables();

    /**
     * Close the execution engine and release resources.
     */
    @Override
    void close();
}