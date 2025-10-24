package com.thunderduck.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Structured logging for query execution lifecycle.
 *
 * <p>Provides correlation IDs for tracing queries through the system using SLF4J's
 * Mapped Diagnostic Context (MDC). Each query gets a unique identifier that is
 * included in all log messages for that query, making it easy to trace a query's
 * execution path through the system.
 *
 * <p><b>Thread Safety:</b> This class uses ThreadLocal storage for correlation IDs,
 * making it safe for concurrent query execution. Each thread maintains its own
 * correlation ID that doesn't interfere with other threads.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * // Start a new query
 * String queryId = UUID.randomUUID().toString();
 * QueryLogger.startQuery(queryId);
 *
 * try {
 *     // Log SQL generation
 *     long startGen = System.currentTimeMillis();
 *     String sql = generateSQL(plan);
 *     QueryLogger.logSQLGeneration(sql, System.currentTimeMillis() - startGen);
 *
 *     // Log optimization
 *     long startOpt = System.currentTimeMillis();
 *     Plan optimized = optimize(plan);
 *     QueryLogger.logOptimization(
 *         plan.toString(),
 *         optimized.toString(),
 *         System.currentTimeMillis() - startOpt
 *     );
 *
 *     // Log execution
 *     long startExec = System.currentTimeMillis();
 *     ResultSet results = execute(sql);
 *     QueryLogger.logExecution(
 *         System.currentTimeMillis() - startExec,
 *         results.size()
 *     );
 *
 *     // Complete query
 *     QueryLogger.completeQuery(System.currentTimeMillis() - queryStart);
 * } catch (Exception e) {
 *     QueryLogger.logError(e);
 *     throw e;
 * } finally {
 *     // Always clear context to prevent memory leaks
 *     QueryLogger.clearContext();
 * }
 * }</pre>
 *
 * <p><b>Log Output Example:</b>
 * <pre>
 * 2025-10-14 10:15:30.123 [main] INFO  com.thunderduck.logging.QueryLogger [abc-123] - Query started: abc-123
 * 2025-10-14 10:15:30.145 [main] DEBUG com.thunderduck.logging.QueryLogger [abc-123] - SQL generated in 22ms: SELECT * FROM users
 * 2025-10-14 10:15:30.167 [main] INFO  com.thunderduck.logging.QueryLogger [abc-123] - Query executed in 22ms, 100 rows returned
 * 2025-10-14 10:15:30.170 [main] INFO  com.thunderduck.logging.QueryLogger [abc-123] - Query completed in 47ms
 * </pre>
 *
 * @see org.slf4j.MDC
 * @since 1.0.0
 */
public class QueryLogger {

    private static final Logger logger = LoggerFactory.getLogger(QueryLogger.class);

    /**
     * Thread-local storage for correlation IDs.
     * This ensures each thread has its own correlation ID that doesn't interfere
     * with other threads processing queries concurrently.
     */
    private static final ThreadLocal<String> correlationId = new ThreadLocal<>();

    /**
     * MDC key used for storing the query correlation ID.
     */
    private static final String MDC_QUERY_ID_KEY = "queryId";

    /**
     * Starts logging for a new query with a correlation ID.
     *
     * <p>This method should be called at the beginning of query processing.
     * It sets up the correlation ID in both ThreadLocal storage and SLF4J's MDC,
     * ensuring all subsequent log messages include the query ID.
     *
     * <p><b>Important:</b> Always pair this with {@link #clearContext()} in a
     * finally block to prevent memory leaks.
     *
     * @param queryId unique identifier for this query (e.g., UUID)
     * @throws IllegalArgumentException if queryId is null or empty
     */
    public static void startQuery(String queryId) {
        if (queryId == null || queryId.trim().isEmpty()) {
            throw new IllegalArgumentException("Query ID cannot be null or empty");
        }

        correlationId.set(queryId);
        MDC.put(MDC_QUERY_ID_KEY, queryId);
        logger.info("Query started: {}", queryId);
    }

    /**
     * Logs the SQL generation phase of query processing.
     *
     * <p>This method logs at DEBUG level and includes the generated SQL text
     * along with timing information. Use this to track how long it takes to
     * translate logical plans into SQL.
     *
     * @param sql the generated SQL statement
     * @param generationTimeMs time taken to generate SQL in milliseconds
     * @throws IllegalStateException if called without an active query context
     */
    public static void logSQLGeneration(String sql, long generationTimeMs) {
        validateContext();
        logger.debug("SQL generated in {}ms: {}", generationTimeMs, sql);
    }

    /**
     * Logs the query execution phase.
     *
     * <p>This method logs at INFO level and includes execution timing and
     * row count information. Use this to track query execution performance
     * and result sizes.
     *
     * @param executionTimeMs time taken to execute the query in milliseconds
     * @param rowCount number of rows returned by the query
     * @throws IllegalStateException if called without an active query context
     */
    public static void logExecution(long executionTimeMs, long rowCount) {
        validateContext();
        logger.info("Query executed in {}ms, {} rows returned", executionTimeMs, rowCount);
    }

    /**
     * Logs the query optimization phase.
     *
     * <p>This method logs at DEBUG level and includes both the original and
     * optimized query plans along with timing information. Use this to track
     * the effectiveness of query optimization rules.
     *
     * @param originalPlan string representation of the original query plan
     * @param optimizedPlan string representation of the optimized query plan
     * @param optimizationTimeMs time taken to optimize in milliseconds
     * @throws IllegalStateException if called without an active query context
     */
    public static void logOptimization(String originalPlan, String optimizedPlan, long optimizationTimeMs) {
        validateContext();
        logger.debug("Query optimized in {}ms:\n  Original: {}\n  Optimized: {}",
            optimizationTimeMs, originalPlan, optimizedPlan);
    }

    /**
     * Logs a query error with full stack trace.
     *
     * <p>This method logs at ERROR level and includes the full exception
     * stack trace. Use this to log any errors that occur during query processing.
     *
     * <p>Note: This method does not require an active query context, as errors
     * may occur before a query context is established or after it's cleared.
     *
     * @param error the error that occurred during query processing
     * @throws IllegalArgumentException if error is null
     */
    public static void logError(Throwable error) {
        if (error == null) {
            throw new IllegalArgumentException("Error cannot be null");
        }

        // Log error even if no correlation ID is set
        String currentQueryId = correlationId.get();
        if (currentQueryId != null) {
            logger.error("Query failed: {}", error.getMessage(), error);
        } else {
            logger.error("Error occurred (no active query): {}", error.getMessage(), error);
        }
    }

    /**
     * Completes logging for the current query.
     *
     * <p>This method logs at INFO level with the total query execution time.
     * It automatically clears the MDC and ThreadLocal correlation ID, so there's
     * no need to call {@link #clearContext()} after this method.
     *
     * @param totalTimeMs total time from query start to completion in milliseconds
     * @throws IllegalStateException if called without an active query context
     */
    public static void completeQuery(long totalTimeMs) {
        validateContext();
        logger.info("Query completed in {}ms", totalTimeMs);
        clearContext();
    }

    /**
     * Clears the correlation ID from both MDC and ThreadLocal storage.
     *
     * <p>This method should be called in a finally block to ensure the context
     * is always cleared, preventing memory leaks in thread pools where threads
     * are reused.
     *
     * <p><b>Important:</b> Always call this method when query processing is complete,
     * whether the query succeeded or failed.
     *
     * <p><b>Example:</b>
     * <pre>{@code
     * QueryLogger.startQuery(queryId);
     * try {
     *     // Process query
     * } finally {
     *     QueryLogger.clearContext();
     * }
     * }</pre>
     */
    public static void clearContext() {
        MDC.remove(MDC_QUERY_ID_KEY);
        correlationId.remove();
    }

    /**
     * Returns the current correlation ID for the active query.
     *
     * <p>This method can be used to retrieve the correlation ID for manual
     * logging or debugging purposes.
     *
     * @return the correlation ID or null if no query is active
     */
    public static String getCorrelationId() {
        return correlationId.get();
    }

    /**
     * Validates that a query context is active.
     *
     * @throws IllegalStateException if no query context is active
     */
    private static void validateContext() {
        if (correlationId.get() == null) {
            throw new IllegalStateException(
                "No active query context. Call startQuery() before logging query operations."
            );
        }
    }
}
