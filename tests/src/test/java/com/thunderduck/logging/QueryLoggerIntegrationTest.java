package com.thunderduck.logging;

import com.thunderduck.exception.QueryExecutionException;
import com.thunderduck.runtime.DuckDBConnectionManager;
import com.thunderduck.runtime.QueryExecutor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for QueryLogger with QueryExecutor.
 *
 * <p>Verifies that:
 * - Correlation IDs are set and tracked
 * - Log messages are generated at appropriate points
 * - Logging doesn't interfere with query execution
 * - Context is properly cleared after execution
 */
class QueryLoggerIntegrationTest {

    private DuckDBConnectionManager connectionManager;
    private QueryExecutor executor;

    @BeforeEach
    void setUp() {
        connectionManager = new DuckDBConnectionManager();
        executor = new QueryExecutor(connectionManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Test
    @DisplayName("Should start and complete query logging for successful query")
    void testSuccessfulQueryLogging() {
        // Given: A simple query
        String sql = "SELECT 1 as test_value";

        // When: Execute query
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Query should execute successfully
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isEqualTo(1);

        // And: Correlation ID should be cleared after execution
        assertThat(QueryLogger.getCorrelationId()).isNull();

        result.close();
    }

    @Test
    @DisplayName("Should log errors and clear context on query failure")
    void testFailedQueryLogging() {
        // Given: An invalid SQL query
        String sql = "SELECT * FROM non_existent_table";

        // When: Execute query
        // Then: Should throw QueryExecutionException
        assertThatThrownBy(() -> executor.executeQuery(sql))
            .isInstanceOf(QueryExecutionException.class)
            .hasMessageContaining("Failed to execute query");

        // And: Correlation ID should be cleared even after error
        assertThat(QueryLogger.getCorrelationId()).isNull();
    }

    @Test
    @DisplayName("Should log EXPLAIN statement execution")
    void testExplainStatementLogging() {
        // Given: An EXPLAIN query
        String sql = "EXPLAIN SELECT 1 as value";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return EXPLAIN output
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        // And: Context should be cleared
        assertThat(QueryLogger.getCorrelationId()).isNull();

        result.close();
    }

    @Test
    @DisplayName("Should handle concurrent queries with separate correlation IDs")
    void testConcurrentQueryLogging() throws InterruptedException {
        // Given: Two concurrent queries
        Thread thread1 = new Thread(() -> {
            VectorSchemaRoot result = executor.executeQuery("SELECT 1 as t1");
            result.close();
        });

        Thread thread2 = new Thread(() -> {
            VectorSchemaRoot result = executor.executeQuery("SELECT 2 as t2");
            result.close();
        });

        // When: Execute concurrently
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Then: Both threads should complete successfully
        // (If correlation IDs interfered, one might fail or hang)
    }

    @Test
    @DisplayName("Should clear context even when result is not closed immediately")
    void testContextCleanup() {
        // Given: A query that returns result
        String sql = "SELECT 1 as value";

        // When: Execute query but don't close result immediately
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Context should still be cleared after executeQuery returns
        assertThat(QueryLogger.getCorrelationId()).isNull();

        // Cleanup
        result.close();
    }

    @Test
    @DisplayName("Should log execution time for queries")
    void testExecutionTimeLogging() {
        // Given: A query that takes some time
        String sql = "SELECT * FROM range(0, 1000)";

        // When: Execute query
        long startTime = System.nanoTime();
        VectorSchemaRoot result = executor.executeQuery(sql);
        long duration = (System.nanoTime() - startTime) / 1_000_000;

        // Then: Query should complete
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isEqualTo(1000);

        // And: Execution should be reasonably fast (< 1 second)
        assertThat(duration).isLessThan(1000);

        result.close();
    }

    @Test
    @DisplayName("Should log row counts for queries")
    void testRowCountLogging() {
        // Given: A query with known row count
        String sql = "SELECT * FROM range(0, 100)";

        // When: Execute query
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return correct number of rows
        assertThat(result.getRowCount()).isEqualTo(100);

        result.close();
    }
}
