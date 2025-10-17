package com.catalyst2sql.introspection;

import com.catalyst2sql.exception.QueryExecutionException;
import com.catalyst2sql.runtime.DuckDBConnectionManager;
import com.catalyst2sql.runtime.QueryExecutor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for EXPLAIN statement support in QueryExecutor.
 *
 * <p>Verifies that catalyst2sql correctly handles DuckDB EXPLAIN statements
 * for SQL introspection without requiring API changes.
 */
class ExplainStatementTest {

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
    @DisplayName("Should execute basic EXPLAIN statement")
    void testBasicExplain() {
        // Given: A simple EXPLAIN query
        String sql = "EXPLAIN SELECT 1 as value";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return EXPLAIN output
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        // And: Output should contain plan information
        VarCharVector explainOutput = (VarCharVector) result.getVector(0);
        String planText = explainOutput.getObject(0).toString();
        assertThat(planText).isNotBlank();

        result.close();
    }

    @Test
    @DisplayName("Should execute EXPLAIN ANALYZE statement")
    void testExplainAnalyze() {
        // Given: An EXPLAIN ANALYZE query
        String sql = "EXPLAIN ANALYZE SELECT * FROM range(0, 100)";

        // When: Execute EXPLAIN ANALYZE
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return execution statistics
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should execute EXPLAIN with FORMAT JSON")
    void testExplainJson() {
        // Given: An EXPLAIN query with JSON format
        String sql = "EXPLAIN (FORMAT JSON) SELECT 1 as value";

        // When: Execute EXPLAIN with JSON format
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return JSON-formatted plan
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        // Note: Actual JSON parsing would be done by the client
        result.close();
    }

    @Test
    @DisplayName("Should explain query with FROM clause")
    void testExplainWithFrom() {
        // Given: EXPLAIN with range function
        String sql = "EXPLAIN SELECT * FROM range(0, 1000) WHERE range > 500";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan showing FILTER operation
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should explain aggregation query")
    void testExplainAggregation() {
        // Given: EXPLAIN with aggregation
        String sql = "EXPLAIN SELECT COUNT(*) as count FROM range(0, 1000)";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan with AGGREGATE operation
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should explain join query")
    void testExplainJoin() {
        // Given: EXPLAIN with join
        String sql = "EXPLAIN SELECT * FROM " +
                     "range(0, 10) r1 " +
                     "JOIN range(0, 10) r2 ON r1.range = r2.range";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan with JOIN operation
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should handle EXPLAIN with ORDER BY")
    void testExplainOrderBy() {
        // Given: EXPLAIN with sorting
        String sql = "EXPLAIN SELECT * FROM range(0, 100) ORDER BY range DESC";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan with ORDER operation
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should handle EXPLAIN with LIMIT")
    void testExplainLimit() {
        // Given: EXPLAIN with limit
        String sql = "EXPLAIN SELECT * FROM range(0, 1000) LIMIT 10";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan with LIMIT operation
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should handle EXPLAIN with subquery")
    void testExplainSubquery() {
        // Given: EXPLAIN with subquery
        String sql = "EXPLAIN SELECT * FROM " +
                     "(SELECT range as id FROM range(0, 100)) " +
                     "WHERE id > 50";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return plan showing subquery structure
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should detect EXPLAIN with different case")
    void testExplainCaseInsensitive() {
        // Given: EXPLAIN in lowercase
        String sql = "explain SELECT 1 as value";

        // When: Execute explain
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should still work (case-insensitive detection)
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should detect EXPLAIN with leading whitespace")
    void testExplainWithWhitespace() {
        // Given: EXPLAIN with leading whitespace
        String sql = "  \n  EXPLAIN SELECT 1 as value";

        // When: Execute EXPLAIN
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should work (whitespace is trimmed)
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        result.close();
    }

    @Test
    @DisplayName("Should handle invalid EXPLAIN query")
    void testInvalidExplainQuery() {
        // Given: EXPLAIN with invalid SQL
        String sql = "EXPLAIN SELECT * FROM non_existent_table";

        // When: Execute invalid EXPLAIN
        // Then: Should throw QueryExecutionException
        assertThatThrownBy(() -> executor.executeQuery(sql))
            .isInstanceOf(QueryExecutionException.class);
    }

    @Test
    @DisplayName("Should compare regular query vs EXPLAIN performance")
    void testExplainVsActualPerformance() {
        // Given: A query
        String regularSQL = "SELECT * FROM range(0, 10000)";
        String explainSQL = "EXPLAIN " + regularSQL;

        // When: Execute EXPLAIN (should be fast, no actual execution)
        long explainStart = System.nanoTime();
        VectorSchemaRoot explainResult = executor.executeQuery(explainSQL);
        long explainDuration = (System.nanoTime() - explainStart) / 1_000_000;

        // And: Execute actual query
        long regularStart = System.nanoTime();
        VectorSchemaRoot regularResult = executor.executeQuery(regularSQL);
        long regularDuration = (System.nanoTime() - regularStart) / 1_000_000;

        // Then: EXPLAIN should be faster (just planning, no execution)
        assertThat(explainDuration).isLessThan(regularDuration);

        explainResult.close();
        regularResult.close();
    }

    @Test
    @DisplayName("Should handle EXPLAIN ANALYZE with actual execution")
    void testExplainAnalyzeExecutes() {
        // Given: EXPLAIN ANALYZE query
        String sql = "EXPLAIN ANALYZE SELECT COUNT(*) FROM range(0, 10000)";

        // When: Execute EXPLAIN ANALYZE
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Then: Should return actual execution statistics
        assertThat(result).isNotNull();
        assertThat(result.getRowCount()).isGreaterThan(0);

        // Note: EXPLAIN ANALYZE actually executes the query,
        // so it will take time proportional to query complexity
        result.close();
    }
}
