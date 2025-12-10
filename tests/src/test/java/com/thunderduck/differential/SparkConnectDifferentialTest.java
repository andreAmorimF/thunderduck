package com.thunderduck.differential;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Differential test suite for comparing thunderduck with Apache Spark via Spark Connect.
 *
 * This test requires a running Spark Connect server. Start it with:
 * ./start-spark-connect.sh
 *
 * Run tests with:
 * mvn test -Dtest=SparkConnectDifferentialTest -Dspark.connect.enabled=true
 */
@EnabledIfSystemProperty(named = "spark.connect.enabled", matches = "true")
public class SparkConnectDifferentialTest {

    private static SparkSession spark;
    private static Connection thunderduck;
    private static final String SPARK_CONNECT_URL = System.getProperty("spark.connect.url", "sc://localhost:15002");

    @BeforeAll
    static void setUp() throws Exception {
        // Connect to Spark via Spark Connect
        // NOTE: This requires spark-connect-client-jvm and a running Spark Connect server
        // For now, using local mode for differential testing
        // TODO: Enable remote() when testing against actual Spark Connect server
        spark = SparkSession.builder()
            .appName("DifferentialTest")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        // Create thunderduck connection
        thunderduck = DriverManager.getConnection("jdbc:duckdb:");

        // Create test data in both systems
        createTestData();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (spark != null) {
            spark.stop();
        }
        if (thunderduck != null && !thunderduck.isClosed()) {
            thunderduck.close();
        }
    }

    private static void createTestData() throws Exception {
        // Create sample data in Spark
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW employees AS " +
            "SELECT * FROM VALUES " +
            "(1, 'John', 'Engineering', 75000), " +
            "(2, 'Jane', 'Marketing', 65000), " +
            "(3, 'Bob', 'Engineering', 80000), " +
            "(4, 'Alice', 'HR', 55000), " +
            "(5, 'Charlie', 'Engineering', 70000) " +
            "AS t(id, name, department, salary)");

        // Create same data in thunderduck
        try (Statement stmt = thunderduck.createStatement()) {
            stmt.execute("CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR, salary INT)");
            stmt.execute("INSERT INTO employees VALUES " +
                "(1, 'John', 'Engineering', 75000), " +
                "(2, 'Jane', 'Marketing', 65000), " +
                "(3, 'Bob', 'Engineering', 80000), " +
                "(4, 'Alice', 'HR', 55000), " +
                "(5, 'Charlie', 'Engineering', 70000)");
        }
    }

    @Test
    @DisplayName("Test basic SELECT query")
    void testBasicSelect() {
        String query = "SELECT * FROM employees ORDER BY id";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test filtering with WHERE clause")
    void testWhereClause() {
        String query = "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY name";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test aggregation with GROUP BY")
    void testGroupBy() {
        String query = "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary " +
                      "FROM employees GROUP BY department ORDER BY department";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test aggregation with HAVING")
    void testHaving() {
        String query = "SELECT department, COUNT(*) as count " +
                      "FROM employees GROUP BY department " +
                      "HAVING COUNT(*) > 1 ORDER BY department";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test window functions")
    void testWindowFunctions() {
        String query = "SELECT name, department, salary, " +
                      "RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank " +
                      "FROM employees ORDER BY department, rank";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test CASE expressions")
    void testCaseExpressions() {
        String query = "SELECT name, " +
                      "CASE " +
                      "  WHEN salary > 70000 THEN 'High' " +
                      "  WHEN salary > 60000 THEN 'Medium' " +
                      "  ELSE 'Low' " +
                      "END as salary_band " +
                      "FROM employees ORDER BY name";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test string functions")
    void testStringFunctions() {
        String query = "SELECT UPPER(name) as upper_name, " +
                      "LOWER(department) as lower_dept, " +
                      "LENGTH(name) as name_length " +
                      "FROM employees ORDER BY id";
        assertResultsMatch(query);
    }

    @Test
    @DisplayName("Test numeric functions")
    void testNumericFunctions() {
        String query = "SELECT name, " +
                      "ROUND(salary / 1000.0, 1) as salary_k, " +
                      "salary % 10000 as salary_mod " +
                      "FROM employees ORDER BY id";
        assertResultsMatch(query);
    }

    private void assertResultsMatch(String query) {
        try {
            // Execute on Spark
            Dataset<Row> sparkResult = spark.sql(query);
            List<Row> sparkRows = sparkResult.collectAsList();

            // Execute on thunderduck
            List<Row> duckRows = new ArrayList<>();
            try (Statement stmt = thunderduck.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Object[] values = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        values[i] = rs.getObject(i + 1);
                    }
                    // Note: In real implementation, would need proper Row creation
                    // This is simplified for example
                }
            }

            // Compare results
            assertEquals(sparkRows.size(), duckRows.size(),
                "Row count mismatch for query: " + query);

            // Compare each row
            for (int i = 0; i < sparkRows.size(); i++) {
                Row sparkRow = sparkRows.get(i);
                Row duckRow = duckRows.get(i);

                assertEquals(sparkRow.length(), duckRow.length(),
                    "Column count mismatch at row " + i);

                for (int j = 0; j < sparkRow.length(); j++) {
                    Object sparkValue = sparkRow.get(j);
                    Object duckValue = duckRow.get(j);

                    if (sparkValue instanceof Double && duckValue instanceof Double) {
                        // For doubles, use epsilon comparison
                        assertEquals((Double) sparkValue, (Double) duckValue, 1e-10,
                            String.format("Value mismatch at row %d, column %d", i, j));
                    } else {
                        assertEquals(sparkValue, duckValue,
                            String.format("Value mismatch at row %d, column %d", i, j));
                    }
                }
            }

            System.out.println("✓ Query passed: " + query);

        } catch (Exception e) {
            fail("Query execution failed: " + query + "\n" + e.getMessage());
        }
    }
}