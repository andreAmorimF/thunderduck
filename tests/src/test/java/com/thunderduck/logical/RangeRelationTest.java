package com.thunderduck.logical;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.types.LongType;
import com.thunderduck.types.StructType;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for RangeRelation - the logical plan node that handles
 * spark.range() operations.
 *
 * Tests cover:
 * - SQL generation for various range parameters
 * - Schema inference (single column "id" of type BIGINT)
 * - Edge cases (negative step, empty range, large values)
 * - Integration with DuckDB execution
 *
 * @see RangeRelation
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("RangeRelation Unit Tests")
public class RangeRelationTest extends TestBase {

    private SQLGenerator generator;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("Constructor Validation Tests")
    class ConstructorValidationTests {

        @Test
        @DisplayName("Should create RangeRelation with valid parameters")
        void testValidConstruction() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            assertThat(range.start()).isEqualTo(0);
            assertThat(range.end()).isEqualTo(10);
            assertThat(range.step()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should reject zero step")
        void testZeroStepRejected() {
            assertThatThrownBy(() -> new RangeRelation(0, 10, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("step cannot be zero");
        }

        @Test
        @DisplayName("Should allow negative step for descending ranges")
        void testNegativeStepAllowed() {
            RangeRelation range = new RangeRelation(10, 0, -1);

            assertThat(range.start()).isEqualTo(10);
            assertThat(range.end()).isEqualTo(0);
            assertThat(range.step()).isEqualTo(-1);
        }

        @Test
        @DisplayName("Should allow negative start value")
        void testNegativeStartAllowed() {
            RangeRelation range = new RangeRelation(-10, 10, 1);

            assertThat(range.start()).isEqualTo(-10);
            assertThat(range.end()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("Schema Inference Tests")
    class SchemaInferenceTests {

        @Test
        @DisplayName("Should have schema with single 'id' column of type BIGINT")
        void testSchemaIsIdBigint() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            StructType schema = range.inferSchema();

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(LongType.class);
            assertThat(schema.fieldAt(0).nullable()).isFalse();
        }

        @Test
        @DisplayName("Should return same schema instance on multiple calls")
        void testSchemaIsCached() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            StructType schema1 = range.schema();
            StructType schema2 = range.schema();

            assertThat(schema1).isSameAs(schema2);
        }
    }

    @Nested
    @DisplayName("Count Calculation Tests")
    class CountCalculationTests {

        @Test
        @DisplayName("Should calculate count for simple range")
        void testSimpleRangeCount() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            assertThat(range.count()).isEqualTo(10);
        }

        @Test
        @DisplayName("Should calculate count for range with step > 1")
        void testRangeWithLargerStep() {
            RangeRelation range = new RangeRelation(0, 10, 2);
            assertThat(range.count()).isEqualTo(5); // 0, 2, 4, 6, 8
        }

        @Test
        @DisplayName("Should calculate count for range with step = 3")
        void testRangeWithStepThree() {
            RangeRelation range = new RangeRelation(0, 10, 3);
            assertThat(range.count()).isEqualTo(4); // 0, 3, 6, 9
        }

        @Test
        @DisplayName("Should return 0 for empty ascending range")
        void testEmptyAscendingRange() {
            RangeRelation range = new RangeRelation(10, 5, 1);
            assertThat(range.count()).isEqualTo(0);
        }

        @Test
        @DisplayName("Should calculate count for descending range")
        void testDescendingRange() {
            RangeRelation range = new RangeRelation(10, 0, -1);
            assertThat(range.count()).isEqualTo(10); // 10, 9, 8, ..., 1
        }

        @Test
        @DisplayName("Should return 0 for empty descending range")
        void testEmptyDescendingRange() {
            RangeRelation range = new RangeRelation(0, 10, -1);
            assertThat(range.count()).isEqualTo(0);
        }

        @Test
        @DisplayName("Should calculate count for range starting at non-zero")
        void testRangeStartingAtNonZero() {
            RangeRelation range = new RangeRelation(5, 15, 1);
            assertThat(range.count()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("SQL Generation Tests")
    class SQLGenerationTests {

        @Test
        @DisplayName("Should generate correct SQL for simple range")
        void testSimpleRangeSQL() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            String sql = generator.generate(range);

            assertThat(sql)
                    .contains("SELECT")
                    .contains("range")
                    .contains("AS")
                    .contains("\"id\"")
                    .contains("FROM range(0, 10, 1)");
        }

        @Test
        @DisplayName("Should generate correct SQL for range with custom start")
        void testRangeWithCustomStartSQL() {
            RangeRelation range = new RangeRelation(5, 15, 1);
            String sql = generator.generate(range);

            assertThat(sql).contains("range(5, 15, 1)");
        }

        @Test
        @DisplayName("Should generate correct SQL for range with step > 1")
        void testRangeWithLargerStepSQL() {
            RangeRelation range = new RangeRelation(0, 100, 10);
            String sql = generator.generate(range);

            assertThat(sql).contains("range(0, 100, 10)");
        }

        @Test
        @DisplayName("Should generate correct SQL for descending range")
        void testDescendingRangeSQL() {
            RangeRelation range = new RangeRelation(10, 0, -1);
            String sql = generator.generate(range);

            assertThat(sql).contains("range(10, 0, -1)");
        }

        @Test
        @DisplayName("Should generate correct SQL for negative values")
        void testNegativeValuesSQL() {
            RangeRelation range = new RangeRelation(-10, 10, 1);
            String sql = generator.generate(range);

            assertThat(sql).contains("range(-10, 10, 1)");
        }

        @Test
        @DisplayName("Should generate correct SQL for large values")
        void testLargeValuesSQL() {
            RangeRelation range = new RangeRelation(0, 1_000_000_000L, 1);
            String sql = generator.generate(range);

            assertThat(sql).contains("range(0, 1000000000, 1)");
        }
    }

    @Nested
    @DisplayName("Equality and HashCode Tests")
    class EqualityTests {

        @Test
        @DisplayName("Should be equal to another RangeRelation with same parameters")
        void testEquality() {
            RangeRelation range1 = new RangeRelation(0, 10, 1);
            RangeRelation range2 = new RangeRelation(0, 10, 1);

            assertThat(range1).isEqualTo(range2);
            assertThat(range1.hashCode()).isEqualTo(range2.hashCode());
        }

        @Test
        @DisplayName("Should not be equal with different start")
        void testInequalityDifferentStart() {
            RangeRelation range1 = new RangeRelation(0, 10, 1);
            RangeRelation range2 = new RangeRelation(1, 10, 1);

            assertThat(range1).isNotEqualTo(range2);
        }

        @Test
        @DisplayName("Should not be equal with different end")
        void testInequalityDifferentEnd() {
            RangeRelation range1 = new RangeRelation(0, 10, 1);
            RangeRelation range2 = new RangeRelation(0, 20, 1);

            assertThat(range1).isNotEqualTo(range2);
        }

        @Test
        @DisplayName("Should not be equal with different step")
        void testInequalityDifferentStep() {
            RangeRelation range1 = new RangeRelation(0, 10, 1);
            RangeRelation range2 = new RangeRelation(0, 10, 2);

            assertThat(range1).isNotEqualTo(range2);
        }
    }

    @Nested
    @DisplayName("ToString Tests")
    class ToStringTests {

        @Test
        @DisplayName("Should produce readable toString output")
        void testToString() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            String str = range.toString();

            assertThat(str)
                    .contains("RangeRelation")
                    .contains("start=0")
                    .contains("end=10")
                    .contains("step=1");
        }
    }

    @Nested
    @DisplayName("DuckDB Execution Tests")
    @TestCategories.Integration
    class DuckDBExecutionTests {

        @Test
        @DisplayName("Should execute simple range in DuckDB")
        void testSimpleRangeExecution() throws Exception {
            RangeRelation range = new RangeRelation(0, 5, 1);
            String sql = generator.generate(range);

            List<Long> results = executeAndCollect(sql);

            assertThat(results).containsExactly(0L, 1L, 2L, 3L, 4L);
        }

        @Test
        @DisplayName("Should execute range with step in DuckDB")
        void testRangeWithStepExecution() throws Exception {
            RangeRelation range = new RangeRelation(0, 10, 2);
            String sql = generator.generate(range);

            List<Long> results = executeAndCollect(sql);

            assertThat(results).containsExactly(0L, 2L, 4L, 6L, 8L);
        }

        @Test
        @DisplayName("Should execute descending range in DuckDB")
        void testDescendingRangeExecution() throws Exception {
            RangeRelation range = new RangeRelation(5, 0, -1);
            String sql = generator.generate(range);

            List<Long> results = executeAndCollect(sql);

            assertThat(results).containsExactly(5L, 4L, 3L, 2L, 1L);
        }

        @Test
        @DisplayName("Should execute range with negative start in DuckDB")
        void testNegativeStartExecution() throws Exception {
            RangeRelation range = new RangeRelation(-3, 3, 1);
            String sql = generator.generate(range);

            List<Long> results = executeAndCollect(sql);

            assertThat(results).containsExactly(-3L, -2L, -1L, 0L, 1L, 2L);
        }

        @Test
        @DisplayName("Should return empty result for empty range")
        void testEmptyRangeExecution() throws Exception {
            RangeRelation range = new RangeRelation(10, 5, 1);
            String sql = generator.generate(range);

            List<Long> results = executeAndCollect(sql);

            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("Should support range with filter")
        void testRangeWithFilter() throws Exception {
            RangeRelation range = new RangeRelation(0, 10, 1);
            String rangeSQL = generator.generate(range);

            // Wrap in filter
            String sql = "SELECT * FROM (" + rangeSQL + ") WHERE \"id\" > 5";

            List<Long> results = executeAndCollect(sql);

            assertThat(results).containsExactly(6L, 7L, 8L, 9L);
        }

        @Test
        @DisplayName("Should support range with aggregation")
        void testRangeWithAggregation() throws Exception {
            RangeRelation range = new RangeRelation(0, 10, 1);
            String rangeSQL = generator.generate(range);

            String sql = "SELECT SUM(\"id\") as total FROM (" + rangeSQL + ")";

            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("total")).isEqualTo(45L); // 0+1+2+...+9 = 45
            }
        }

        private List<Long> executeAndCollect(String sql) throws Exception {
            List<Long> results = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                while (rs.next()) {
                    results.add(rs.getLong("id"));
                }
            }
            return results;
        }
    }

    @Nested
    @DisplayName("Composability Tests")
    class ComposabilityTests {

        @Test
        @DisplayName("RangeRelation should have no children (leaf node)")
        void testIsLeafNode() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            assertThat(range.children()).isEmpty();
        }

        @Test
        @DisplayName("RangeRelation can be used as input to Project")
        void testAsProjectInput() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            // Create a simple project that selects the id column
            com.thunderduck.expression.ColumnReference idCol =
                com.thunderduck.expression.ColumnReference.of("id", com.thunderduck.types.LongType.get());
            Project project = new Project(range, List.of(idCol));

            String sql = generator.generate(project);

            assertThat(sql)
                    .contains("SELECT")
                    .contains("id")
                    .contains("FROM")
                    .contains("range(0, 10, 1)");
        }

        @Test
        @DisplayName("RangeRelation can be used as input to Filter")
        void testAsFilterInput() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            // Create a filter: id > 5
            com.thunderduck.expression.ColumnReference idCol =
                com.thunderduck.expression.ColumnReference.of("id", com.thunderduck.types.LongType.get());
            com.thunderduck.expression.Literal five =
                com.thunderduck.expression.Literal.of(5L);
            com.thunderduck.expression.BinaryExpression condition =
                new com.thunderduck.expression.BinaryExpression(
                    idCol,
                    com.thunderduck.expression.BinaryExpression.Operator.GREATER_THAN,
                    five);

            Filter filter = new Filter(range, condition);

            String sql = generator.generate(filter);

            assertThat(sql)
                    .contains("WHERE")
                    .contains("id")
                    .contains(">")
                    .contains("5");
        }

        @Test
        @DisplayName("RangeRelation can be used as input to Limit")
        void testAsLimitInput() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Limit limit = new Limit(range, 10);

            String sql = generator.generate(limit);

            assertThat(sql)
                    .contains("LIMIT 10")
                    .contains("range(0, 100, 1)");
        }
    }
}
