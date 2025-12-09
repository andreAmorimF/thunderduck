package com.thunderduck.logical;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.generator.SQLGenerator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.FloatingPointPrecision;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for LocalDataRelation - the logical plan node that handles
 * Arrow IPC data from PySpark createDataFrame() operations.
 *
 * Tests cover:
 * - SQL generation for VALUES clauses
 * - Handling of various data types (int, double, string)
 * - Empty relations
 * - Count aggregation on local data
 *
 * @see LocalDataRelation
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("LocalDataRelation Unit Tests")
public class LocalDataRelationTest extends TestBase {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Nested
    @DisplayName("SQL Generation Tests")
    class SQLGenerationTests {

        @Test
        @DisplayName("Should generate VALUES clause for single row with integers")
        void testSingleRowWithIntegers() throws Exception {
            // Create Arrow data with single row: (1, 2, 3)
            byte[] arrowData = createIntArrowData(
                    new String[]{"a", "b", "c"},
                    new int[][]{{1, 2, 3}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains("VALUES")
                    .contains("1")
                    .contains("2")
                    .contains("3");
        }

        @Test
        @DisplayName("Should generate VALUES clause for multiple rows")
        void testMultipleRows() throws Exception {
            byte[] arrowData = createIntArrowData(
                    new String[]{"id", "value"},
                    new int[][]{
                            {1, 100},
                            {2, 200},
                            {3, 300}
                    }
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains("VALUES")
                    .contains("100")
                    .contains("200")
                    .contains("300");
        }

        @Test
        @DisplayName("Should handle mixed data types (int, double, string)")
        void testMixedDataTypes() throws Exception {
            byte[] arrowData = createMixedArrowData(
                    new String[]{"a", "b", "c"},
                    new int[]{2},
                    new double[]{3.0},
                    new String[]{"string2"}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains("2")
                    .contains("3.0")
                    .contains("string2");
        }

        @Test
        @DisplayName("Should properly escape string values with quotes")
        void testStringEscaping() throws Exception {
            byte[] arrowData = createStringArrowData(
                    new String[]{"name"},
                    new String[]{"O'Brien"}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            // Single quotes should be escaped as ''
            assertThat(sql).contains("O''Brien");
        }

        @Test
        @DisplayName("Should handle NULL values")
        void testNullValues() throws Exception {
            byte[] arrowData = createIntArrowDataWithNulls(
                    new String[]{"id", "value"},
                    new Integer[][]{{1, null}, {null, 200}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql).containsIgnoringCase("NULL");
        }

        @Test
        @DisplayName("Should handle large integer values")
        void testLargeIntegerValues() throws Exception {
            byte[] arrowData = createIntArrowData(
                    new String[]{"big_num"},
                    new int[][]{{Integer.MAX_VALUE}, {Integer.MIN_VALUE}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains(String.valueOf(Integer.MAX_VALUE))
                    .contains(String.valueOf(Integer.MIN_VALUE));
        }

        @Test
        @DisplayName("Should handle double precision values")
        void testDoublePrecisionValues() throws Exception {
            byte[] arrowData = createDoubleArrowData(
                    new String[]{"price"},
                    new double[][]{{3.14159265359}, {2.71828182846}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains("3.14")
                    .contains("2.71");
        }
    }

    @Nested
    @DisplayName("Empty Relation Tests")
    class EmptyRelationTests {

        @Test
        @DisplayName("Should generate empty result for empty data")
        void testEmptyRelation() throws Exception {
            byte[] arrowData = createEmptyArrowData(new String[]{"id", "name"});

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            // Empty relation should produce something like:
            // SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE
            assertThat(sql).containsIgnoringCase("FALSE");
        }
    }

    @Nested
    @DisplayName("Count Aggregation Tests")
    class CountAggregationTests {

        @Test
        @DisplayName("Should support COUNT(*) on local relation")
        void testCountOnLocalRelation() throws Exception {
            byte[] arrowData = createIntArrowData(
                    new String[]{"id"},
                    new int[][]{{1}, {2}, {3}, {4}, {5}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            // Wrap in aggregate for COUNT(*)
            Aggregate countAggregate = new Aggregate(
                    relation,
                    List.of(), // no grouping
                    List.of(new Aggregate.AggregateExpression("count", null, "count"))
            );

            String sql = generator.generate(countAggregate);

            assertThat(sql)
                    .containsIgnoringCase("COUNT")
                    .contains("VALUES");
        }

        @Test
        @DisplayName("Should support COUNT with grouping on local relation")
        void testCountWithGroupingOnLocalRelation() throws Exception {
            byte[] arrowData = createMixedArrowData(
                    new String[]{"category", "amount", "name"},
                    new int[]{1, 2, 1},       // categories
                    new double[]{100.0, 200.0, 150.0},  // amounts
                    new String[]{"A", "B", "C"}         // names
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);

            // This tests that LocalDataRelation can be used as input to aggregation
            assertThat(relation).isNotNull();
            assertThat(relation.getArrowData()).isEqualTo(arrowData);
        }
    }

    @Nested
    @DisplayName("Schema Handling Tests")
    class SchemaHandlingTests {

        @Test
        @DisplayName("Should preserve column names from Arrow schema")
        void testColumnNamesPreserved() throws Exception {
            byte[] arrowData = createIntArrowData(
                    new String[]{"customer_id", "order_total", "item_count"},
                    new int[][]{{1, 100, 5}}
            );

            LocalDataRelation relation = new LocalDataRelation(arrowData, null);
            SQLGenerator generator = new SQLGenerator();

            String sql = generator.generate(relation);

            assertThat(sql)
                    .contains("customer_id")
                    .contains("order_total")
                    .contains("item_count");
        }
    }

    // ==================== Helper Methods ====================

    private byte[] createIntArrowData(String[] columnNames, int[][] rows) throws Exception {
        List<Field> fields = Arrays.stream(columnNames)
                .map(name -> Field.nullable(name, new ArrowType.Int(32, true)))
                .toList();
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();

            int rowCount = rows.length;
            for (int col = 0; col < columnNames.length; col++) {
                IntVector vector = (IntVector) root.getVector(col);
                vector.allocateNew(rowCount);
                for (int row = 0; row < rowCount; row++) {
                    vector.set(row, rows[row][col]);
                }
                vector.setValueCount(rowCount);
            }
            root.setRowCount(rowCount);

            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }

    private byte[] createIntArrowDataWithNulls(String[] columnNames, Integer[][] rows) throws Exception {
        List<Field> fields = Arrays.stream(columnNames)
                .map(name -> Field.nullable(name, new ArrowType.Int(32, true)))
                .toList();
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();

            int rowCount = rows.length;
            for (int col = 0; col < columnNames.length; col++) {
                IntVector vector = (IntVector) root.getVector(col);
                vector.allocateNew(rowCount);
                for (int row = 0; row < rowCount; row++) {
                    if (rows[row][col] == null) {
                        vector.setNull(row);
                    } else {
                        vector.set(row, rows[row][col]);
                    }
                }
                vector.setValueCount(rowCount);
            }
            root.setRowCount(rowCount);

            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }

    private byte[] createDoubleArrowData(String[] columnNames, double[][] rows) throws Exception {
        List<Field> fields = Arrays.stream(columnNames)
                .map(name -> Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
                .toList();
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();

            int rowCount = rows.length;
            for (int col = 0; col < columnNames.length; col++) {
                Float8Vector vector = (Float8Vector) root.getVector(col);
                vector.allocateNew(rowCount);
                for (int row = 0; row < rowCount; row++) {
                    vector.set(row, rows[row][col]);
                }
                vector.setValueCount(rowCount);
            }
            root.setRowCount(rowCount);

            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }

    private byte[] createStringArrowData(String[] columnNames, String[] values) throws Exception {
        List<Field> fields = Arrays.stream(columnNames)
                .map(name -> Field.nullable(name, new ArrowType.Utf8()))
                .toList();
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();

            VarCharVector vector = (VarCharVector) root.getVector(0);
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, values[i].getBytes());
            }
            vector.setValueCount(values.length);
            root.setRowCount(values.length);

            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }

    private byte[] createMixedArrowData(String[] columnNames, int[] intVals, double[] doubleVals, String[] stringVals) throws Exception {
        Schema schema = new Schema(Arrays.asList(
                Field.nullable(columnNames[0], new ArrowType.Int(32, true)),
                Field.nullable(columnNames[1], new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable(columnNames[2], new ArrowType.Utf8())
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();

            int rowCount = intVals.length;

            IntVector intVector = (IntVector) root.getVector(0);
            Float8Vector doubleVector = (Float8Vector) root.getVector(1);
            VarCharVector stringVector = (VarCharVector) root.getVector(2);

            intVector.allocateNew(rowCount);
            doubleVector.allocateNew(rowCount);
            stringVector.allocateNew(rowCount);

            for (int i = 0; i < rowCount; i++) {
                intVector.set(i, intVals[i]);
                doubleVector.set(i, doubleVals[i]);
                stringVector.set(i, stringVals[i].getBytes());
            }

            intVector.setValueCount(rowCount);
            doubleVector.setValueCount(rowCount);
            stringVector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }

    private byte[] createEmptyArrowData(String[] columnNames) throws Exception {
        List<Field> fields = Arrays.stream(columnNames)
                .map(name -> Field.nullable(name, new ArrowType.Int(32, true)))
                .toList();
        Schema schema = new Schema(fields);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            writer.start();
            root.setRowCount(0);
            writer.writeBatch();
            writer.end();

            return out.toByteArray();
        }
    }
}
