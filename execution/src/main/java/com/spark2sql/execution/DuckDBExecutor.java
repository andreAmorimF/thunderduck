package com.spark2sql.execution;

import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.translator.PlanToSQLTranslator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DuckDB execution engine for running SQL queries.
 * Handles query execution and result materialization.
 */
public class DuckDBExecutor implements ExecutionEngine {
    private static final Logger LOG = LoggerFactory.getLogger(DuckDBExecutor.class);

    private final Connection connection;
    private final PlanToSQLTranslator translator;
    private final Map<String, String> tempViews = new HashMap<>();

    public DuckDBExecutor() throws SQLException {
        // Initialize embedded DuckDB
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("DuckDB driver not found", e);
        }

        this.connection = DriverManager.getConnection("jdbc:duckdb:");
        this.translator = new PlanToSQLTranslator();

        configureDuckDB();
        LOG.info("DuckDB executor initialized");
    }

    private void configureDuckDB() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Configure DuckDB for Spark compatibility

            // Set memory limit (optional)
            stmt.execute("SET memory_limit='4GB'");

            // Set threads
            stmt.execute("SET threads=4");

            // Disable progress bar
            stmt.execute("SET enable_progress_bar=false");

            // Configure null ordering to match Spark
            stmt.execute("SET default_null_order='nulls_last'");

            // Register Spark-compatible UDFs
            registerSparkUDFs();
        }
    }

    private void registerSparkUDFs() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Integer division with truncation semantics (Spark-compatible)
            // This ensures -7/2 = -3 (truncation) not -4 (floor)
            stmt.execute(
                "CREATE OR REPLACE MACRO spark_int_divide(a, b) AS (" +
                "    CASE" +
                "        WHEN b = 0 THEN NULL" +
                "        ELSE CAST(TRUNC(CAST(a AS DOUBLE) / CAST(b AS DOUBLE)) AS INTEGER)" +
                "    END" +
                ")"
            );

            LOG.debug("Spark UDFs registered");
        }
    }

    @Override
    public <T> List<T> execute(LogicalPlan plan, Encoder<T> encoder) {
        try {
            // Translate plan to SQL
            String sql = translator.translate(plan);
            LOG.debug("Generated SQL: {}", sql);

            // Execute query
            List<Row> rows = executeSQL(sql, plan.schema());

            // Convert rows using encoder
            // For now, we'll just cast since we're using RowEncoder
            // In a full implementation, we'd properly deserialize using the encoder
            @SuppressWarnings("unchecked")
            List<T> results = (List<T>) rows;

            return results;

        } catch (Exception e) {
            LOG.error("Execution failed", e);
            throw new RuntimeException("Query execution failed", e);
        }
    }

    @Override
    public Dataset<Row> sql(String sqlQuery, SparkSession session) {
        try {
            // For now, execute directly and return as LocalRelation
            // In full implementation, would parse SQL to LogicalPlan
            List<Row> rows = executeSQLDirect(sqlQuery);
            if (rows.isEmpty()) {
                // Return empty dataset with unknown schema
                // Return empty dataset - the encoder will be created by Dataset constructor
                return createEmptyDataset(session);
            }

            // Infer schema from first row
            StructType schema = inferSchema(rows.get(0));
            LogicalPlan plan = new com.spark2sql.plan.nodes.LocalRelation(rows, schema);
            return createDatasetWithPlan(session, plan);

        } catch (SQLException e) {
            throw new RuntimeException("SQL execution failed: " + sqlQuery, e);
        }
    }

    @Override
    public void createTempView(String viewName, LogicalPlan plan) {
        try {
            String sql = translator.translate(plan);
            String createView = String.format(
                "CREATE OR REPLACE TEMPORARY VIEW \"%s\" AS %s",
                viewName.replace("\"", "\"\""),
                sql
            );

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createView);
                tempViews.put(viewName, createView);
                LOG.debug("Created temp view: {}", viewName);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to create temp view: " + viewName, e);
        }
    }

    @Override
    public void dropTempView(String viewName) {
        try {
            String dropView = String.format(
                "DROP VIEW IF EXISTS \"%s\"",
                viewName.replace("\"", "\"\"")
            );

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropView);
                tempViews.remove(viewName);
                LOG.debug("Dropped temp view: {}", viewName);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop temp view: " + viewName, e);
        }
    }

    @Override
    public Dataset<Row> getTable(String tableName, SparkSession session) {
        // Create a table scan plan
        LogicalPlan plan = new com.spark2sql.plan.nodes.TableScan(tableName);
        return createDatasetWithPlan(session, plan);
    }

    @Override
    public boolean tableExists(String tableName) {
        try (Statement stmt = connection.createStatement()) {
            String query = String.format(
                "SELECT 1 FROM information_schema.tables WHERE table_name = '%s' LIMIT 1",
                tableName.replace("'", "''")
            );
            ResultSet rs = stmt.executeQuery(query);
            return rs.next();
        } catch (SQLException e) {
            LOG.warn("Failed to check table existence: {}", tableName, e);
            return false;
        }
    }

    @Override
    public List<String> listTables() {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'main' ORDER BY table_name"
            );
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        } catch (SQLException e) {
            LOG.error("Failed to list tables", e);
            throw new RuntimeException("Failed to list tables", e);
        }
        return tables;
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                LOG.info("DuckDB connection closed");
            }
        } catch (SQLException e) {
            LOG.error("Failed to close DuckDB connection", e);
            throw new RuntimeException("Failed to close DuckDB connection", e);
        }
    }

    private List<Row> executeSQL(String sql, StructType expectedSchema) throws SQLException {
        List<Row> rows = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Use expected schema if provided, otherwise infer
            StructType schema = expectedSchema != null ? expectedSchema : inferSchema(metaData);

            while (rs.next()) {
                Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = extractValue(rs, i + 1, schema.fields()[i]);
                }
                rows.add(new GenericRowWithSchema(values, schema));
            }
        }

        LOG.debug("Query returned {} rows", rows.size());
        return rows;
    }

    private List<Row> executeSQLDirect(String sql) throws SQLException {
        List<Row> rows = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            StructType schema = inferSchema(metaData);

            while (rs.next()) {
                Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = extractValueDynamic(rs, i + 1);
                }
                rows.add(new GenericRowWithSchema(values, schema));
            }
        }

        return rows;
    }

    private Object extractValue(ResultSet rs, int columnIndex, StructField field) throws SQLException {
        if (rs.getObject(columnIndex) == null) {
            return null;
        }

        DataType dataType = field.dataType();

        if (dataType instanceof IntegerType) {
            return rs.getInt(columnIndex);
        } else if (dataType instanceof LongType) {
            return rs.getLong(columnIndex);
        } else if (dataType instanceof ShortType) {
            return rs.getShort(columnIndex);
        } else if (dataType instanceof ByteType) {
            return rs.getByte(columnIndex);
        } else if (dataType instanceof FloatType) {
            return rs.getFloat(columnIndex);
        } else if (dataType instanceof DoubleType) {
            return rs.getDouble(columnIndex);
        } else if (dataType instanceof DecimalType) {
            BigDecimal bd = rs.getBigDecimal(columnIndex);
            if (bd != null) {
                DecimalType dt = (DecimalType) dataType;
                return Decimal.apply(bd, dt.precision(), dt.scale());
            }
            return null;
        } else if (dataType instanceof StringType) {
            return rs.getString(columnIndex);
        } else if (dataType instanceof BooleanType) {
            return rs.getBoolean(columnIndex);
        } else if (dataType instanceof DateType) {
            Date sqlDate = rs.getDate(columnIndex);
            return sqlDate != null ? sqlDate.toLocalDate() : null;
        } else if (dataType instanceof TimestampType) {
            Timestamp ts = rs.getTimestamp(columnIndex);
            return ts != null ? ts.toInstant() : null;
        } else if (dataType instanceof BinaryType) {
            return rs.getBytes(columnIndex);
        } else {
            // Default to object
            return rs.getObject(columnIndex);
        }
    }

    private Object extractValueDynamic(ResultSet rs, int columnIndex) throws SQLException {
        // Extract value without known schema
        Object value = rs.getObject(columnIndex);
        if (value == null) {
            return null;
        }

        // Convert JDBC types to Spark-compatible types
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        } else if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant();
        } else if (value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;
            return Decimal.apply(bd);
        }

        return value;
    }

    private StructType inferSchema(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        StructField[] fields = new StructField[columnCount];

        for (int i = 0; i < columnCount; i++) {
            String columnName = metaData.getColumnLabel(i + 1);
            int sqlType = metaData.getColumnType(i + 1);
            boolean nullable = metaData.isNullable(i + 1) != ResultSetMetaData.columnNoNulls;

            DataType dataType = mapSQLTypeToSpark(sqlType, metaData, i + 1);
            fields[i] = new StructField(columnName, dataType, nullable, Metadata.empty());
        }

        return new StructType(fields);
    }

    private StructType inferSchema(Row firstRow) {
        if (firstRow == null) {
            return new StructType();
        }

        StructField[] fields = new StructField[firstRow.length()];
        for (int i = 0; i < firstRow.length(); i++) {
            Object value = firstRow.get(i);
            DataType dataType = inferDataType(value);
            fields[i] = new StructField("col" + i, dataType, true, Metadata.empty());
        }

        return new StructType(fields);
    }

    private DataType inferDataType(Object value) {
        if (value == null) {
            return DataTypes.StringType;  // Default for null
        } else if (value instanceof Integer) {
            return DataTypes.IntegerType;
        } else if (value instanceof Long) {
            return DataTypes.LongType;
        } else if (value instanceof Double) {
            return DataTypes.DoubleType;
        } else if (value instanceof Float) {
            return DataTypes.FloatType;
        } else if (value instanceof BigDecimal || value instanceof Decimal) {
            return DataTypes.createDecimalType();
        } else if (value instanceof Boolean) {
            return DataTypes.BooleanType;
        } else if (value instanceof java.time.LocalDate) {
            return DataTypes.DateType;
        } else if (value instanceof java.time.Instant) {
            return DataTypes.TimestampType;
        } else if (value instanceof byte[]) {
            return DataTypes.BinaryType;
        } else {
            return DataTypes.StringType;
        }
    }

    private DataType mapSQLTypeToSpark(int sqlType, ResultSetMetaData metaData, int column)
            throws SQLException {
        switch (sqlType) {
            case Types.INTEGER:
                return DataTypes.IntegerType;
            case Types.BIGINT:
                return DataTypes.LongType;
            case Types.SMALLINT:
                return DataTypes.ShortType;
            case Types.TINYINT:
                return DataTypes.ByteType;
            case Types.REAL:
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.DOUBLE:
                return DataTypes.DoubleType;
            case Types.DECIMAL:
            case Types.NUMERIC:
                int precision = metaData.getPrecision(column);
                int scale = metaData.getScale(column);
                return DataTypes.createDecimalType(precision, scale);
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return DataTypes.StringType;
            case Types.BOOLEAN:
            case Types.BIT:
                return DataTypes.BooleanType;
            case Types.DATE:
                return DataTypes.DateType;
            case Types.TIMESTAMP:
                return DataTypes.TimestampType;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return DataTypes.BinaryType;
            default:
                return DataTypes.StringType;
        }
    }

    // Helper methods to create Dataset without directly using RowEncoder
    @SuppressWarnings("unchecked")
    private Dataset<Row> createEmptyDataset(SparkSession session) {
        try {
            // Use reflection to create Dataset without directly referencing RowEncoder
            LogicalPlan plan = new com.spark2sql.plan.nodes.LocalRelation(new ArrayList<>(), new StructType());
            Class<?> datasetClass = Dataset.class;
            Class<?> encoderClass = Class.forName("org.apache.spark.sql.Encoder");
            Class<?> rowEncoderClass = Class.forName("org.apache.spark.sql.RowEncoder");

            java.lang.reflect.Method applyMethod = rowEncoderClass.getMethod("apply", StructType.class);
            Object encoder = applyMethod.invoke(null, new StructType());

            java.lang.reflect.Constructor<?> constructor = datasetClass.getConstructor(
                SparkSession.class, LogicalPlan.class, encoderClass);
            return (Dataset<Row>) constructor.newInstance(session, plan, encoder);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create empty dataset", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Dataset<Row> createDatasetWithPlan(SparkSession session, LogicalPlan plan) {
        try {
            // Use reflection to create Dataset without directly referencing RowEncoder
            Class<?> datasetClass = Dataset.class;
            Class<?> encoderClass = Class.forName("org.apache.spark.sql.Encoder");
            Class<?> rowEncoderClass = Class.forName("org.apache.spark.sql.RowEncoder");

            java.lang.reflect.Method applyMethod = rowEncoderClass.getMethod("apply", StructType.class);
            Object encoder = applyMethod.invoke(null, plan.schema());

            java.lang.reflect.Constructor<?> constructor = datasetClass.getConstructor(
                SparkSession.class, LogicalPlan.class, encoderClass);
            return (Dataset<Row>) constructor.newInstance(session, plan, encoder);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create dataset", e);
        }
    }
}