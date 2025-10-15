package com.catalyst2sql.differential.datagen;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates synthetic test data for differential testing.
 *
 * <p>Features:
 * <ul>
 *   <li>Deterministic generation with fixed seeds</li>
 *   <li>Support for all primitive types</li>
 *   <li>Configurable null percentage</li>
 *   <li>Various value distributions</li>
 * </ul>
 */
public class SyntheticDataGenerator {

    private final Random random;
    private final long seed;

    public SyntheticDataGenerator() {
        this(12345L);
    }

    public SyntheticDataGenerator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    /**
     * Generate a simple test dataset with ID, name, and value columns.
     */
    public Dataset<Row> generateSimpleDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                          // id
                    "name_" + i,                                // name
                    random.nextDouble() * 100.0                 // value
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with all primitive types.
     */
    public Dataset<Row> generateAllTypesDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                          // id (int)
                    (long) i * 1000,                            // long_val
                    (short) (i % 100),                          // short_val
                    (byte) (i % 10),                            // byte_val
                    random.nextFloat(),                         // float_val
                    random.nextDouble(),                        // double_val
                    "string_" + i,                              // string_val
                    i % 2 == 0                                  // boolean_val
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("long_val", DataTypes.LongType, false),
                DataTypes.createStructField("short_val", DataTypes.ShortType, false),
                DataTypes.createStructField("byte_val", DataTypes.ByteType, false),
                DataTypes.createStructField("float_val", DataTypes.FloatType, false),
                DataTypes.createStructField("double_val", DataTypes.DoubleType, false),
                DataTypes.createStructField("string_val", DataTypes.StringType, false),
                DataTypes.createStructField("boolean_val", DataTypes.BooleanType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with nullable columns and null values.
     */
    public Dataset<Row> generateNullableDataset(SparkSession spark, int rowCount, double nullPercentage) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            Integer id = i;
            String name = random.nextDouble() < nullPercentage ? null : "name_" + i;
            Double value = random.nextDouble() < nullPercentage ? null : random.nextDouble() * 100.0;

            rows.add(RowFactory.create(id, name, value));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.DoubleType, true)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset for join operations.
     */
    public Dataset<Row> generateJoinDataset(SparkSession spark, int rowCount, String prefix) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed + prefix.hashCode()); // Different seed per table

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,
                    prefix + "_" + i,
                    random.nextInt(100)
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with specific value ranges.
     */
    public Dataset<Row> generateRangeDataset(SparkSession spark, int rowCount, int minValue, int maxValue) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            int value = minValue + random.nextInt(maxValue - minValue + 1);
            rows.add(RowFactory.create(i, value));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with duplicate values for aggregate testing.
     */
    public Dataset<Row> generateGroupByDataset(SparkSession spark, int rowCount, int numGroups) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            String group = "group_" + (i % numGroups);
            rows.add(RowFactory.create(
                    i,
                    group,
                    random.nextInt(1000)
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("category", DataTypes.StringType, false),
                DataTypes.createStructField("amount", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate time-series dataset for window function tests.
     */
    public Dataset<Row> generateTimeSeriesDataset(SparkSession spark, int rowCount, int numPartitions) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            String partition = "partition_" + (i % numPartitions);
            rows.add(RowFactory.create(
                    i,                                      // id
                    partition,                              // partition_key
                    i,                                      // sequence_num
                    random.nextDouble() * 100.0,            // value
                    random.nextInt(100)                     // rank_value
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("partition_key", DataTypes.StringType, false),
                DataTypes.createStructField("sequence_num", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false),
                DataTypes.createStructField("rank_value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate multi-table scenario for subquery/join tests.
     */
    public Dataset<Row> generateOrdersDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            int customerId = i % (rowCount / 2); // Many orders per customer
            rows.add(RowFactory.create(
                    i,                                      // order_id
                    customerId,                             // customer_id
                    random.nextDouble() * 1000.0,           // amount
                    "product_" + (i % 5)                    // product
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("order_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("customer_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("amount", DataTypes.DoubleType, false),
                DataTypes.createStructField("product", DataTypes.StringType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate customers dataset for subquery/join tests.
     */
    public Dataset<Row> generateCustomersDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed + 1); // Different seed for different table

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                      // customer_id
                    "customer_" + i,                        // customer_name
                    "city_" + (i % 5),                      // city
                    random.nextInt(100)                     // age
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("customer_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("customer_name", DataTypes.StringType, false),
                DataTypes.createStructField("city", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with statistical distributions for advanced aggregates.
     */
    public Dataset<Row> generateStatisticalDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            // Generate normal distribution values
            double normalValue = random.nextGaussian() * 15 + 50; // mean=50, stddev=15

            rows.add(RowFactory.create(
                    i,                                      // id
                    "group_" + (i % 3),                     // group_key
                    normalValue,                            // value
                    random.nextInt(100)                     // int_value
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("group_key", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false),
                DataTypes.createStructField("int_value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with date/timestamp data.
     */
    public Dataset<Row> generateDateTimeDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        // Base date: 2024-01-01
        long baseTimestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC

        for (int i = 0; i < rowCount; i++) {
            long timestamp = baseTimestamp + (i * 86400000L); // Add i days
            java.sql.Date date = new java.sql.Date(timestamp);
            java.sql.Timestamp ts = new java.sql.Timestamp(timestamp);

            rows.add(RowFactory.create(
                    i,                                      // id
                    date,                                   // date_val
                    ts,                                     // timestamp_val
                    random.nextDouble() * 100.0             // value
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("date_val", DataTypes.DateType, false),
                DataTypes.createStructField("timestamp_val", DataTypes.TimestampType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset for set operations (UNION, INTERSECT, EXCEPT).
     */
    public Dataset<Row> generateSetDataset(SparkSession spark, int rowCount, String prefix) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed + prefix.hashCode()); // Different seed per set

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                      // id
                    prefix + "_value_" + i,                 // name
                    random.nextInt(50)                      // category (overlap possible)
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("category", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }
}
