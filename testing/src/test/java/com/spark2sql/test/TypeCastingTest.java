package com.spark2sql.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for type casting and conversion operations.
 */
public class TypeCastingTest extends DifferentialTestFramework {

    @Test
    public void testNumericTypeCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("123", 456.789, true),
            RowFactory.create("45.67", 100.0, false),
            RowFactory.create("-123", -456.789, true),
            RowFactory.create("0", 0.0, false),
            RowFactory.create("not_a_number", 123.456, true),
            RowFactory.create(null, null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str_num", DataTypes.StringType, true),
            DataTypes.createStructField("double_num", DataTypes.DoubleType, true),
            DataTypes.createStructField("bool_val", DataTypes.BooleanType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test string to integer casting
        assertSameResults(
            df.spark.select(
                df.spark.col("str_num"),
                df.spark.col("str_num").cast(DataTypes.IntegerType).as("str_to_int")
            ),
            df.embedded.select(
                df.embedded.col("str_num"),
                df.embedded.col("str_num").cast(DataTypes.IntegerType).as("str_to_int")
            )
        );

        // Test string to double casting
        assertSameResults(
            df.spark.select(
                df.spark.col("str_num"),
                df.spark.col("str_num").cast(DataTypes.DoubleType).as("str_to_double")
            ),
            df.embedded.select(
                df.embedded.col("str_num"),
                df.embedded.col("str_num").cast(DataTypes.DoubleType).as("str_to_double")
            )
        );

        // Test double to integer casting (truncation)
        assertSameResults(
            df.spark.select(
                df.spark.col("double_num"),
                df.spark.col("double_num").cast(DataTypes.IntegerType).as("double_to_int")
            ),
            df.embedded.select(
                df.embedded.col("double_num"),
                df.embedded.col("double_num").cast(DataTypes.IntegerType).as("double_to_int")
            )
        );

        // Test boolean to integer casting
        assertSameResults(
            df.spark.select(
                df.spark.col("bool_val"),
                df.spark.col("bool_val").cast(DataTypes.IntegerType).as("bool_to_int")
            ),
            df.embedded.select(
                df.embedded.col("bool_val"),
                df.embedded.col("bool_val").cast(DataTypes.IntegerType).as("bool_to_int")
            )
        );
    }

    @Test
    public void testStringTypeCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(123, 456.789, true),
            RowFactory.create(0, 0.0, false),
            RowFactory.create(-456, -123.456, true),
            RowFactory.create(Integer.MAX_VALUE, Double.NaN, false),
            RowFactory.create(null, null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("int_val", DataTypes.IntegerType, true),
            DataTypes.createStructField("double_val", DataTypes.DoubleType, true),
            DataTypes.createStructField("bool_val", DataTypes.BooleanType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test integer to string casting
        assertSameResults(
            df.spark.select(
                df.spark.col("int_val"),
                df.spark.col("int_val").cast(DataTypes.StringType).as("int_to_str")
            ),
            df.embedded.select(
                df.embedded.col("int_val"),
                df.embedded.col("int_val").cast(DataTypes.StringType).as("int_to_str")
            )
        );

        // Test double to string casting
        assertSameResults(
            df.spark.select(
                df.spark.col("double_val"),
                df.spark.col("double_val").cast(DataTypes.StringType).as("double_to_str")
            ),
            df.embedded.select(
                df.embedded.col("double_val"),
                df.embedded.col("double_val").cast(DataTypes.StringType).as("double_to_str")
            )
        );

        // Test boolean to string casting
        assertSameResults(
            df.spark.select(
                df.spark.col("bool_val"),
                df.spark.col("bool_val").cast(DataTypes.StringType).as("bool_to_str")
            ),
            df.embedded.select(
                df.embedded.col("bool_val"),
                df.embedded.col("bool_val").cast(DataTypes.StringType).as("bool_to_str")
            )
        );
    }

    @Test
    public void testBooleanTypeCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("true", 1, 1.0),
            RowFactory.create("false", 0, 0.0),
            RowFactory.create("TRUE", -1, -1.0),
            RowFactory.create("FALSE", 100, 100.0),
            RowFactory.create("yes", 0, Double.NaN),
            RowFactory.create("", 0, 0.0),
            RowFactory.create(null, null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str_val", DataTypes.StringType, true),
            DataTypes.createStructField("int_val", DataTypes.IntegerType, true),
            DataTypes.createStructField("double_val", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test string to boolean casting
        assertSameResults(
            df.spark.select(
                df.spark.col("str_val"),
                df.spark.col("str_val").cast(DataTypes.BooleanType).as("str_to_bool")
            ),
            df.embedded.select(
                df.embedded.col("str_val"),
                df.embedded.col("str_val").cast(DataTypes.BooleanType).as("str_to_bool")
            )
        );

        // Test integer to boolean casting (0 = false, non-zero = true)
        assertSameResults(
            df.spark.select(
                df.spark.col("int_val"),
                df.spark.col("int_val").cast(DataTypes.BooleanType).as("int_to_bool")
            ),
            df.embedded.select(
                df.embedded.col("int_val"),
                df.embedded.col("int_val").cast(DataTypes.BooleanType).as("int_to_bool")
            )
        );

        // Test double to boolean casting
        assertSameResults(
            df.spark.select(
                df.spark.col("double_val"),
                df.spark.col("double_val").cast(DataTypes.BooleanType).as("double_to_bool")
            ),
            df.embedded.select(
                df.embedded.col("double_val"),
                df.embedded.col("double_val").cast(DataTypes.BooleanType).as("double_to_bool")
            )
        );
    }

    @Test
    public void testDecimalTypeCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("123.45", 678.90),
            RowFactory.create("999999.99", 1234567.89),
            RowFactory.create("-123.45", -678.90),
            RowFactory.create("0.00001", 0.00001),
            RowFactory.create("invalid", 123.45),
            RowFactory.create(null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str_decimal", DataTypes.StringType, true),
            DataTypes.createStructField("double_val", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test string to decimal casting
        DecimalType decimalType = DataTypes.createDecimalType(10, 2);
        assertSameResults(
            df.spark.select(
                df.spark.col("str_decimal"),
                df.spark.col("str_decimal").cast(decimalType).as("str_to_decimal")
            ),
            df.embedded.select(
                df.embedded.col("str_decimal"),
                df.embedded.col("str_decimal").cast(decimalType).as("str_to_decimal")
            )
        );

        // Test double to decimal casting
        assertSameResults(
            df.spark.select(
                df.spark.col("double_val"),
                df.spark.col("double_val").cast(decimalType).as("double_to_decimal")
            ),
            df.embedded.select(
                df.embedded.col("double_val"),
                df.embedded.col("double_val").cast(decimalType).as("double_to_decimal")
            )
        );
    }

    @Test
    public void testCastingWithStringFormat() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(123),
            RowFactory.create(456),
            RowFactory.create(-789),
            RowFactory.create(0),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("value", DataTypes.IntegerType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test cast using string format
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                df.spark.col("value").cast("double").as("to_double"),
                df.spark.col("value").cast("string").as("to_string")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                df.embedded.col("value").cast("double").as("to_double"),
                df.embedded.col("value").cast("string").as("to_string")
            )
        );
    }

    @Test
    public void testLossyCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(Long.MAX_VALUE),
            RowFactory.create(Long.MIN_VALUE),
            RowFactory.create((long) Integer.MAX_VALUE + 1),
            RowFactory.create((long) Integer.MIN_VALUE - 1),
            RowFactory.create(0L),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("long_val", DataTypes.LongType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test lossy long to int casting (overflow)
        assertSameResults(
            df.spark.select(
                df.spark.col("long_val"),
                df.spark.col("long_val").cast(DataTypes.IntegerType).as("long_to_int")
            ),
            df.embedded.select(
                df.embedded.col("long_val"),
                df.embedded.col("long_val").cast(DataTypes.IntegerType).as("long_to_int")
            )
        );

        // Test long to float casting (precision loss)
        assertSameResults(
            df.spark.select(
                df.spark.col("long_val"),
                df.spark.col("long_val").cast(DataTypes.FloatType).as("long_to_float")
            ),
            df.embedded.select(
                df.embedded.col("long_val"),
                df.embedded.col("long_val").cast(DataTypes.FloatType).as("long_to_float")
            )
        );
    }

    @Test
    public void testCastingInExpressions() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("10", "20"),
            RowFactory.create("5.5", "2.5"),
            RowFactory.create("100", "3"),
            RowFactory.create(null, "10"),
            RowFactory.create("10", null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str1", DataTypes.StringType, true),
            DataTypes.createStructField("str2", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test casting within arithmetic expressions
        assertSameResults(
            df.spark.selectExpr(
                "cast(str1 as int) + cast(str2 as int) as int_sum",
                "cast(str1 as double) * cast(str2 as double) as double_product"
            ),
            df.embedded.selectExpr(
                "cast(str1 as int) + cast(str2 as int) as int_sum",
                "cast(str1 as double) * cast(str2 as double) as double_product"
            )
        );

        // Test casting in conditions
        assertSameResults(
            df.spark.selectExpr(
                "CASE WHEN cast(str1 as int) > 10 THEN 'large' ELSE 'small' END as category"
            ),
            df.embedded.selectExpr(
                "CASE WHEN cast(str1 as int) > 10 THEN 'large' ELSE 'small' END as category"
            )
        );
    }

    @Test
    public void testInvalidCasting() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("not_a_number"),
            RowFactory.create("12.34.56"),  // Invalid number format
            RowFactory.create("1e308"),      // Very large number
            RowFactory.create("infinity"),
            RowFactory.create("")
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("invalid_str", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test invalid string to number casting (should return null)
        assertSameResults(
            df.spark.select(
                df.spark.col("invalid_str"),
                df.spark.col("invalid_str").cast(DataTypes.IntegerType).as("to_int"),
                df.spark.col("invalid_str").cast(DataTypes.DoubleType).as("to_double")
            ),
            df.embedded.select(
                df.embedded.col("invalid_str"),
                df.embedded.col("invalid_str").cast(DataTypes.IntegerType).as("to_int"),
                df.embedded.col("invalid_str").cast(DataTypes.DoubleType).as("to_double")
            )
        );
    }
}