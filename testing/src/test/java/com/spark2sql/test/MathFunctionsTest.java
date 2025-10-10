package com.spark2sql.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for mathematical functions and operations.
 */
public class MathFunctionsTest extends DifferentialTestFramework {

    @Test
    public void testBasicMathFunctions() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(-10.5, 3.7),
            RowFactory.create(0.0, 0.0),
            RowFactory.create(5.5, -2.3),
            RowFactory.create(-3.14159, 2.71828),
            RowFactory.create(100.0, 10.0),
            RowFactory.create(Double.NaN, 1.0),
            RowFactory.create(Double.POSITIVE_INFINITY, 1.0),
            RowFactory.create(Double.NEGATIVE_INFINITY, 1.0),
            RowFactory.create(null, 1.0),
            RowFactory.create(1.0, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("x", DataTypes.DoubleType, true),
            DataTypes.createStructField("y", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test abs
        assertSameResults(
            df.spark.select(
                df.spark.col("x"),
                functions.abs(df.spark.col("x")).as("abs_x")
            ),
            df.embedded.select(
                df.embedded.col("x"),
                functions.abs(df.embedded.col("x")).as("abs_x")
            )
        );

        // Test sqrt
        assertSameResults(
            df.spark.select(
                df.spark.col("x"),
                functions.sqrt(functions.abs(df.spark.col("x"))).as("sqrt_abs_x")
            ),
            df.embedded.select(
                df.embedded.col("x"),
                functions.sqrt(functions.abs(df.embedded.col("x"))).as("sqrt_abs_x")
            )
        );
    }

    @Test
    public void testRoundingFunctions() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(3.14159),
            RowFactory.create(2.5),
            RowFactory.create(2.4),
            RowFactory.create(-2.5),
            RowFactory.create(-2.4),
            RowFactory.create(0.0),
            RowFactory.create(10.999),
            RowFactory.create(-10.999),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("value", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test ceil
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                functions.ceil(df.spark.col("value")).as("ceiling")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                functions.ceil(df.embedded.col("value")).as("ceiling")
            )
        );

        // Test floor
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                functions.floor(df.spark.col("value")).as("floor")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                functions.floor(df.embedded.col("value")).as("floor")
            )
        );

        // Test round without scale
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                functions.round(df.spark.col("value")).as("rounded")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                functions.round(df.embedded.col("value")).as("rounded")
            )
        );

        // Test round with scale
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                functions.round(df.spark.col("value"), 2).as("rounded_2")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                functions.round(df.embedded.col("value"), 2).as("rounded_2")
            )
        );
    }

    @Test
    public void testArithmeticCombinations() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(10, 3, 2),
            RowFactory.create(5, 2, 3),
            RowFactory.create(-10, 3, -2),
            RowFactory.create(0, 5, 1),
            RowFactory.create(100, 0, 10),  // Division by zero case
            RowFactory.create(7, 3, 2),
            RowFactory.create(null, 2, 3),
            RowFactory.create(2, null, 3),
            RowFactory.create(2, 3, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, true),
            DataTypes.createStructField("b", DataTypes.IntegerType, true),
            DataTypes.createStructField("c", DataTypes.IntegerType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test complex arithmetic with column methods
        assertSameResults(
            df.spark.select(
                df.spark.col("a").plus(df.spark.col("b")).multiply(df.spark.col("c")).as("result")
            ),
            df.embedded.select(
                df.embedded.col("a").plus(df.embedded.col("b")).multiply(df.embedded.col("c")).as("result")
            )
        );

        // Test modulo operation
        assertSameResults(
            df.spark.select(
                df.spark.col("a").mod(df.spark.col("b")).as("modulo")
            ),
            df.embedded.select(
                df.embedded.col("a").mod(df.embedded.col("b")).as("modulo")
            )
        );

        // Test division
        assertSameResults(
            df.spark.select(
                df.spark.col("a").divide(df.spark.col("b")).as("division")
            ),
            df.embedded.select(
                df.embedded.col("a").divide(df.embedded.col("b")).as("division")
            )
        );
    }

    @Test
    public void testBitwiseOperations() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(5, 3),      // 101 & 011 = 001
            RowFactory.create(12, 10),    // 1100 & 1010 = 1000
            RowFactory.create(0, 255),
            RowFactory.create(-1, 1),
            RowFactory.create(null, 1),
            RowFactory.create(1, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("x", DataTypes.IntegerType, true),
            DataTypes.createStructField("y", DataTypes.IntegerType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test bitwise AND
        assertSameResults(
            df.spark.select(
                df.spark.col("x"),
                df.spark.col("y"),
                df.spark.col("x").bitwiseAND(df.spark.col("y")).as("and")
            ),
            df.embedded.select(
                df.embedded.col("x"),
                df.embedded.col("y"),
                df.embedded.col("x").bitwiseAND(df.embedded.col("y")).as("and")
            )
        );

        // Test bitwise OR
        assertSameResults(
            df.spark.select(
                df.spark.col("x"),
                df.spark.col("y"),
                df.spark.col("x").bitwiseOR(df.spark.col("y")).as("or")
            ),
            df.embedded.select(
                df.embedded.col("x"),
                df.embedded.col("y"),
                df.embedded.col("x").bitwiseOR(df.embedded.col("y")).as("or")
            )
        );

        // Test bitwise XOR
        assertSameResults(
            df.spark.select(
                df.spark.col("x"),
                df.spark.col("y"),
                df.spark.col("x").bitwiseXOR(df.spark.col("y")).as("xor")
            ),
            df.embedded.select(
                df.embedded.col("x"),
                df.embedded.col("y"),
                df.embedded.col("x").bitwiseXOR(df.embedded.col("y")).as("xor")
            )
        );
    }

    @Test
    public void testNaNHandling() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1.0),
            RowFactory.create(Double.NaN),
            RowFactory.create(Double.POSITIVE_INFINITY),
            RowFactory.create(Double.NEGATIVE_INFINITY),
            RowFactory.create(0.0),
            RowFactory.create(-0.0),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("value", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test isNaN
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                df.spark.col("value").isNaN().as("is_nan")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                df.embedded.col("value").isNaN().as("is_nan")
            )
        );

        // Test NaN in arithmetic
        assertSameResults(
            df.spark.select(
                df.spark.col("value"),
                df.spark.col("value").plus(1.0).as("plus_one"),
                df.spark.col("value").multiply(0.0).as("times_zero")
            ),
            df.embedded.select(
                df.embedded.col("value"),
                df.embedded.col("value").plus(1.0).as("plus_one"),
                df.embedded.col("value").multiply(0.0).as("times_zero")
            )
        );
    }

    @Test
    public void testMathWithDifferentNumericTypes() {
        List<Row> testData = Arrays.asList(
            RowFactory.create((byte) 10, (short) 100, 1000, 10000L, 3.14f, 2.71828),
            RowFactory.create((byte) -128, (short) -32768, Integer.MIN_VALUE, Long.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE),
            RowFactory.create((byte) 127, (short) 32767, Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE),
            RowFactory.create((byte) 0, (short) 0, 0, 0L, 0.0f, 0.0),
            RowFactory.create(null, null, null, null, null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("byte_col", DataTypes.ByteType, true),
            DataTypes.createStructField("short_col", DataTypes.ShortType, true),
            DataTypes.createStructField("int_col", DataTypes.IntegerType, true),
            DataTypes.createStructField("long_col", DataTypes.LongType, true),
            DataTypes.createStructField("float_col", DataTypes.FloatType, true),
            DataTypes.createStructField("double_col", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test mixed type arithmetic
        assertSameResults(
            df.spark.selectExpr(
                "byte_col + short_col as byte_short",
                "int_col + long_col as int_long",
                "float_col + double_col as float_double"
            ),
            df.embedded.selectExpr(
                "byte_col + short_col as byte_short",
                "int_col + long_col as int_long",
                "float_col + double_col as float_double"
            )
        );

        // Test type promotion
        assertSameResults(
            df.spark.selectExpr(
                "byte_col * 1000 as promoted",
                "int_col / 2.0 as int_to_double"
            ),
            df.embedded.selectExpr(
                "byte_col * 1000 as promoted",
                "int_col / 2.0 as int_to_double"
            )
        );
    }

    @Test
    public void testPowerAndLogarithms() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(2.0, 3.0),
            RowFactory.create(10.0, 2.0),
            RowFactory.create(Math.E, 1.0),
            RowFactory.create(1.0, 0.0),
            RowFactory.create(0.0, 1.0),
            RowFactory.create(-2.0, 2.0),  // Negative base with even exponent
            RowFactory.create(null, 2.0),
            RowFactory.create(2.0, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("base", DataTypes.DoubleType, true),
            DataTypes.createStructField("exp", DataTypes.DoubleType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test power operation using SQL
        assertSameResults(
            df.spark.selectExpr("pow(base, exp) as power"),
            df.embedded.selectExpr("pow(base, exp) as power")
        );

        // Test logarithm (when implemented)
        // This would test log, log10, log2, etc.
    }
}