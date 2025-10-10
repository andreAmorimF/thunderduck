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
 * Tests for string operations and functions.
 */
public class StringOperationsTest extends DifferentialTestFramework {

    @Test
    public void testStringComparisons() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("apple", "banana"),
            RowFactory.create("banana", "apple"),
            RowFactory.create("cherry", "cherry"),
            RowFactory.create("", "empty"),
            RowFactory.create("APPLE", "apple"),
            RowFactory.create(null, "null_test"),
            RowFactory.create("null_test", null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str1", DataTypes.StringType, true),
            DataTypes.createStructField("str2", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test contains
        assertSameResults(
            df.spark.filter(df.spark.col("str1").contains("app")),
            df.embedded.filter(df.embedded.col("str1").contains("app")),
            false
        );

        // Test startsWith
        assertSameResults(
            df.spark.filter(df.spark.col("str1").startsWith("ban")),
            df.embedded.filter(df.embedded.col("str1").startsWith("ban")),
            false
        );

        // Test endsWith
        assertSameResults(
            df.spark.filter(df.spark.col("str1").endsWith("erry")),
            df.embedded.filter(df.embedded.col("str1").endsWith("erry")),
            false
        );
    }

    @Test
    public void testStringPatternMatching() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("hello world"),
            RowFactory.create("hello spark"),
            RowFactory.create("goodbye world"),
            RowFactory.create("HELLO WORLD"),
            RowFactory.create("123-456-7890"),
            RowFactory.create("abc@def.com"),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test LIKE pattern
        assertSameResults(
            df.spark.filter(df.spark.col("text").like("hello%")),
            df.embedded.filter(df.embedded.col("text").like("hello%")),
            false
        );

        // Test LIKE with underscore wildcard
        assertSameResults(
            df.spark.filter(df.spark.col("text").like("h_llo%")),
            df.embedded.filter(df.embedded.col("text").like("h_llo%")),
            false
        );

        // Test RLIKE (regex)
        assertSameResults(
            df.spark.filter(df.spark.col("text").rlike("^hello.*")),
            df.embedded.filter(df.embedded.col("text").rlike("^hello.*")),
            false
        );

        // Test RLIKE with digit pattern
        assertSameResults(
            df.spark.filter(df.spark.col("text").rlike("\\d{3}-\\d{3}-\\d{4}")),
            df.embedded.filter(df.embedded.col("text").rlike("\\d{3}-\\d{3}-\\d{4}")),
            false
        );
    }

    @Test
    public void testStringTransformations() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("Hello World"),
            RowFactory.create("UPPER CASE"),
            RowFactory.create("lower case"),
            RowFactory.create("  spaces  "),
            RowFactory.create("\ttabs\t"),
            RowFactory.create("MiXeD CaSe"),
            RowFactory.create(""),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test upper
        assertSameResults(
            df.spark.select(functions.upper(df.spark.col("text")).as("upper_text")),
            df.embedded.select(functions.upper(df.embedded.col("text")).as("upper_text"))
        );

        // Test lower
        assertSameResults(
            df.spark.select(functions.lower(df.spark.col("text")).as("lower_text")),
            df.embedded.select(functions.lower(df.embedded.col("text")).as("lower_text"))
        );

        // Test trim
        assertSameResults(
            df.spark.select(functions.trim(df.spark.col("text")).as("trimmed")),
            df.embedded.select(functions.trim(df.embedded.col("text")).as("trimmed"))
        );

        // Test ltrim
        assertSameResults(
            df.spark.select(functions.ltrim(df.spark.col("text")).as("ltrimmed")),
            df.embedded.select(functions.ltrim(df.embedded.col("text")).as("ltrimmed"))
        );

        // Test rtrim
        assertSameResults(
            df.spark.select(functions.rtrim(df.spark.col("text")).as("rtrimmed")),
            df.embedded.select(functions.rtrim(df.embedded.col("text")).as("rtrimmed"))
        );
    }

    @Test
    public void testStringLength() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("hello"),
            RowFactory.create(""),
            RowFactory.create("a"),
            RowFactory.create("hello world"),
            RowFactory.create("Unicode: 你好"),
            RowFactory.create("   spaces   "),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test length function
        assertSameResults(
            df.spark.select(
                df.spark.col("text"),
                functions.length(df.spark.col("text")).as("len")
            ),
            df.embedded.select(
                df.embedded.col("text"),
                functions.length(df.embedded.col("text")).as("len")
            )
        );
    }

    @Test
    public void testSubstring() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("Hello World"),
            RowFactory.create("Apache Spark"),
            RowFactory.create("Short"),
            RowFactory.create(""),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test substring with literals
        assertSameResults(
            df.spark.select(df.spark.col("text").substr(1, 5).as("substr")),
            df.embedded.select(df.embedded.col("text").substr(1, 5).as("substr"))
        );

        // Test substring with different positions
        assertSameResults(
            df.spark.select(df.spark.col("text").substr(7, 5).as("substr")),
            df.embedded.select(df.embedded.col("text").substr(7, 5).as("substr"))
        );

        // Test substring beyond string length
        assertSameResults(
            df.spark.select(df.spark.col("text").substr(1, 100).as("substr")),
            df.embedded.select(df.embedded.col("text").substr(1, 100).as("substr"))
        );
    }

    @Test
    public void testInOperator() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("apple"),
            RowFactory.create("banana"),
            RowFactory.create("cherry"),
            RowFactory.create("date"),
            RowFactory.create("elderberry"),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("fruit", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test isin with multiple values
        assertSameResults(
            df.spark.filter(df.spark.col("fruit").isin("apple", "banana", "cherry")),
            df.embedded.filter(df.embedded.col("fruit").isin("apple", "banana", "cherry")),
            false
        );

        // Test isin with single value
        assertSameResults(
            df.spark.filter(df.spark.col("fruit").isin("apple")),
            df.embedded.filter(df.embedded.col("fruit").isin("apple")),
            false
        );

        // Test isin with no matches
        assertSameResults(
            df.spark.filter(df.spark.col("fruit").isin("mango", "papaya")),
            df.embedded.filter(df.embedded.col("fruit").isin("mango", "papaya")),
            false
        );
    }

    @Test
    public void testStringConcatenation() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("Hello", "World"),
            RowFactory.create("Apache", "Spark"),
            RowFactory.create("", "Empty"),
            RowFactory.create("Null", null),
            RowFactory.create(null, "Test"),
            RowFactory.create(null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("str1", DataTypes.StringType, true),
            DataTypes.createStructField("str2", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test concatenation using SQL expression
        assertSameResults(
            df.spark.selectExpr("concat(str1, str2) as concatenated"),
            df.embedded.selectExpr("concat(str1, str2) as concatenated")
        );

        // Test concatenation with separator
        assertSameResults(
            df.spark.selectExpr("concat(str1, ' ', str2) as concatenated"),
            df.embedded.selectExpr("concat(str1, ' ', str2) as concatenated")
        );
    }

    @Test
    public void testStringReplace() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("hello world"),
            RowFactory.create("foo bar foo"),
            RowFactory.create("no match here"),
            RowFactory.create(""),
            RowFactory.create(null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test replace using SQL
        assertSameResults(
            df.spark.selectExpr("replace(text, 'foo', 'baz') as replaced"),
            df.embedded.selectExpr("replace(text, 'foo', 'baz') as replaced")
        );

        // Test replace with empty string
        assertSameResults(
            df.spark.selectExpr("replace(text, 'world', '') as replaced"),
            df.embedded.selectExpr("replace(text, 'world', '') as replaced")
        );
    }

    @Test
    public void testStringWithSpecialCharacters() {
        List<Row> testData = Arrays.asList(
            RowFactory.create("hello\nworld"),     // newline
            RowFactory.create("tab\there"),        // tab
            RowFactory.create("quote's"),          // single quote
            RowFactory.create("double\"quote"),    // double quote
            RowFactory.create("back\\slash"),      // backslash
            RowFactory.create("emoji 😀"),         // emoji
            RowFactory.create("unicode 你好"),      // unicode
            RowFactory.create("null\0char")        // null character
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test handling of special characters
        assertSameResults(
            df.spark.select("text"),
            df.embedded.select("text")
        );

        // Test operations on special characters
        assertSameResults(
            df.spark.select(functions.length(df.spark.col("text")).as("len")),
            df.embedded.select(functions.length(df.embedded.col("text")).as("len"))
        );
    }
}