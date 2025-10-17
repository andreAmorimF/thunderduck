#!/usr/bin/env python3

from pyspark.sql import SparkSession
import sys

def test_spark_connect():
    try:
        # Create Spark session
        print("Connecting to Spark Connect server...")
        spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
        print("✓ Connected successfully")

        # Test 1: Simple SQL with df.show()
        print("\nTest 1: SELECT 1 AS col")
        df = spark.sql("SELECT 1 AS col")
        df.show()

        # Test 2: Multiple columns
        print("\nTest 2: Multiple columns")
        df2 = spark.sql("SELECT 42 AS answer, 'hello' AS greeting")
        df2.show()

        # Test 3: Multiple rows
        print("\nTest 3: Multiple rows")
        df3 = spark.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
        df3.show()

        # Test 4: Truncation test
        print("\nTest 4: Truncation (should truncate long strings)")
        df4 = spark.sql("SELECT 'This is a very long string that should be truncated' AS long_text")
        df4.show(truncate=10)

        # Test 5: Vertical display
        print("\nTest 5: Vertical display")
        df5 = spark.sql("SELECT 1 AS id, 'test' AS name")
        df5.show(vertical=True)

        print("\n✅ All tests passed!")
        spark.stop()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_spark_connect()