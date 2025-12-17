"""
Integration tests for Lambda function expressions and higher-order functions.

Tests cover:
- Basic lambda syntax (transform, filter)
- Array HOFs (exists, forall, aggregate)
- Nested lambdas

NOTE: These tests use a combination of SQL for creating arrays (due to serialization
limitations with createDataFrame for complex types) and DataFrame API for lambda operations.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType, StructField


class TestTransformFunction:
    """Tests for transform (list_transform) higher-order function"""

    @pytest.mark.timeout(30)
    def test_transform_add_one(self, spark):
        """Test transform with simple addition using SQL-created array"""
        # Create a view with an actual array column
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.transform("arr", lambda x: x + 1).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [2, 3, 4]
        print("\n✓ transform(array, x -> x + 1) works")

    @pytest.mark.timeout(30)
    def test_transform_multiply(self, spark):
        """Test transform with multiplication"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.transform("arr", lambda x: x * 2).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [2, 4, 6, 8]
        print("\n✓ transform(array, x -> x * 2) works")

    @pytest.mark.timeout(30)
    def test_transform_from_subquery(self, spark):
        """Test transform using array() function in SQL"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30] AS numbers")
        df = spark.table("arr_view")
        result = df.select(F.transform("numbers", lambda x: x + 5).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [15, 25, 35]
        print("\n✓ transform from SQL-created array works")


class TestFilterFunction:
    """Tests for filter (list_filter) higher-order function"""

    @pytest.mark.timeout(30)
    def test_filter_greater_than(self, spark):
        """Test filter with greater than predicate"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4, 5] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.filter("arr", lambda x: x > 2).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [3, 4, 5]
        print("\n✓ filter(array, x -> x > 2) works")

    @pytest.mark.timeout(30)
    def test_filter_even_numbers(self, spark):
        """Test filter for even numbers"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4, 5, 6] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.filter("arr", lambda x: x % 2 == 0).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [2, 4, 6]
        print("\n✓ filter for even numbers works")

    @pytest.mark.timeout(30)
    def test_filter_all_pass(self, spark):
        """Test filter where all elements pass"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.filter("arr", lambda x: x > 5).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == [10, 20, 30]
        print("\n✓ filter where all pass works")

    @pytest.mark.timeout(30)
    def test_filter_none_pass(self, spark):
        """Test filter where no elements pass"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.filter("arr", lambda x: x > 10).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert list(rows[0]['result']) == []
        print("\n✓ filter where none pass works")


class TestExistsFunction:
    """Tests for exists (list_any) higher-order function"""

    @pytest.mark.timeout(30)
    def test_exists_true(self, spark):
        """Test exists returns true when condition matches"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4, 5] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.exists("arr", lambda x: x > 3).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == True
        print("\n✓ exists returns true correctly")

    @pytest.mark.timeout(30)
    def test_exists_false(self, spark):
        """Test exists returns false when no match"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.exists("arr", lambda x: x > 10).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == False
        print("\n✓ exists returns false correctly")


class TestForallFunction:
    """Tests for forall (list_all) higher-order function"""

    @pytest.mark.timeout(30)
    def test_forall_true(self, spark):
        """Test forall returns true when all match"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.forall("arr", lambda x: x > 5).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == True
        print("\n✓ forall returns true correctly")

    @pytest.mark.timeout(30)
    def test_forall_false(self, spark):
        """Test forall returns false when not all match"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4, 5] AS arr")
        df = spark.table("arr_view")
        result = df.select(F.forall("arr", lambda x: x > 2).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == False
        print("\n✓ forall returns false correctly")


class TestAggregateFunction:
    """Tests for aggregate (list_reduce) higher-order function"""

    @pytest.mark.timeout(30)
    def test_aggregate_sum(self, spark):
        """Test aggregate to compute sum"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4] AS arr")
        df = spark.table("arr_view")
        result = df.select(
            F.aggregate("arr", F.lit(0), lambda acc, x: acc + x).alias("result")
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == 10
        print("\n✓ aggregate sum works")

    @pytest.mark.timeout(30)
    def test_aggregate_product(self, spark):
        """Test aggregate to compute product"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4] AS arr")
        df = spark.table("arr_view")
        result = df.select(
            F.aggregate("arr", F.lit(1), lambda acc, x: acc * x).alias("result")
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == 24
        print("\n✓ aggregate product works")

    @pytest.mark.timeout(30)
    def test_aggregate_with_init(self, spark):
        """Test aggregate with non-zero initial value"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3] AS arr")
        df = spark.table("arr_view")
        result = df.select(
            F.aggregate("arr", F.lit(100), lambda acc, x: acc + x).alias("result")
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['result'] == 106
        print("\n✓ aggregate with init value works")


class TestNestedLambdas:
    """Tests for nested lambda expressions"""

    @pytest.mark.timeout(30)
    def test_nested_transform(self, spark):
        """Test nested transform for 2D array"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [[1, 2], [3, 4]] AS arr")
        df = spark.table("arr_view")
        result = df.select(
            F.transform("arr", lambda arr: F.transform(arr, lambda x: x * 2)).alias("result")
        )
        rows = result.collect()
        assert len(rows) == 1
        # Result should be [[2, 4], [6, 8]]
        nested_result = rows[0]['result']
        assert list(nested_result[0]) == [2, 4]
        assert list(nested_result[1]) == [6, 8]
        print("\n✓ nested transform works")

    @pytest.mark.timeout(30)
    def test_transform_then_filter(self, spark):
        """Test chaining transform and filter"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3, 4, 5] AS arr")
        df = spark.table("arr_view")
        # First transform to double, then filter > 5
        transformed = df.select(F.transform("arr", lambda x: x * 2).alias("doubled"))
        result = transformed.select(F.filter("doubled", lambda y: y > 5).alias("result"))
        rows = result.collect()
        assert len(rows) == 1
        # [2, 4, 6, 8, 10] filtered by > 5 = [6, 8, 10]
        assert list(rows[0]['result']) == [6, 8, 10]
        print("\n✓ transform then filter works")


class TestCombinedOperations:
    """Tests combining lambda functions with other DataFrame operations"""

    @pytest.mark.timeout(30)
    def test_transform_multiple_rows(self, spark):
        """Test transform on multiple rows"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW multi_arr_view AS
            SELECT 1 AS id, [1, 2, 3] AS numbers
            UNION ALL
            SELECT 2 AS id, [4, 5, 6] AS numbers
        """)
        df = spark.table("multi_arr_view")
        result = df.select(
            "id",
            F.transform("numbers", lambda x: x + 10).alias("transformed")
        ).orderBy("id")
        rows = result.collect()
        assert len(rows) == 2
        assert list(rows[0]['transformed']) == [11, 12, 13]
        assert list(rows[1]['transformed']) == [14, 15, 16]
        print("\n✓ transform on multiple rows works")

    @pytest.mark.timeout(30)
    def test_filter_in_where(self, spark):
        """Test using exists in filter"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW multi_arr_view AS
            SELECT 1 AS id, [1, 2, 3] AS numbers
            UNION ALL
            SELECT 2 AS id, [10, 20, 30] AS numbers
        """)
        df = spark.table("multi_arr_view")
        # Filter rows where any number > 15
        result = df.filter(F.exists("numbers", lambda x: x > 15)).select("id")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['id'] == 2
        print("\n✓ exists in filter works")
