"""
Statistics Operations Tests

Tests for Spark Connect statistics operations (cov, corr, describe, etc.)
comparing Thunderduck against Apache Spark 4.0.1.
"""

import pytest
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType


@pytest.fixture(scope="function")
def setup_test_data(spark):
    """Create test data for statistics operations testing."""
    # Create a numeric test table for correlation/covariance
    spark.sql("""
        CREATE OR REPLACE TABLE test_stats_numeric AS
        SELECT * FROM (VALUES
            (1, 10.0, 100.0, 'A'),
            (2, 20.0, 200.0, 'B'),
            (3, 30.0, 300.0, 'A'),
            (4, 40.0, 400.0, 'B'),
            (5, 50.0, 500.0, 'A'),
            (6, 60.0, 600.0, 'B'),
            (7, 70.0, 700.0, 'A'),
            (8, 80.0, 800.0, 'B'),
            (9, 90.0, 900.0, 'A'),
            (10, 100.0, 1000.0, 'B')
        ) AS t(id, val1, val2, category)
    """)

    # Create a table with string and numeric columns for describe
    spark.sql("""
        CREATE OR REPLACE TABLE test_stats_mixed AS
        SELECT * FROM (VALUES
            (1, 'apple', 10.5),
            (2, 'banana', 20.3),
            (3, 'cherry', 15.7),
            (4, 'date', 25.9),
            (5, 'elderberry', 12.1)
        ) AS t(id, name, price)
    """)

    yield

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_stats_numeric")
    spark.sql("DROP TABLE IF EXISTS test_stats_mixed")


class TestStatCov:
    """Test df.stat.cov() - sample covariance"""

    def test_cov_positive_correlation(self, spark, setup_test_data):
        """Covariance of perfectly correlated columns should be positive."""
        df = spark.table("test_stats_numeric")
        cov = df.stat.cov("val1", "val2")
        # val1 and val2 are perfectly correlated (val2 = val1 * 10)
        assert cov > 0, "Covariance should be positive for positively correlated columns"

    def test_cov_is_scalar(self, spark, setup_test_data):
        """Covariance should return a numeric scalar."""
        df = spark.table("test_stats_numeric")
        cov = df.stat.cov("val1", "val2")
        assert isinstance(cov, (int, float)), f"Covariance should be numeric, got {type(cov)}"

    def test_cov_symmetric(self, spark, setup_test_data):
        """Covariance should be symmetric: cov(a,b) == cov(b,a)."""
        df = spark.table("test_stats_numeric")
        cov1 = df.stat.cov("val1", "val2")
        cov2 = df.stat.cov("val2", "val1")
        assert abs(cov1 - cov2) < 1e-10, "Covariance should be symmetric"


class TestStatCorr:
    """Test df.stat.corr() - Pearson correlation"""

    def test_corr_perfect_correlation(self, spark, setup_test_data):
        """Correlation of perfectly correlated columns should be 1.0."""
        df = spark.table("test_stats_numeric")
        corr = df.stat.corr("val1", "val2")
        # val1 and val2 are perfectly correlated (linear relationship)
        assert abs(corr - 1.0) < 1e-10, f"Perfect correlation should be 1.0, got {corr}"

    def test_corr_is_scalar(self, spark, setup_test_data):
        """Correlation should return a numeric scalar."""
        df = spark.table("test_stats_numeric")
        corr = df.stat.corr("val1", "val2")
        assert isinstance(corr, (int, float)), f"Correlation should be numeric, got {type(corr)}"

    def test_corr_range(self, spark, setup_test_data):
        """Correlation should be in range [-1, 1]."""
        df = spark.table("test_stats_numeric")
        corr = df.stat.corr("val1", "val2")
        assert -1.0 <= corr <= 1.0, f"Correlation should be in [-1, 1], got {corr}"

    def test_corr_symmetric(self, spark, setup_test_data):
        """Correlation should be symmetric: corr(a,b) == corr(b,a)."""
        df = spark.table("test_stats_numeric")
        corr1 = df.stat.corr("val1", "val2")
        corr2 = df.stat.corr("val2", "val1")
        assert abs(corr1 - corr2) < 1e-10, "Correlation should be symmetric"


class TestStatDescribe:
    """Test df.describe() - basic statistics"""

    def test_describe_returns_dataframe(self, spark, setup_test_data):
        """Describe should return a DataFrame."""
        df = spark.table("test_stats_numeric")
        result = df.describe()
        assert result is not None, "describe() should return a result"

    def test_describe_row_count(self, spark, setup_test_data):
        """Describe should return 5 rows: count, mean, stddev, min, max."""
        df = spark.table("test_stats_numeric")
        result = df.describe()
        rows = result.collect()
        assert len(rows) == 5, f"describe() should return 5 rows, got {len(rows)}"

    def test_describe_summary_column(self, spark, setup_test_data):
        """Describe should have a 'summary' column with stat names."""
        df = spark.table("test_stats_numeric")
        result = df.describe()
        rows = result.collect()
        summaries = [row.summary for row in rows]
        expected = ["count", "mean", "stddev", "min", "max"]
        assert summaries == expected, f"Expected {expected}, got {summaries}"

    def test_describe_specific_columns(self, spark, setup_test_data):
        """Describe with specific columns should only show those columns."""
        df = spark.table("test_stats_numeric")
        result = df.describe("val1")
        columns = result.columns
        # Should have 'summary' plus the specified column
        assert "summary" in columns, "Should have 'summary' column"
        assert "val1" in columns, "Should have 'val1' column"


class TestStatSummary:
    """Test df.summary() - configurable statistics"""

    def test_summary_default_stats(self, spark, setup_test_data):
        """Summary with no args should return default statistics."""
        df = spark.table("test_stats_numeric")
        result = df.summary()
        rows = result.collect()
        # Default: count, mean, stddev, min, 25%, 50%, 75%, max
        assert len(rows) == 8, f"Default summary should have 8 rows, got {len(rows)}"

    def test_summary_returns_dataframe(self, spark, setup_test_data):
        """Summary should return a DataFrame."""
        df = spark.table("test_stats_numeric")
        result = df.summary()
        assert result is not None, "summary() should return a result"

    def test_summary_custom_stats(self, spark, setup_test_data):
        """Summary with custom statistics list."""
        df = spark.table("test_stats_numeric")
        result = df.summary("count", "min", "max")
        rows = result.collect()
        summaries = [row.summary for row in rows]
        assert "count" in summaries
        assert "min" in summaries
        assert "max" in summaries


class TestStatCrosstab:
    """Test df.stat.crosstab() - contingency table"""

    def test_crosstab_returns_dataframe(self, spark, setup_test_data):
        """Crosstab should return a DataFrame."""
        df = spark.table("test_stats_numeric")
        result = df.stat.crosstab("category", "id")
        assert result is not None, "crosstab() should return a result"

    def test_crosstab_index_column_name(self, spark, setup_test_data):
        """Crosstab first column should be named 'col1_col2'."""
        df = spark.table("test_stats_numeric")
        result = df.stat.crosstab("category", "id")
        columns = result.columns
        expected_first_col = "category_id"
        assert columns[0] == expected_first_col, f"First column should be '{expected_first_col}', got {columns[0]}"

    def test_crosstab_counts(self, spark, setup_test_data):
        """Crosstab should contain counts."""
        df = spark.table("test_stats_numeric")
        result = df.stat.crosstab("category", "id")
        rows = result.collect()
        # Should have row for 'A' and row for 'B'
        assert len(rows) == 2, f"Should have 2 rows (A and B), got {len(rows)}"


class TestStatFreqItems:
    """Test df.stat.freqItems() - frequent items"""

    def test_freqitems_returns_dataframe(self, spark, setup_test_data):
        """FreqItems should return a DataFrame."""
        df = spark.table("test_stats_numeric")
        result = df.stat.freqItems(["category"])
        assert result is not None, "freqItems() should return a result"

    def test_freqitems_column_naming(self, spark, setup_test_data):
        """FreqItems should name columns as 'col_freqItems'."""
        df = spark.table("test_stats_numeric")
        result = df.stat.freqItems(["category"])
        columns = result.columns
        assert "category_freqItems" in columns, f"Should have 'category_freqItems' column, got {columns}"

    def test_freqitems_returns_array(self, spark, setup_test_data):
        """FreqItems should return arrays of frequent values."""
        df = spark.table("test_stats_numeric")
        result = df.stat.freqItems(["category"])
        rows = result.collect()
        assert len(rows) == 1, "freqItems should return 1 row"


class TestStatApproxQuantile:
    """Test df.stat.approxQuantile() - approximate quantiles"""

    def test_approx_quantile_median(self, spark, setup_test_data):
        """Approximate median (0.5 quantile) should be reasonable."""
        df = spark.table("test_stats_numeric")
        quantiles = df.stat.approxQuantile("val1", [0.5], 0.0)
        assert len(quantiles) == 1, "Should return one quantile value"
        # Median of 10,20,30,40,50,60,70,80,90,100 is 55
        assert 45 <= quantiles[0] <= 65, f"Median should be around 55, got {quantiles[0]}"

    def test_approx_quantile_multiple(self, spark, setup_test_data):
        """Multiple quantiles should return multiple values."""
        df = spark.table("test_stats_numeric")
        quantiles = df.stat.approxQuantile("val1", [0.25, 0.5, 0.75], 0.0)
        assert len(quantiles) == 3, f"Should return 3 quantile values, got {len(quantiles)}"

    def test_approx_quantile_ordering(self, spark, setup_test_data):
        """Quantiles should be in increasing order."""
        df = spark.table("test_stats_numeric")
        quantiles = df.stat.approxQuantile("val1", [0.25, 0.5, 0.75], 0.0)
        assert quantiles[0] <= quantiles[1] <= quantiles[2], "Quantiles should be in increasing order"

    def test_approx_quantile_min_max(self, spark, setup_test_data):
        """0.0 quantile should be min, 1.0 should be max."""
        df = spark.table("test_stats_numeric")
        quantiles = df.stat.approxQuantile("val1", [0.0, 1.0], 0.0)
        # Min is 10, max is 100
        assert quantiles[0] == 10.0, f"0.0 quantile should be min (10), got {quantiles[0]}"
        assert quantiles[1] == 100.0, f"1.0 quantile should be max (100), got {quantiles[1]}"


class TestStatSampleBy:
    """Test df.stat.sampleBy() - stratified sampling"""

    def test_sampleby_returns_dataframe(self, spark, setup_test_data):
        """SampleBy should return a DataFrame."""
        df = spark.table("test_stats_numeric")
        fractions = {"A": 0.5, "B": 0.5}
        result = df.stat.sampleBy("category", fractions, seed=42)
        assert result is not None, "sampleBy() should return a result"

    def test_sampleby_reduces_rows(self, spark, setup_test_data):
        """SampleBy with fraction < 1 should reduce row count (probabilistically)."""
        df = spark.table("test_stats_numeric")
        original_count = df.count()
        fractions = {"A": 0.2, "B": 0.2}
        result = df.stat.sampleBy("category", fractions, seed=42)
        sampled_count = result.count()
        # With 20% sampling, we expect fewer rows on average
        # But this is probabilistic, so we just check it's <= original
        assert sampled_count <= original_count, "Sampled count should be <= original"

    def test_sampleby_preserves_schema(self, spark, setup_test_data):
        """SampleBy should preserve the DataFrame schema."""
        df = spark.table("test_stats_numeric")
        fractions = {"A": 0.5, "B": 0.5}
        result = df.stat.sampleBy("category", fractions, seed=42)
        assert df.columns == result.columns, "SampleBy should preserve column names"


class TestStatisticsWithNulls:
    """Test statistics operations with NULL values."""

    def test_cov_ignores_nulls(self, spark):
        """Covariance should ignore NULL values."""
        df = spark.createDataFrame([
            (1, 10.0, 100.0),
            (2, 20.0, 200.0),
            (3, None, 300.0),
            (4, 40.0, None),
            (5, 50.0, 500.0)
        ], ["id", "val1", "val2"])

        # Should not throw an error
        cov = df.stat.cov("val1", "val2")
        assert not math.isnan(cov), "Covariance should handle NULLs"

    def test_describe_with_nulls(self, spark):
        """Describe should handle NULL values gracefully."""
        df = spark.createDataFrame([
            (1, "a"),
            (2, None),
            (None, "c")
        ], ["id", "name"])

        # Should not throw an error
        result = df.describe()
        rows = result.collect()
        assert len(rows) == 5, "describe() should return 5 rows even with NULLs"
