"""
TPC-DS DataFrame Comparison Utilities

Provides order-independent comparison of DataFrames with tolerance for:
- Ordering differences (tie-breakers)
- Floating point precision
- NULL handling variations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull
import pandas as pd
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class DataFrameComparator:
    """Compare two DataFrames with configurable tolerance"""

    def __init__(self, float_tolerance: float = 1e-6, debug: bool = False):
        """
        Initialize comparator with tolerance settings

        Args:
            float_tolerance: Maximum difference for float comparison
            debug: Enable detailed debug output
        """
        self.float_tolerance = float_tolerance
        self.debug = debug

    def compare(self, expected: DataFrame, actual: DataFrame,
                query_name: str = "unknown") -> 'ComparisonResult':
        """
        Compare two DataFrames with order-independent matching

        Args:
            expected: Reference DataFrame (e.g., from SQL)
            actual: DataFrame to validate (e.g., from DataFrame API)
            query_name: Query identifier for logging

        Returns:
            ComparisonResult with detailed comparison information
        """
        result = ComparisonResult(query_name)

        # 1. Compare schemas
        result.schema_match = self._compare_schemas(expected, actual)

        # 2. Compare row counts
        expected_count = expected.count()
        actual_count = actual.count()
        result.row_count_match = (expected_count == actual_count)
        result.expected_rows = expected_count
        result.actual_rows = actual_count

        if not result.row_count_match:
            result.add_difference(
                f"Row count mismatch: expected {expected_count}, got {actual_count}"
            )

        # 3. Compare data (order-independent)
        if result.schema_match and result.row_count_match:
            result.data_match = self._compare_data(expected, actual, result)

        return result

    def _compare_schemas(self, expected: DataFrame, actual: DataFrame) -> bool:
        """Compare DataFrame schemas"""
        expected_schema = expected.schema
        actual_schema = actual.schema

        # Check column count
        if len(expected_schema.fields) != len(actual_schema.fields):
            return False

        # Check each field
        for exp_field, act_field in zip(expected_schema.fields, actual_schema.fields):
            if exp_field.name != act_field.name:
                return False
            # Allow some type flexibility (e.g., int vs bigint)
            if not self._types_compatible(exp_field.dataType, act_field.dataType):
                return False

        return True

    def _types_compatible(self, expected_type, actual_type) -> bool:
        """Check if two types are compatible"""
        # Convert to string for comparison
        exp_str = str(expected_type)
        act_str = str(actual_type)

        # Exact match
        if exp_str == act_str:
            return True

        # Allow numeric type flexibility
        numeric_types = ['IntegerType', 'LongType', 'FloatType', 'DoubleType',
                        'DecimalType', 'ShortType', 'ByteType']

        exp_base = exp_str.replace('()', '')
        act_base = act_str.replace('()', '')

        if exp_base in numeric_types and act_base in numeric_types:
            return True

        return False

    def _compare_data(self, expected: DataFrame, actual: DataFrame,
                      result: 'ComparisonResult') -> bool:
        """Compare DataFrame data with order-independent matching"""
        try:
            # Normalize and sort both DataFrames
            expected_sorted = self._normalize_and_sort(expected)
            actual_sorted = self._normalize_and_sort(actual)

            # Convert to Pandas for detailed comparison
            expected_pd = expected_sorted.toPandas()
            actual_pd = actual_sorted.toPandas()

            # Compare row by row
            differences = []
            for idx in range(len(expected_pd)):
                row_diffs = self._compare_rows(
                    expected_pd.iloc[idx],
                    actual_pd.iloc[idx],
                    idx
                )
                if row_diffs:
                    differences.extend(row_diffs)
                    if len(differences) >= 10:  # Limit reported differences
                        differences.append("... (more differences omitted)")
                        break

            if differences:
                for diff in differences:
                    result.add_difference(diff)
                return False

            return True

        except Exception as e:
            result.add_difference(f"Error comparing data: {str(e)}")
            return False

    def _normalize_and_sort(self, df: DataFrame) -> DataFrame:
        """Normalize DataFrame for comparison and sort by all columns"""
        # Replace NaN with NULL for consistent handling
        normalized = df
        for field in df.schema.fields:
            col_name = field.name
            # Handle float columns - replace NaN with NULL
            normalized = normalized.withColumn(
                col_name,
                when(isnan(col(col_name)), None).otherwise(col(col_name))
            )

        # Sort by all columns to handle tie-breakers
        return normalized.orderBy(*[col(c) for c in df.columns])

    def _compare_rows(self, expected_row, actual_row, row_idx: int) -> List[str]:
        """Compare two rows and return differences"""
        differences = []

        for col_name in expected_row.index:
            exp_val = expected_row[col_name]
            act_val = actual_row[col_name]

            if not self._values_equal(exp_val, act_val):
                differences.append(
                    f"Row {row_idx}, Column '{col_name}': "
                    f"expected {exp_val}, got {act_val}"
                )

        return differences

    def _values_equal(self, expected, actual) -> bool:
        """Compare two values with appropriate tolerance"""
        # Both NULL
        if pd.isna(expected) and pd.isna(actual):
            return True

        # One NULL
        if pd.isna(expected) or pd.isna(actual):
            return False

        # Float comparison
        if isinstance(expected, float) and isinstance(actual, float):
            return abs(expected - actual) < self.float_tolerance

        # Default comparison
        return expected == actual


class ComparisonResult:
    """Result of DataFrame comparison"""

    def __init__(self, query_name: str):
        self.query_name = query_name
        self.schema_match = False
        self.row_count_match = False
        self.data_match = False
        self.expected_rows = 0
        self.actual_rows = 0
        self.differences = []

    @property
    def success(self) -> bool:
        """Check if comparison was successful"""
        return self.schema_match and self.row_count_match and self.data_match

    def add_difference(self, diff: str):
        """Add a difference description"""
        self.differences.append(diff)

    def summary(self) -> str:
        """Get summary of comparison result"""
        status = "PASS" if self.success else "FAIL"
        summary = f"Query {self.query_name}: {status}\n"

        if not self.success:
            summary += f"  Schema Match: {self.schema_match}\n"
            summary += f"  Row Count Match: {self.row_count_match} "
            summary += f"(expected: {self.expected_rows}, actual: {self.actual_rows})\n"
            summary += f"  Data Match: {self.data_match}\n"

            if self.differences:
                summary += "  Differences:\n"
                for diff in self.differences[:5]:  # Show first 5 differences
                    summary += f"    - {diff}\n"
                if len(self.differences) > 5:
                    summary += f"    ... and {len(self.differences) - 5} more\n"

        return summary

    def to_dict(self) -> Dict:
        """Convert to dictionary for reporting"""
        return {
            'query': self.query_name,
            'success': self.success,
            'schema_match': self.schema_match,
            'row_count_match': self.row_count_match,
            'data_match': self.data_match,
            'expected_rows': self.expected_rows,
            'actual_rows': self.actual_rows,
            'differences': self.differences[:10]  # Limit for reporting
        }


def compare_with_sql_reference(
    spark,
    query_num: int,
    dataframe_impl,
    sql_query: str
) -> ComparisonResult:
    """
    Compare a DataFrame implementation with SQL reference

    Args:
        spark: SparkSession
        query_num: Query number for identification
        dataframe_impl: Function that returns DataFrame result
        sql_query: SQL query string for reference

    Returns:
        ComparisonResult with validation details
    """
    comparator = DataFrameComparator()

    try:
        # Execute SQL reference
        sql_result = spark.sql(sql_query)

        # Execute DataFrame implementation
        df_result = dataframe_impl(spark)

        # Compare results
        result = comparator.compare(sql_result, df_result, f"Q{query_num}")

        return result

    except Exception as e:
        # Create failure result
        result = ComparisonResult(f"Q{query_num}")
        result.add_difference(f"Execution error: {str(e)}")
        return result