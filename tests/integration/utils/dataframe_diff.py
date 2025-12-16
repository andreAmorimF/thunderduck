"""
DataFrame Diff Utility for Differential Testing

Provides detailed row-by-row comparison of DataFrames from Spark Reference
and Thunderduck, with clear diff output for mismatches.
"""

from typing import List, Tuple, Optional, Any
from decimal import Decimal
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType
import difflib


class DataFrameDiff:
    """
    Compare two DataFrames and produce detailed diff output
    """

    def __init__(self, epsilon: float = 1e-6):
        """
        Initialize DataFrame diff utility

        Args:
            epsilon: Tolerance for floating point comparisons
        """
        self.epsilon = epsilon

    def compare(
        self,
        reference_df: DataFrame,
        test_df: DataFrame,
        query_name: str = "Query",
        max_diff_rows: int = 10
    ) -> Tuple[bool, str, dict]:
        """
        Compare two DataFrames and produce detailed diff

        Args:
            reference_df: Reference DataFrame (Spark)
            test_df: Test DataFrame (Thunderduck)
            query_name: Name of the query for logging
            max_diff_rows: Maximum number of diff rows to show

        Returns:
            Tuple of (passed: bool, message: str, stats: dict)
        """
        print(f"\n{'=' * 80}")
        print(f"Comparing: {query_name}")
        print(f"{'=' * 80}")

        stats = {
            'query_name': query_name,
            'schemas_match': False,
            'row_counts_match': False,
            'data_match': False,
            'reference_rows': 0,
            'test_rows': 0
        }

        # 1. Compare schemas
        schema_match, schema_msg = self._compare_schemas(
            reference_df.schema, test_df.schema
        )
        stats['schemas_match'] = schema_match

        if not schema_match:
            print(f"✗ Schema mismatch")
            print(schema_msg)
            return False, f"Schema mismatch:\n{schema_msg}", stats

        print("✓ Schemas match")

        # 2. Collect and compare row counts
        reference_rows = reference_df.collect()
        test_rows = test_df.collect()

        stats['reference_rows'] = len(reference_rows)
        stats['test_rows'] = len(test_rows)

        print(f"  Reference rows: {len(reference_rows):,}")
        print(f"  Test rows:      {len(test_rows):,}")

        if len(reference_rows) != len(test_rows):
            stats['row_counts_match'] = False
            msg = f"Row count mismatch: Reference={len(reference_rows)}, Test={len(test_rows)}"
            print(f"✗ {msg}")
            return False, msg, stats

        stats['row_counts_match'] = True
        print("✓ Row counts match")

        # 3. Compare data row-by-row
        mismatches = []
        for i, (ref_row, test_row) in enumerate(zip(reference_rows, test_rows)):
            match, diff_msg = self._compare_rows(
                ref_row, test_row, i, reference_df.columns
            )
            if not match:
                mismatches.append((i, diff_msg))
                if len(mismatches) >= max_diff_rows:
                    break

        if mismatches:
            stats['data_match'] = False
            diff_output = self._format_diff_output(
                mismatches, reference_rows, test_rows, reference_df.columns
            )
            print(f"✗ Found {len(mismatches)} mismatched rows (showing first {max_diff_rows})")
            print(diff_output)
            return False, f"Data mismatch:\n{diff_output}", stats

        stats['data_match'] = True
        print(f"✓ All {len(reference_rows):,} rows match")
        print(f"\n{'=' * 80}")
        print(f"✓ {query_name} PASSED")
        print(f"{'=' * 80}")

        return True, "Results match perfectly", stats

    def _compare_schemas(
        self, reference_schema: StructType, test_schema: StructType
    ) -> Tuple[bool, str]:
        """
        Compare two schemas

        Returns:
            Tuple of (match: bool, message: str)
        """
        ref_fields = [(f.name, str(f.dataType), f.nullable) for f in reference_schema.fields]
        test_fields = [(f.name, str(f.dataType), f.nullable) for f in test_schema.fields]

        if len(ref_fields) != len(test_fields):
            msg = f"Column count mismatch: Reference={len(ref_fields)}, Test={len(test_fields)}"
            return False, msg

        mismatches = []
        for i, (ref_field, test_field) in enumerate(zip(ref_fields, test_fields)):
            ref_name, ref_type, ref_nullable = ref_field
            test_name, test_type, test_nullable = test_field

            if ref_name != test_name:
                mismatches.append(
                    f"  Column {i}: name mismatch - Reference='{ref_name}', Test='{test_name}'"
                )

            if ref_type != test_type:
                mismatches.append(
                    f"  Column '{ref_name}': type mismatch - Reference={ref_type}, Test={test_type}"
                )

            if ref_nullable != test_nullable:
                mismatches.append(
                    f"  Column '{ref_name}': nullable mismatch - Reference={ref_nullable}, Test={test_nullable}"
                )

        if mismatches:
            return False, "\n".join(mismatches)

        return True, ""

    def _compare_rows(
        self, ref_row, test_row, row_index: int, columns: List[str]
    ) -> Tuple[bool, str]:
        """
        Compare two rows

        Returns:
            Tuple of (match: bool, diff_message: str)
        """
        ref_dict = ref_row.asDict()
        test_dict = test_row.asDict()

        diffs = []
        for col_name in columns:
            ref_val = ref_dict[col_name]
            test_val = test_dict[col_name]

            if not self._values_equal(ref_val, test_val):
                diffs.append({
                    'column': col_name,
                    'reference': ref_val,
                    'test': test_val
                })

        if diffs:
            return False, diffs

        return True, ""

    def _values_equal(self, ref_val: Any, test_val: Any) -> bool:
        """
        Compare two values with appropriate tolerance

        Args:
            ref_val: Reference value
            test_val: Test value

        Returns:
            True if values are equal (within tolerance)
        """
        # Handle nulls
        if ref_val is None and test_val is None:
            return True
        if ref_val is None or test_val is None:
            return False

        # Handle floats with epsilon
        if isinstance(ref_val, float) and isinstance(test_val, float):
            return abs(ref_val - test_val) < self.epsilon

        # Handle Decimal
        if isinstance(ref_val, Decimal) and isinstance(test_val, Decimal):
            return abs(ref_val - test_val) < Decimal(str(self.epsilon))

        # Handle mixed numeric types (int vs float)
        if isinstance(ref_val, (int, float)) and isinstance(test_val, (int, float)):
            return abs(float(ref_val) - float(test_val)) < self.epsilon

        # Exact equality for everything else
        return ref_val == test_val

    def _format_diff_output(
        self,
        mismatches: List[Tuple[int, List[dict]]],
        reference_rows: List,
        test_rows: List,
        columns: List[str]
    ) -> str:
        """
        Format diff output in a readable way

        Args:
            mismatches: List of (row_index, diff_list) tuples
            reference_rows: Reference rows
            test_rows: Test rows
            columns: Column names

        Returns:
            Formatted diff string
        """
        output = []
        output.append(f"\n{'=' * 80}")
        output.append(f"DIFF: {len(mismatches)} mismatched rows")
        output.append(f"{'=' * 80}")

        for row_idx, diffs in mismatches:
            output.append(f"\n--- Row {row_idx} ---")

            ref_row = reference_rows[row_idx].asDict()
            test_row = test_rows[row_idx].asDict()

            # Show full row context
            output.append(f"Reference row:")
            for col in columns:
                val = ref_row[col]
                output.append(f"  {col}: {val}")

            output.append(f"\nTest row:")
            for col in columns:
                val = test_row[col]
                # Highlight mismatched columns
                marker = "  "
                for diff in diffs:
                    if diff['column'] == col:
                        marker = "❌"
                        break
                output.append(f"{marker} {col}: {val}")

            # Show detailed diff for mismatched columns
            output.append(f"\nDifferences:")
            for diff in diffs:
                col = diff['column']
                ref_val = diff['reference']
                test_val = diff['test']
                output.append(f"  {col}:")
                output.append(f"    Reference: {ref_val} ({type(ref_val).__name__})")
                output.append(f"    Test:      {test_val} ({type(test_val).__name__})")

                # For numeric types, show the difference
                if isinstance(ref_val, (int, float)) and isinstance(test_val, (int, float)):
                    diff_val = abs(float(ref_val) - float(test_val))
                    output.append(f"    Diff:      {diff_val:.10f}")

        output.append(f"\n{'=' * 80}")
        return "\n".join(output)


# Convenience function for pytest
def assert_dataframes_equal(
    reference_df: DataFrame,
    test_df: DataFrame,
    query_name: str = "Query",
    epsilon: float = 1e-6,
    max_diff_rows: int = 10
):
    """
    Assert that two DataFrames are equal, with detailed diff on failure

    Args:
        reference_df: Reference DataFrame (Spark)
        test_df: Test DataFrame (Thunderduck)
        query_name: Name of the query for error messages
        epsilon: Tolerance for floating point comparisons
        max_diff_rows: Maximum number of diff rows to show

    Raises:
        AssertionError: If DataFrames don't match
    """
    diff = DataFrameDiff(epsilon=epsilon)
    passed, message, stats = diff.compare(
        reference_df, test_df, query_name, max_diff_rows
    )

    if not passed:
        raise AssertionError(f"{query_name} failed:\n{message}")


if __name__ == "__main__":
    # Example usage
    print("DataFrameDiff utility loaded")
    print("Use assert_dataframes_equal() for easy pytest integration")
