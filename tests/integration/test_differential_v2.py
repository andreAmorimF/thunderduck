"""
TPC-H Differential Testing V2: Apache Spark Connect vs Thunderduck

This test suite runs TPC-H queries on both:
1. Apache Spark 4.0.1 Connect (reference) - running in Podman container
2. Thunderduck Connect (test) - system under test

Results are compared row-by-row with detailed diff output on mismatch.

Key improvements over V1:
- Uses Podman container for Spark Connect (no manual install needed)
- Detailed row-by-row diff on mismatch
- Better error messages
- Session-scoped fixtures for performance
"""

import pytest
import time
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# TPC-H Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q1_Differential:
    """TPC-H Q1: Pricing Summary Report"""

    def test_q1_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q1"""
        query = load_tpch_query(1)

        # Execute on Spark reference
        print("\n" + "=" * 80)
        print("Executing Q1 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        # Execute on Thunderduck
        print("\nExecuting Q1 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Compare results with detailed diff
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Assert equality with detailed diff on failure
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q1",
            epsilon=1e-6,
            max_diff_rows=5
        )


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q3_Differential:
    """TPC-H Q3: Shipping Priority"""

    def test_q3_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q3"""
        query = load_tpch_query(3)

        # Execute on both systems
        print("\n" + "=" * 80)
        print("Executing Q3 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        print("\nExecuting Q3 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q3",
            epsilon=1e-6
        )


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q6_Differential:
    """TPC-H Q6: Forecasting Revenue Change"""

    def test_q6_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q6"""
        query = load_tpch_query(6)

        # Execute on both systems
        print("\n" + "=" * 80)
        print("Executing Q6 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        print("\nExecuting Q6 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q6",
            epsilon=1e-6
        )


# ============================================================================
# Parameterized Tests for All TPC-H Queries
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
@pytest.mark.parametrize("query_num", [
    2, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
])
class TestTPCH_AllQueries_Differential:
    """Differential tests for all other TPC-H queries"""

    def test_query_differential(
        self,
        query_num,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for query N"""
        query = load_tpch_query(query_num)

        # Execute on Spark reference
        print("\n" + "=" * 80)
        print(f"Executing Q{query_num} on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        # Execute on Thunderduck
        print(f"\nExecuting Q{query_num} on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        if td_time > 0:
            print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name=f"TPC-H Q{query_num}",
            epsilon=1e-6,
            max_diff_rows=5
        )


# ============================================================================
# Quick Sanity Test
# ============================================================================

@pytest.mark.differential
@pytest.mark.quick
class TestDifferential_Sanity:
    """Quick sanity test to verify differential framework is working"""

    def test_simple_select(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck
    ):
        """Test simple SELECT query"""
        query = "SELECT COUNT(*) as cnt FROM lineitem"

        print("\n" + "=" * 80)
        print("Sanity Test: Simple SELECT COUNT(*)")
        print("=" * 80)

        # Execute on both
        ref_result = spark_reference.sql(query)
        test_result = spark_thunderduck.sql(query)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="Sanity: SELECT COUNT(*)"
        )

        print("✓ Differential framework is working correctly!")
