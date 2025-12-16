"""
TPC-DS Differential Testing: Apache Spark Connect vs Thunderduck

This test suite runs TPC-DS queries on both:
1. Apache Spark 4.0.1 Connect (reference) - running natively
2. Thunderduck Connect (test) - system under test

Results are compared row-by-row with detailed diff output on mismatch.

Coverage:
- 94 out of 99 TPC-DS queries (Q36 excluded due to DuckDB limitation)
- Variant queries: Q14a/b, Q23a/b, Q24a/b, Q39a/b
- Total: ~98 unique query patterns

Note: Q36 uses GROUPING() in window PARTITION BY clause which DuckDB
does not support. See docs/research/TPCDS_ROOT_CAUSE_ANALYSIS.md
"""

import pytest
import time
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# TPC-DS Query Lists
# ============================================================================

# Standard queries (1-99, excluding those with variants and Q36)
STANDARD_QUERIES = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 15, 16, 17, 18, 19, 20,
    21, 22, 25, 26, 27, 28, 29, 30,
    31, 32, 33, 34, 35, 37, 38,  # Q36 excluded (DuckDB limitation)
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
    61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
    71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
    81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
    91, 92, 93, 94, 95, 96, 97, 98, 99
]

# Queries with a/b variants (instead of just the number)
VARIANT_QUERIES = ['14a', '14b', '23a', '23b', '24a', '24b', '39a', '39b']

# All query identifiers for parameterized testing
ALL_QUERIES = STANDARD_QUERIES + VARIANT_QUERIES


def run_differential_query(
    spark_reference,
    spark_thunderduck,
    query: str,
    query_name: str,
    epsilon: float = 1e-6
):
    """
    Execute a query on both Spark Reference and Thunderduck, compare results.

    Args:
        spark_reference: Spark session connected to Apache Spark
        spark_thunderduck: Spark session connected to Thunderduck
        query: SQL query string
        query_name: Name for display purposes
        epsilon: Tolerance for floating-point comparison
    """
    # Execute on Spark reference
    print("\n" + "=" * 80)
    print(f"Executing {query_name} on Spark Reference...")
    start_ref = time.time()
    reference_result = spark_reference.sql(query)
    ref_time = time.time() - start_ref
    print(f"✓ Spark Reference completed in {ref_time:.3f}s")

    # Execute on Thunderduck
    print(f"\nExecuting {query_name} on Thunderduck...")
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

    # Compare with detailed diff
    assert_dataframes_equal(
        reference_result,
        test_result,
        query_name=query_name,
        epsilon=epsilon,
        max_diff_rows=5
    )


# ============================================================================
# TPC-DS Differential Tests - Parameterized
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.parametrize("query_id", ALL_QUERIES)
class TestTPCDS_Differential:
    """
    Differential tests for all TPC-DS queries.

    Runs each query on both Apache Spark 4.0.1 and Thunderduck,
    comparing results row-by-row.
    """

    def test_query_differential(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """Compare Spark and Thunderduck results for TPC-DS query"""
        query = load_tpcds_query(query_id)

        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}",
            epsilon=1e-6
        )


# ============================================================================
# Quick Sanity Test
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.quick
class TestTPCDS_Sanity:
    """Quick sanity test to verify TPC-DS differential framework is working"""

    def test_simple_count(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck
    ):
        """Test simple SELECT COUNT(*) on a TPC-DS table"""
        query = "SELECT COUNT(*) as cnt FROM store_sales"

        print("\n" + "=" * 80)
        print("TPC-DS Sanity Test: Simple SELECT COUNT(*)")
        print("=" * 80)

        # Execute on both
        ref_result = spark_reference.sql(query)
        test_result = spark_thunderduck.sql(query)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Sanity: SELECT COUNT(*)"
        )

        print("✓ TPC-DS differential framework is working correctly!")


# ============================================================================
# Batch Tests for Faster Feedback
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch1:
    """TPC-DS Q1-Q10 differential tests (batch for faster feedback)"""

    @pytest.mark.parametrize("query_id", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    def test_batch1(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 1-10"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch2:
    """TPC-DS Q11-Q20 differential tests"""

    @pytest.mark.parametrize("query_id", [11, 12, 13, '14a', '14b', 15, 16, 17, 18, 19, 20])
    def test_batch2(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 11-20 (with Q14 variants)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch3:
    """TPC-DS Q21-Q30 differential tests"""

    @pytest.mark.parametrize("query_id", [21, 22, '23a', '23b', '24a', '24b', 25, 26, 27, 28, 29, 30])
    def test_batch3(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 21-30 (with Q23, Q24 variants)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch4:
    """TPC-DS Q31-Q40 differential tests (Q36 excluded)"""

    @pytest.mark.parametrize("query_id", [31, 32, 33, 34, 35, 37, 38, '39a', '39b', 40])
    def test_batch4(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 31-40 (Q36 excluded, Q39 variants included)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch5:
    """TPC-DS Q41-Q50 differential tests"""

    @pytest.mark.parametrize("query_id", [41, 42, 43, 44, 45, 46, 47, 48, 49, 50])
    def test_batch5(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 41-50"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch6:
    """TPC-DS Q51-Q60 differential tests"""

    @pytest.mark.parametrize("query_id", [51, 52, 53, 54, 55, 56, 57, 58, 59, 60])
    def test_batch6(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 51-60"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch7:
    """TPC-DS Q61-Q70 differential tests"""

    @pytest.mark.parametrize("query_id", [61, 62, 63, 64, 65, 66, 67, 68, 69, 70])
    def test_batch7(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 61-70"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch8:
    """TPC-DS Q71-Q80 differential tests"""

    @pytest.mark.parametrize("query_id", [71, 72, 73, 74, 75, 76, 77, 78, 79, 80])
    def test_batch8(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 71-80"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch9:
    """TPC-DS Q81-Q90 differential tests"""

    @pytest.mark.parametrize("query_id", [81, 82, 83, 84, 85, 86, 87, 88, 89, 90])
    def test_batch9(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 81-90"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch10:
    """TPC-DS Q91-Q99 differential tests"""

    @pytest.mark.parametrize("query_id", [91, 92, 93, 94, 95, 96, 97, 98, 99])
    def test_batch10(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 91-99"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


# ============================================================================
# TPC-DS DataFrame API Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.dataframe
class TestTPCDS_DataFrame_Differential:
    """
    TPC-DS queries implemented using DataFrame API for differential testing.

    These tests verify that DataFrame operations produce identical results
    on both Spark Reference and Thunderduck. Only queries compatible with
    pure DataFrame API (no CTEs, ROLLUP, etc.) are included.
    """

    def test_q3_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q3 via DataFrame API: Item brand sales analysis
        Tests: multi-table join, filter, groupBy, agg, orderBy, limit
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q3 DataFrame API: Item Brand Sales")
        print("=" * 80)

        def build_q3(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manufact_id") == 128) &
                    (F.col("d_moy") == 11)
                )
                .groupBy("d_year", "i_brand", "i_brand_id")
                .agg(F.sum("ss_ext_sales_price").alias("sum_agg"))
                .select(
                    F.col("d_year"),
                    F.col("i_brand_id").alias("brand_id"),
                    F.col("i_brand").alias("brand"),
                    F.col("sum_agg")
                )
                .orderBy("d_year", F.col("sum_agg").desc(), "brand_id")
                .limit(100)
            )

        ref_result = build_q3(spark_reference)
        test_result = build_q3(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q3 DataFrame",
            epsilon=1e-6
        )

    def test_q7_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q7 via DataFrame API: Promotional item analysis
        Tests: 5-table join, filter with OR, groupBy, avg aggregations
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q7 DataFrame API: Promotional Item Analysis")
        print("=" * 80)

        def build_q7(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            promotion = spark.read.parquet(str(tpcds_data_dir / "promotion.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .join(customer_demographics, store_sales["ss_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(promotion, store_sales["ss_promo_sk"] == promotion["p_promo_sk"])
                .filter(
                    (F.col("cd_gender") == "M") &
                    (F.col("cd_marital_status") == "S") &
                    (F.col("cd_education_status") == "College") &
                    ((F.col("p_channel_email") == "N") | (F.col("p_channel_event") == "N")) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("i_item_id")
                .agg(
                    F.avg("ss_quantity").alias("agg1"),
                    F.avg("ss_list_price").alias("agg2"),
                    F.avg("ss_coupon_amt").alias("agg3"),
                    F.avg("ss_sales_price").alias("agg4")
                )
                .orderBy("i_item_id")
                .limit(100)
            )

        ref_result = build_q7(spark_reference)
        test_result = build_q7(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q7 DataFrame",
            epsilon=1e-6
        )

    def test_q42_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q42 via DataFrame API: Monthly sales by category
        Tests: join, filter, groupBy with multiple columns, sum, orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q42 DataFrame API: Monthly Sales by Category")
        print("=" * 80)

        def build_q42(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manager_id") == 1) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("d_year", "i_category_id", "i_category")
                .agg(F.sum("ss_ext_sales_price").alias("sum_agg"))
                .orderBy(
                    F.col("sum_agg").desc(),
                    "d_year",
                    "i_category_id",
                    "i_category"
                )
                .limit(100)
            )

        ref_result = build_q42(spark_reference)
        test_result = build_q42(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q42 DataFrame",
            epsilon=1e-6
        )

    def test_q52_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q52 via DataFrame API: Brand revenue analysis
        Tests: 3-table join, filter, groupBy, sum, orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q52 DataFrame API: Brand Revenue Analysis")
        print("=" * 80)

        def build_q52(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manager_id") == 1) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("d_year", "i_brand", "i_brand_id")
                .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
                .orderBy("d_year", F.col("ext_price").desc(), "i_brand_id")
                .limit(100)
            )

        ref_result = build_q52(spark_reference)
        test_result = build_q52(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q52 DataFrame",
            epsilon=1e-6
        )
