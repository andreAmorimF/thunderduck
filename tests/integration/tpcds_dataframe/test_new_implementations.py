#!/usr/bin/env python3
"""
Test newly added DataFrame query implementations
"""

from pyspark.sql import SparkSession
from tpcds_dataframe_queries import TpcdsDataFrameQueries
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_query(spark, query_num):
    """Test a single query"""
    try:
        logger.info(f"Testing Q{query_num}...")
        query_func = getattr(TpcdsDataFrameQueries, f"q{query_num}")
        result = query_func(spark)

        # Try to get count
        count = result.count()
        logger.info(f"  Q{query_num}: SUCCESS - {count} rows")
        return True
    except Exception as e:
        logger.error(f"  Q{query_num}: FAILED - {str(e)[:100]}")
        return False

def main():
    """Test newly implemented queries"""
    spark = SparkSession.builder \
        .appName("Test-New-Implementations") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    # Register tables
    data_dir = "/workspace/data/tpcds_sf1"
    tables = [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales",
        "customer", "customer_address", "customer_demographics", "date_dim",
        "household_demographics", "income_band", "inventory", "item",
        "promotion", "reason", "ship_mode", "store", "store_returns",
        "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
        "web_sales", "web_site"
    ]

    for table in tables:
        try:
            df = spark.read.parquet(f"{data_dir}/{table}.parquet")
            df.createOrReplaceTempView(table)
        except Exception as e:
            logger.warning(f"Could not register table {table}: {e}")

    # Test newly added queries (sample)
    new_queries = [25, 26, 29, 32, 37]  # Test first 5 new ones

    passed = 0
    failed = 0

    print("\n" + "="*60)
    print("Testing Newly Implemented Queries")
    print("="*60)

    for q in new_queries:
        if test_query(spark, q):
            passed += 1
        else:
            failed += 1

    print("\n" + "="*60)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    main()