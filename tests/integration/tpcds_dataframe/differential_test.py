#!/usr/bin/env python3
"""
Differential Testing: Spark vs ThunderDuck for DataFrame API

This script runs the same DataFrame queries on both Spark (local) and ThunderDuck (remote)
and compares the results to identify compatibility gaps.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Optional, Tuple
import logging
import time
import json
from datetime import datetime
from comparison_utils import DataFrameComparator
from tpcds_dataframe_queries import (
    TpcdsDataFrameQueries,
    COMPATIBLE_QUERIES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DifferentialTester:
    """Runs DataFrame queries on both Spark and ThunderDuck"""

    def __init__(self, data_dir: str = "/workspace/data/tpcds_sf1"):
        self.data_dir = data_dir
        self.comparator = DataFrameComparator()
        self.spark_session = None
        self.thunderduck_session = None

    def setup_spark(self) -> SparkSession:
        """Setup local Spark session"""
        logger.info("Setting up local Spark session...")
        spark = SparkSession.builder \
            .appName("Spark-DataFrame-Test") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()

        self.spark_session = spark
        self._register_tables(spark)
        return spark

    def setup_thunderduck(self) -> SparkSession:
        """Setup remote ThunderDuck session"""
        logger.info("Setting up remote ThunderDuck session...")
        spark = SparkSession.builder \
            .remote("sc://localhost:15002") \
            .appName("ThunderDuck-DataFrame-Test") \
            .getOrCreate()

        self.thunderduck_session = spark
        self._register_tables(spark)
        return spark

    def _register_tables(self, spark: SparkSession):
        """Register TPC-DS tables from parquet files"""
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
                df = spark.read.parquet(f"{self.data_dir}/{table}.parquet")
                df.createOrReplaceTempView(table)
                logger.debug(f"Registered table: {table}")
            except Exception as e:
                logger.warning(f"Could not register table {table}: {e}")

    def run_query_on_spark(self, query_num: int) -> Tuple[Optional[DataFrame], float, Optional[str]]:
        """Run query on Spark"""
        start_time = time.time()
        error = None
        result = None

        try:
            query_func = getattr(TpcdsDataFrameQueries, f"q{query_num}")
            result = query_func(self.spark_session)
            # Cache and count to ensure execution
            result.cache()
            count = result.count()
            logger.debug(f"Spark Q{query_num}: {count} rows")
        except Exception as e:
            error = str(e)
            logger.error(f"Spark Q{query_num} failed: {error}")

        execution_time = time.time() - start_time
        return result, execution_time, error

    def run_query_on_thunderduck(self, query_num: int) -> Tuple[Optional[DataFrame], float, Optional[str]]:
        """Run query on ThunderDuck"""
        start_time = time.time()
        error = None
        result = None

        try:
            query_func = getattr(TpcdsDataFrameQueries, f"q{query_num}")
            result = query_func(self.thunderduck_session)
            # Cache and count to ensure execution
            result.cache()
            count = result.count()
            logger.debug(f"ThunderDuck Q{query_num}: {count} rows")
        except Exception as e:
            error = str(e)
            logger.error(f"ThunderDuck Q{query_num} failed: {error}")

        execution_time = time.time() - start_time
        return result, execution_time, error

    def compare_query(self, query_num: int) -> Dict:
        """Run query on both systems and compare"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing Query {query_num}")
        logger.info(f"{'='*60}")

        result = {
            'query_num': query_num,
            'spark_success': False,
            'spark_time': 0,
            'spark_error': None,
            'thunderduck_success': False,
            'thunderduck_time': 0,
            'thunderduck_error': None,
            'results_match': False,
            'differences': []
        }

        # Run on Spark
        logger.info(f"Running Q{query_num} on Spark...")
        spark_df, spark_time, spark_error = self.run_query_on_spark(query_num)
        result['spark_time'] = spark_time
        result['spark_error'] = spark_error
        result['spark_success'] = spark_error is None

        # Run on ThunderDuck
        logger.info(f"Running Q{query_num} on ThunderDuck...")
        td_df, td_time, td_error = self.run_query_on_thunderduck(query_num)
        result['thunderduck_time'] = td_time
        result['thunderduck_error'] = td_error
        result['thunderduck_success'] = td_error is None

        # Compare results if both succeeded
        if spark_df is not None and td_df is not None:
            logger.info(f"Comparing results...")
            is_equal = self.comparator.compare(spark_df, td_df)
            result['results_match'] = is_equal
            if not is_equal:
                result['differences'] = self.comparator.result.differences

        # Report status
        if result['spark_success'] and result['thunderduck_success']:
            if result['results_match']:
                logger.info(f"✅ Q{query_num}: PASS - Results match")
            else:
                logger.warning(f"❌ Q{query_num}: FAIL - Results differ")
                for diff in result['differences'][:3]:
                    logger.warning(f"  - {diff}")
        elif not result['spark_success']:
            logger.error(f"❌ Q{query_num}: Spark failed - {spark_error}")
        elif not result['thunderduck_success']:
            logger.error(f"❌ Q{query_num}: ThunderDuck failed - {td_error}")

        return result

    def run_differential_tests(self, query_nums: Optional[List[int]] = None) -> Dict:
        """Run differential tests on specified queries"""
        if query_nums is None:
            query_nums = COMPATIBLE_QUERIES[:5]  # Start with first 5 compatible queries

        results = {
            'timestamp': datetime.now().isoformat(),
            'total': len(query_nums),
            'both_succeeded': 0,
            'results_matched': 0,
            'spark_only_succeeded': 0,
            'thunderduck_only_succeeded': 0,
            'both_failed': 0,
            'queries': {}
        }

        # Setup sessions
        self.setup_spark()
        self.setup_thunderduck()

        logger.info(f"\nStarting differential testing of {len(query_nums)} queries")
        logger.info(f"Queries: {query_nums}")

        for query_num in query_nums:
            query_result = self.compare_query(query_num)
            results['queries'][query_num] = query_result

            # Update statistics
            if query_result['spark_success'] and query_result['thunderduck_success']:
                results['both_succeeded'] += 1
                if query_result['results_match']:
                    results['results_matched'] += 1
            elif query_result['spark_success'] and not query_result['thunderduck_success']:
                results['spark_only_succeeded'] += 1
            elif not query_result['spark_success'] and query_result['thunderduck_success']:
                results['thunderduck_only_succeeded'] += 1
            else:
                results['both_failed'] += 1

        return results

    def cleanup(self):
        """Clean up sessions"""
        if self.spark_session:
            self.spark_session.stop()
        if self.thunderduck_session:
            self.thunderduck_session.stop()


def main():
    """Run differential tests"""
    print("=" * 80)
    print("Differential Testing: Spark vs ThunderDuck")
    print("Testing DataFrame API Compatibility")
    print("=" * 80)

    tester = DifferentialTester()

    try:
        # Test a small sample first
        test_queries = [3, 7, 12]  # Start with 3 simple queries

        print(f"\nTesting queries: {test_queries}")
        results = tester.run_differential_tests(test_queries)

        # Display summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Total queries tested: {results['total']}")
        print(f"Both systems succeeded: {results['both_succeeded']}")
        print(f"Results matched: {results['results_matched']}")
        print(f"Only Spark succeeded: {results['spark_only_succeeded']}")
        print(f"Only ThunderDuck succeeded: {results['thunderduck_only_succeeded']}")
        print(f"Both failed: {results['both_failed']}")

        # Detailed results
        print(f"\n{'='*80}")
        print("DETAILED RESULTS")
        print(f"{'='*80}")

        for q_num in sorted(results['queries'].keys()):
            q_result = results['queries'][q_num]
            spark_status = "✅" if q_result['spark_success'] else "❌"
            td_status = "✅" if q_result['thunderduck_success'] else "❌"
            match_status = "✅" if q_result['results_match'] else "❌"

            print(f"\nQuery {q_num}:")
            print(f"  Spark: {spark_status} ({q_result['spark_time']:.2f}s)")
            if q_result['spark_error']:
                print(f"    Error: {q_result['spark_error'][:100]}")
            print(f"  ThunderDuck: {td_status} ({q_result['thunderduck_time']:.2f}s)")
            if q_result['thunderduck_error']:
                print(f"    Error: {q_result['thunderduck_error'][:100]}")
            if q_result['spark_success'] and q_result['thunderduck_success']:
                print(f"  Results match: {match_status}")
                if not q_result['results_match']:
                    print(f"    Differences: {q_result['differences'][:3]}")

        # Save results
        output_file = f"differential_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {output_file}")

    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()