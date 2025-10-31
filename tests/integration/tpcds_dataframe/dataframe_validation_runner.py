#!/usr/bin/env python3
"""
TPC-DS DataFrame API Validation Runner

This module validates pure DataFrame API implementations against SQL references.
Only tests queries that can be implemented without SQL-specific features.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Optional, Tuple
import logging
import time
import json
from datetime import datetime
from comparison_utils import DataFrameComparator
from tpcds_dataframe_queries import (
    COMPATIBLE_QUERIES,
    get_query_implementation,
    list_compatible_queries,
    list_incompatible_queries
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TpcdsDataFrameValidationRunner:
    """Validates pure DataFrame implementations of TPC-DS queries"""

    def __init__(self, spark: SparkSession, data_dir: str = "/workspace/data/tpcds_sf1"):
        self.spark = spark
        self.data_dir = data_dir
        self.comparator = DataFrameComparator()
        self._register_tables()

    def _register_tables(self):
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
                df = self.spark.read.parquet(f"{self.data_dir}/{table}.parquet")
                df.createOrReplaceTempView(table)
                logger.info(f"Registered table: {table}")
            except Exception as e:
                logger.warning(f"Could not register table {table}: {e}")

    def run_sql_query(self, query_num: int) -> DataFrame:
        """Run SQL version of query for comparison"""
        sql_file = f"/workspace/benchmarks/tpcds_queries/q{query_num}.sql"
        try:
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            return self.spark.sql(sql_content)
        except Exception as e:
            logger.error(f"Failed to run SQL query {query_num}: {e}")
            raise

    def run_dataframe_query(self, query_num: int) -> Optional[DataFrame]:
        """Run DataFrame API version of query"""
        implementation = get_query_implementation(query_num)
        if implementation is None:
            logger.warning(f"Query {query_num} is not compatible with pure DataFrame API")
            return None

        try:
            return implementation(self.spark)
        except Exception as e:
            logger.error(f"Failed to run DataFrame query {query_num}: {e}")
            raise

    def validate_query(self, query_num: int) -> Dict:
        """Validate a single query by comparing DataFrame vs SQL results"""
        logger.info(f"Validating Query {query_num}...")
        result = {
            'query_num': query_num,
            'success': False,
            'dataframe_compatible': query_num in COMPATIBLE_QUERIES,
            'error': None,
            'differences': [],
            'execution_time': 0
        }

        if not result['dataframe_compatible']:
            result['error'] = 'Query requires SQL features not available in DataFrame API'
            logger.info(f"  Query {query_num}: Skipped (incompatible)")
            return result

        start_time = time.time()

        try:
            # Run DataFrame version
            df_result = self.run_dataframe_query(query_num)
            if df_result is None:
                result['error'] = 'No DataFrame implementation available'
                return result

            # Run SQL version
            sql_result = self.run_sql_query(query_num)

            # Compare results
            is_equal = self.comparator.compare(df_result, sql_result)
            differences = self.comparator.result.differences if not is_equal else []

            result['success'] = is_equal
            result['differences'] = differences
            result['execution_time'] = time.time() - start_time

            if is_equal:
                logger.info(f"  Query {query_num}: ✅ PASS ({result['execution_time']:.2f}s)")
            else:
                logger.warning(f"  Query {query_num}: ❌ FAIL - {differences[0] if differences else 'Unknown difference'}")

        except Exception as e:
            result['error'] = str(e)
            result['execution_time'] = time.time() - start_time
            logger.error(f"  Query {query_num}: ❌ ERROR - {e}")

        return result

    def validate_all(self, query_nums: Optional[List[int]] = None) -> Dict:
        """Validate multiple queries"""
        if query_nums is None:
            query_nums = COMPATIBLE_QUERIES

        # Filter to only compatible queries
        query_nums = [q for q in query_nums if q in COMPATIBLE_QUERIES]

        logger.info(f"Starting validation of {len(query_nums)} DataFrame-compatible queries")
        logger.info(f"Queries to validate: {query_nums}")

        results = {
            'timestamp': datetime.now().isoformat(),
            'total': len(query_nums),
            'passed': 0,
            'failed': 0,
            'skipped': 0,
            'queries': {}
        }

        for query_num in query_nums:
            query_result = self.validate_query(query_num)
            results['queries'][query_num] = query_result

            if not query_result['dataframe_compatible']:
                results['skipped'] += 1
            elif query_result['success']:
                results['passed'] += 1
            else:
                results['failed'] += 1

        # Also note incompatible queries
        all_queries = set(range(1, 100))
        incompatible = all_queries - set(COMPATIBLE_QUERIES)
        results['incompatible_queries'] = sorted(list(incompatible))
        results['compatible_queries'] = COMPATIBLE_QUERIES

        logger.info(f"\nValidation Complete:")
        logger.info(f"  Total Compatible: {len(COMPATIBLE_QUERIES)}")
        logger.info(f"  Total Incompatible: {len(incompatible)}")
        logger.info(f"  Tested: {results['total']}")
        logger.info(f"  Passed: {results['passed']}")
        logger.info(f"  Failed: {results['failed']}")

        return results

    def save_results(self, results: Dict, filename: str):
        """Save validation results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {filename}")


def main():
    """Run validation of DataFrame-compatible queries"""
    print("=" * 80)
    print("TPC-DS DataFrame API Validation")
    print("Testing Pure DataFrame Implementations Only")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("TPC-DS DataFrame Validation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        runner = TpcdsDataFrameValidationRunner(spark)

        # Display compatibility information
        compatible = list_compatible_queries()
        incompatible = list_incompatible_queries()

        print(f"\nDataFrame API Compatibility:")
        print(f"  Compatible queries: {len(compatible)} / 99")
        print(f"  Incompatible queries: {len(incompatible)} / 99")
        print(f"\nCompatible queries: {compatible}")

        # Test a sample of compatible queries first
        sample_queries = [3, 7, 12, 13, 15, 19, 20]  # Small sample for initial testing
        print(f"\nTesting sample queries: {sample_queries}")

        results = runner.validate_all(query_nums=sample_queries)

        # Display results
        print(f"\n{'='*80}")
        print("VALIDATION RESULTS")
        print(f"{'='*80}")

        for q_num in sorted(results['queries'].keys()):
            query_result = results['queries'][q_num]
            if query_result['success']:
                status = "✅ PASS"
            elif not query_result['dataframe_compatible']:
                status = "⚠️  SKIP"
            else:
                status = "❌ FAIL"

            print(f"Query {q_num:3d}: {status}")
            if query_result.get('error'):
                print(f"          Error: {query_result['error']}")
            elif query_result.get('differences'):
                print(f"          Diff: {query_result['differences'][0]}")

        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Total DataFrame-compatible queries: {len(compatible)}")
        print(f"Queries tested: {results['total']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")

        # Save results
        output_file = f"dataframe_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        runner.save_results(results, output_file)

    except Exception as e:
        print(f"Error during validation: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()