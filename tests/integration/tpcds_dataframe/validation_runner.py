"""
TPC-DS DataFrame Validation Runner

Validates DataFrame API implementations against SQL reference results.
This is Step 1 of our two-step validation approach.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Optional
import logging
import json
import os
from pathlib import Path
from comparison_utils import DataFrameComparator, ComparisonResult
from tpcds_queries import TpcdsDataFrameQueries, QUERY_IMPLEMENTATIONS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TpcdsValidationRunner:
    """Runs TPC-DS queries and validates DataFrame vs SQL results"""

    def __init__(self, spark: SparkSession, data_path: str = "/workspace/data/tpcds_sf1"):
        """
        Initialize validation runner

        Args:
            spark: SparkSession for execution
            data_path: Path to TPC-DS parquet data
        """
        self.spark = spark
        self.data_path = data_path
        self.comparator = DataFrameComparator(float_tolerance=1e-6, debug=True)
        self.sql_query_path = "/workspace/tests/integration/sql/tpcds_queries"

        # Register all TPC-DS tables
        self._register_tables()

    def _register_tables(self):
        """Register all TPC-DS tables from parquet files"""
        tables = [
            'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
            'customer', 'customer_address', 'customer_demographics', 'date_dim',
            'household_demographics', 'income_band', 'inventory', 'item',
            'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
            'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
            'web_sales', 'web_site'
        ]

        for table in tables:
            table_path = os.path.join(self.data_path, f"{table}.parquet")
            if os.path.exists(table_path):
                df = self.spark.read.parquet(table_path)
                df.createOrReplaceTempView(table)
                logger.info(f"Registered table: {table} ({df.count()} rows)")
            else:
                logger.warning(f"Table file not found: {table_path}")

    def load_sql_query(self, query_num: int) -> Optional[str]:
        """Load SQL query from file"""
        query_file = os.path.join(self.sql_query_path, f"q{query_num}.sql")
        if os.path.exists(query_file):
            with open(query_file, 'r') as f:
                return f.read()
        else:
            logger.warning(f"SQL query file not found: {query_file}")
            return None

    def run_sql_query(self, query_num: int) -> Optional[DataFrame]:
        """Run SQL reference query"""
        sql = self.load_sql_query(query_num)
        if sql:
            try:
                logger.info(f"Running SQL reference for Q{query_num}")
                return self.spark.sql(sql)
            except Exception as e:
                logger.error(f"Failed to run SQL Q{query_num}: {e}")
                return None
        return None

    def run_dataframe_query(self, query_num: int) -> Optional[DataFrame]:
        """Run DataFrame API implementation"""
        if query_num in QUERY_IMPLEMENTATIONS:
            try:
                logger.info(f"Running DataFrame implementation for Q{query_num}")
                impl = QUERY_IMPLEMENTATIONS[query_num]
                return impl(self.spark)
            except Exception as e:
                logger.error(f"Failed to run DataFrame Q{query_num}: {e}")
                return None
        else:
            logger.warning(f"No DataFrame implementation for Q{query_num}")
            return None

    def validate_query(self, query_num: int) -> ComparisonResult:
        """
        Validate a single query by comparing DataFrame vs SQL results

        Args:
            query_num: Query number to validate

        Returns:
            ComparisonResult with validation details
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Validating Query {query_num}")
        logger.info(f"{'='*60}")

        # Run SQL reference
        sql_result = self.run_sql_query(query_num)
        if sql_result is None:
            result = ComparisonResult(f"Q{query_num}")
            result.add_difference("Failed to run SQL reference")
            return result

        # Run DataFrame implementation
        df_result = self.run_dataframe_query(query_num)
        if df_result is None:
            result = ComparisonResult(f"Q{query_num}")
            result.add_difference("Failed to run DataFrame implementation")
            return result

        # Compare results
        result = self.comparator.compare(sql_result, df_result, f"Q{query_num}")

        # Log result
        logger.info(result.summary())

        return result

    def validate_all(self, query_nums: Optional[List[int]] = None) -> Dict:
        """
        Validate all or specified queries

        Args:
            query_nums: List of query numbers to validate, or None for all

        Returns:
            Dictionary with validation results
        """
        if query_nums is None:
            query_nums = list(QUERY_IMPLEMENTATIONS.keys())

        results = {
            'total': len(query_nums),
            'passed': 0,
            'failed': 0,
            'queries': {}
        }

        for query_num in query_nums:
            result = self.validate_query(query_num)
            results['queries'][query_num] = result.to_dict()

            if result.success:
                results['passed'] += 1
            else:
                results['failed'] += 1

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("VALIDATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total Queries: {results['total']}")
        logger.info(f"Passed: {results['passed']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"Pass Rate: {results['passed']/results['total']*100:.1f}%")

        return results

    def save_results(self, results: Dict, output_file: str = "validation_results.json"):
        """Save validation results to JSON file"""
        output_path = os.path.join(
            "/workspace/tests/integration/tpcds_dataframe",
            output_file
        )
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to: {output_path}")


def main():
    """Main entry point for validation runner"""
    # Create Spark session with more memory and disabled broadcast joins
    spark = SparkSession.builder \
        .appName("TPC-DS DataFrame Validation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()

    try:
        # Create validation runner
        runner = TpcdsValidationRunner(spark)

        # Validate implemented queries (1-10 for now)
        results = runner.validate_all(query_nums=[1, 3, 6, 7, 8, 10])

        # Save results
        runner.save_results(results)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()