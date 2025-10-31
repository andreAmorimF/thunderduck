#!/usr/bin/env python3
"""
Test runner for queries 11-20
"""

from pyspark.sql import SparkSession
from validation_runner import TpcdsValidationRunner
import sys

def main():
    """Test queries 11-20"""
    spark = SparkSession.builder \
        .appName("TPC-DS Q11-20 Test") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        runner = TpcdsValidationRunner(spark)

        # Test queries that have DataFrame implementations
        test_queries = [12, 15, 18, 26, 32, 40, 42, 46, 48, 50]

        print(f"\nTesting {len(test_queries)} queries with DataFrame implementations:")
        print(f"Queries: {test_queries}\n")

        results = runner.validate_all(query_nums=test_queries)

        print(f"\n{'='*60}")
        print("FINAL RESULTS")
        print(f"{'='*60}")
        print(f"Total: {results['total']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")
        print(f"Pass Rate: {results['passed']/results['total']*100:.1f}%")

        # Show failed queries if any
        if results['failed'] > 0:
            print("\nFailed queries:")
            for q_num, result in results['queries'].items():
                if not result['success']:
                    print(f"  Q{q_num}: {result['differences'][:1] if result['differences'] else 'Unknown error'}")

        sys.exit(0 if results['failed'] == 0 else 1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()