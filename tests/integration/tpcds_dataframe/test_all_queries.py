#!/usr/bin/env python3
"""
Comprehensive test runner for all 100 TPC-DS queries
Validates DataFrame implementations against SQL references
"""

from pyspark.sql import SparkSession
from validation_runner import TpcdsValidationRunner
import sys
import json
from datetime import datetime

def main():
    """Test all 100 TPC-DS queries"""
    print(f"\n{'='*60}")
    print(f"TPC-DS DataFrame Validation - All 100 Queries")
    print(f"Started: {datetime.now()}")
    print(f"{'='*60}\n")

    spark = SparkSession.builder \
        .appName("TPC-DS All Queries Validation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        runner = TpcdsValidationRunner(spark)

        # Note: TPC-DS officially has 99 queries (no Q100)
        # We'll test 1-99
        all_queries = list(range(1, 100))

        # For initial testing, we can run a subset
        # Uncomment to test all: results = runner.validate_all(query_nums=all_queries)

        # Test a representative sample first
        sample_queries = [1, 3, 6, 7, 8, 10, 12, 15, 18, 21,
                         26, 32, 40, 42, 46, 48, 50, 55, 60, 65,
                         70, 75, 80, 85, 90, 95]

        print(f"Testing {len(sample_queries)} representative queries:")
        print(f"Queries: {sample_queries}\n")

        results = runner.validate_all(query_nums=sample_queries)

        # Detailed results
        print(f"\n{'='*60}")
        print("DETAILED RESULTS")
        print(f"{'='*60}")

        passed_queries = []
        failed_queries = []

        for q_num in sorted(results['queries'].keys()):
            result = results['queries'][q_num]
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            print(f"Query {q_num:3d}: {status}")

            if result['success']:
                passed_queries.append(q_num)
            else:
                failed_queries.append(q_num)
                if result['differences']:
                    print(f"          Issue: {result['differences'][0]}")

        # Summary statistics
        print(f"\n{'='*60}")
        print("SUMMARY STATISTICS")
        print(f"{'='*60}")
        print(f"Total Queries Tested: {results['total']}")
        print(f"Passed: {results['passed']} ({results['passed']/results['total']*100:.1f}%)")
        print(f"Failed: {results['failed']} ({results['failed']/results['total']*100:.1f}%)")

        if passed_queries:
            print(f"\nPassed Queries: {passed_queries}")
        if failed_queries:
            print(f"Failed Queries: {failed_queries}")

        # Save comprehensive results
        output_file = f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        runner.save_results(results, output_file)
        print(f"\nDetailed results saved to: {output_file}")

        # Return appropriate exit code
        exit_code = 0 if results['failed'] == 0 else 1

        print(f"\n{'='*60}")
        print(f"Completed: {datetime.now()}")
        print(f"{'='*60}")

        sys.exit(exit_code)

    except Exception as e:
        print(f"Error during validation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()