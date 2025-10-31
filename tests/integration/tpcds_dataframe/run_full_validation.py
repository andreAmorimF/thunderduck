#!/usr/bin/env python3
"""
Run full validation of all 34 DataFrame-compatible TPC-DS queries
"""

from pyspark.sql import SparkSession
from dataframe_validation_runner import TpcdsDataFrameValidationRunner
from tpcds_dataframe_queries import COMPATIBLE_QUERIES
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Run validation on all 34 DataFrame-compatible queries"""

    print("=" * 80)
    print("TPC-DS DataFrame API Full Validation")
    print("Testing All 34 DataFrame-Compatible Queries")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("TPC-DS-Full-Validation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        runner = TpcdsDataFrameValidationRunner(spark)

        # Run validation on all compatible queries
        print(f"\nRunning validation on {len(COMPATIBLE_QUERIES)} queries...")
        print(f"Queries: {COMPATIBLE_QUERIES}\n")

        results = runner.validate_all(query_nums=COMPATIBLE_QUERIES)

        # Display summary
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        passed_queries = []
        failed_queries = []

        for q_num in sorted(results['queries'].keys()):
            query_result = results['queries'][q_num]
            if query_result['success']:
                passed_queries.append(q_num)
                print(f"Query {q_num:3d}: ✅ PASS ({query_result['execution_time']:.2f}s)")
            else:
                failed_queries.append(q_num)
                error_msg = query_result.get('error', 'Unknown error')[:50]
                print(f"Query {q_num:3d}: ❌ FAIL - {error_msg}")

        print("\n" + "=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        print(f"Total queries tested: {results['total']}")
        print(f"Passed: {results['passed']} ({results['passed']/results['total']*100:.1f}%)")
        print(f"Failed: {results['failed']} ({results['failed']/results['total']*100:.1f}%)")

        if passed_queries:
            print(f"\nPassed queries: {passed_queries}")
        if failed_queries:
            print(f"\nFailed queries: {failed_queries}")

        # Save detailed results
        output_file = f"full_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nDetailed results saved to: {output_file}")

        # Success criteria
        success_rate = results['passed'] / results['total'] * 100
        if success_rate >= 80:
            print(f"\n✅ VALIDATION SUCCESSFUL! {success_rate:.1f}% pass rate")
        else:
            print(f"\n⚠️  VALIDATION NEEDS IMPROVEMENT. {success_rate:.1f}% pass rate")

    except Exception as e:
        print(f"Error during validation: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()

if __name__ == "__main__":
    main()