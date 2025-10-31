#!/usr/bin/env python3
"""
Test the 6 previously failing queries after fixes
"""

from pyspark.sql import SparkSession
from dataframe_validation_runner import TpcdsDataFrameValidationRunner

def main():
    """Test only the queries we just fixed"""

    print("=" * 80)
    print("Testing Fixed Queries")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("TPC-DS-Fixed-Queries-Test") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    try:
        runner = TpcdsDataFrameValidationRunner(spark)

        # Test only the 6 queries we fixed
        fixed_queries = [9, 32, 42, 72, 91, 92]

        print(f"\nTesting {len(fixed_queries)} fixed queries: {fixed_queries}\n")

        results = runner.validate_all(query_nums=fixed_queries)

        # Display results
        print("\n" + "=" * 80)
        print("RESULTS FOR FIXED QUERIES")
        print("=" * 80)

        for q_num in sorted(results['queries'].keys()):
            query_result = results['queries'][q_num]
            if query_result['success']:
                print(f"Query {q_num:3d}: ✅ PASS ({query_result['execution_time']:.2f}s)")
            else:
                error_msg = query_result.get('error', 'Unknown error')
                print(f"Query {q_num:3d}: ❌ FAIL - {error_msg}")

        print(f"\nTotal: {results['total']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")

        if results['failed'] == 0:
            print("\n✅ ALL FIXES SUCCESSFUL!")
        else:
            print(f"\n⚠️  Still have {results['failed']} failing queries")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()

if __name__ == "__main__":
    main()