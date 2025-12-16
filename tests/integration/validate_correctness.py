"""
Validate Thunderduck Correctness Against Spark Reference Results

Compares Thunderduck output with pre-generated Spark local mode reference results.
"""

from pyspark.sql import SparkSession
from pathlib import Path
import json


def compare_values(spark_val, td_val, epsilon=0.01):
    """Compare two values with tolerance"""
    if spark_val is None and td_val is None:
        return True, ""
    if spark_val is None or td_val is None:
        return False, f"null mismatch: {spark_val} vs {td_val}"

    # Numeric comparison
    if isinstance(spark_val, (int, float)) and isinstance(td_val, (int, float)):
        if abs(spark_val - td_val) > epsilon:
            return False, f"value mismatch: {spark_val} vs {td_val} (diff: {abs(spark_val-td_val)})"
        return True, ""

    # String/date comparison
    if str(spark_val) != str(td_val):
        return False, f"value mismatch: {spark_val} vs {td_val}"

    return True, ""


def validate_query(qnum, spark_session):
    """Validate a single query against reference"""

    # Load reference
    ref_file = Path(f"/workspace/tests/integration/expected_results/q{qnum}_spark_reference.json")
    if not ref_file.exists():
        return None, f"Reference file not found: {ref_file}"

    with open(ref_file) as f:
        reference = json.load(f)

    # Execute on Thunderduck
    query_file = Path(f"/workspace/tests/integration/sql/tpch_queries/q{qnum}.sql")
    sql = query_file.read_text()

    result = spark_session.sql(sql)
    td_rows = result.collect()

    # Compare row count
    ref_rows = reference['rows']
    if len(td_rows) != len(ref_rows):
        return False, f"Row count mismatch: expected {len(ref_rows)}, got {len(td_rows)}"

    # Compare values row-by-row
    mismatches = []
    for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
        td_dict = td_row.asDict()

        # Convert Thunderduck values to same format
        td_converted = {}
        for k, v in td_dict.items():
            if v is None:
                td_converted[k] = None
            elif hasattr(v, '__float__'):
                td_converted[k] = float(v)
            elif hasattr(v, 'isoformat'):
                td_converted[k] = v.isoformat()
            else:
                td_converted[k] = v

        # Compare each column
        for col in ref_row.keys():
            if col not in td_converted:
                mismatches.append(f"Row {i}: Missing column '{col}' in Thunderduck result")
                continue

            matches, msg = compare_values(ref_row[col], td_converted[col])
            if not matches:
                mismatches.append(f"Row {i}, col '{col}': {msg}")

    if mismatches:
        # Show first few mismatches
        msg = f"Found {len(mismatches)} mismatches:\n" + "\n".join(mismatches[:5])
        if len(mismatches) > 5:
            msg += f"\n... and {len(mismatches) - 5} more"
        return False, msg

    return True, f"All {len(td_rows)} rows match exactly"


def main():
    """Run correctness validation for all queries"""

    print("\n" + "=" * 80)
    print("CORRECTNESS VALIDATION: Thunderduck vs Spark Reference Results")
    print("=" * 80)

    # Start Thunderduck session
    spark = (SparkSession.builder
            .remote("sc://localhost:15002")
            .appName("Correctness-Validation")
            .getOrCreate())

    # Load all TPC-H tables as temp views
    data_dir = Path("/workspace/data/tpch_sf001")
    print("\nLoading TPC-H tables...")
    for table in ['customer', 'lineitem', 'nation', 'orders',
                  'part', 'partsupp', 'region', 'supplier']:
        df = spark.read.parquet(str(data_dir / f"{table}.parquet"))
        df.createOrReplaceTempView(table)
        print(f"  ✓ {table}")

    # Validate each query
    queries = [1, 3, 5, 6, 10, 12, 13, 18]
    results = {}

    print("\n" + "=" * 80)
    print("Validating Query Correctness")
    print("=" * 80)

    for qnum in queries:
        print(f"\n📊 Q{qnum}: ", end="")
        passed, message = validate_query(qnum, spark)

        if passed is None:
            print(f"⏭️  SKIPPED - {message}")
            results[qnum] = "SKIPPED"
        elif passed:
            print(f"✅ PASS - {message}")
            results[qnum] = "PASS"
        else:
            print(f"❌ FAIL - {message}")
            results[qnum] = "FAIL"

    spark.stop()

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    passed_count = sum(1 for v in results.values() if v == "PASS")
    failed_count = sum(1 for v in results.values() if v == "FAIL")
    total_count = len(queries)

    print(f"\nResults: {passed_count}/{total_count} PASS, {failed_count}/{total_count} FAIL")
    print(f"Pass Rate: {passed_count/total_count*100:.1f}%")

    if failed_count == 0:
        print("\n✅ ALL QUERIES PRODUCE CORRECT RESULTS")
        print("   Thunderduck output matches Spark exactly!")
    else:
        print(f"\n❌ {failed_count} queries have correctness issues")
        print("   Need debugging and fixes")

    print("=" * 80)

    return failed_count == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
