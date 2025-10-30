# Week 17: TPC-DS DataFrame API Implementation Strategy

## Two-Step Validation Approach

### Overview
To ensure correctness and handle potential ordering differences, we'll use a two-step validation process:

1. **Step 1**: Validate our DataFrame API implementations against SQL references (Spark-only)
2. **Step 2**: Compare ThunderDuck DataFrame results with validated Spark references

This approach isolates implementation correctness from compatibility issues.

## Step 1: DataFrame API Implementation & Validation (Days 1-3)

### Objective
Implement all 100 TPC-DS queries using Spark DataFrame API and validate against existing SQL reference results.

### Implementation Framework

```python
class TpcdsDataFrameQueries:
    """All TPC-DS queries implemented in DataFrame API"""

    @staticmethod
    def q1(spark):
        """TPC-DS Query 1: Customer returns analysis"""
        store_returns = spark.table("store_returns")
        date_dim = spark.table("date_dim")
        store = spark.table("store")
        customer = spark.table("customer")

        # Implementation using DataFrame operations
        result = store_returns.alias("sr") \
            .join(date_dim.alias("d"),
                  col("sr.sr_returned_date_sk") == col("d.d_date_sk")) \
            .filter(col("d.d_year") == 2000) \
            .join(store.alias("s"),
                  col("sr.sr_store_sk") == col("s.s_store_sk")) \
            .join(customer.alias("c"),
                  col("sr.sr_customer_sk") == col("c.c_customer_sk")) \
            .groupBy("c.c_customer_id") \
            .agg(sum("sr.sr_return_amt").alias("total_return")) \
            # ... rest of query logic

        return result
```

### Validation Approach

```python
class DataFrameValidator:
    def validate_against_sql(self, query_num: int, spark):
        """Validate DataFrame implementation against SQL reference"""

        # 1. Run SQL version (reference)
        sql_result = self.run_sql_query(query_num, spark)

        # 2. Run DataFrame version
        df_result = self.run_dataframe_query(query_num, spark)

        # 3. Compare with order-independent matching
        comparison = self.compare_results(sql_result, df_result)

        if not comparison.matches:
            # Log detailed differences for debugging
            self.log_differences(query_num, comparison)

        return comparison

    def compare_results(self, expected, actual):
        """Order-independent comparison"""
        # Handle tie-breakers by sorting all columns
        expected_sorted = self.normalize_and_sort(expected)
        actual_sorted = self.normalize_and_sort(actual)

        return ResultComparison(
            row_count_matches=expected.count() == actual.count(),
            schema_matches=self.schemas_equal(expected, actual),
            data_matches=self.data_equal(expected_sorted, actual_sorted),
            differences=self.get_differences(expected_sorted, actual_sorted)
        )

    def normalize_and_sort(self, df):
        """Normalize data types and sort by all columns"""
        # Handle floating point precision
        normalized = self.normalize_floats(df)
        # Sort by all columns to handle tie-breakers
        return normalized.orderBy(*normalized.columns)
```

### Handling Common Issues

#### 1. Ordering Tie-Breakers
```python
def handle_order_ties(df, order_cols):
    """Add additional columns to make ordering deterministic"""
    # If ORDER BY has ties, add more columns
    all_cols = df.columns
    remaining_cols = [c for c in all_cols if c not in order_cols]
    return df.orderBy(*(order_cols + remaining_cols))
```

#### 2. Floating Point Precision
```python
def compare_floats(expected, actual, epsilon=1e-6):
    """Compare with tolerance for floating point"""
    return abs(expected - actual) < epsilon
```

#### 3. NULL Handling
```python
def normalize_nulls(df):
    """Ensure consistent NULL representation"""
    # Replace empty strings with NULL where appropriate
    # Handle NULL ordering consistently
    return df.na.fill({"string_col": None})
```

## Step 2: ThunderDuck Validation (Days 4-5)

### Objective
Run validated DataFrame implementations on ThunderDuck and compare with Spark reference results.

### Execution Framework

```python
class ThunderDuckValidator:
    def __init__(self):
        self.spark_session = self.create_spark_session()
        self.thunderduck_session = self.create_thunderduck_session()

    def validate_query(self, query_num: int):
        """Compare ThunderDuck with validated Spark results"""

        # 1. Get validated Spark DataFrame result
        spark_result = self.run_validated_query(query_num, self.spark_session)

        # 2. Run same DataFrame code on ThunderDuck
        thunderduck_result = self.run_validated_query(query_num, self.thunderduck_session)

        # 3. Compare with same tolerance as Step 1
        return self.compare_with_tolerance(spark_result, thunderduck_result)
```

### Discrepancy Categories

1. **Type Mismatches**
   - Different numeric types (BIGINT vs INT)
   - String vs NULL handling
   - Date/timestamp precision

2. **Function Differences**
   - Function name mappings
   - Parameter order differences
   - Default behavior variations

3. **Optimization Differences**
   - Join reordering
   - Predicate pushdown variations
   - Aggregation strategies

## Implementation Schedule

### Day 1: Infrastructure & Queries 1-33
- [ ] Create test framework with order-independent comparison
- [ ] Implement TPC-DS Q1-Q33 in DataFrame API
- [ ] Validate against SQL references

### Day 2: Queries 34-66
- [ ] Implement TPC-DS Q34-Q66 in DataFrame API
- [ ] Handle complex window functions
- [ ] Address any validation failures from Day 1

### Day 3: Queries 67-100 & Validation
- [ ] Implement TPC-DS Q67-Q100 in DataFrame API
- [ ] Complete all SQL reference validations
- [ ] Document any queries requiring special handling

### Day 4: ThunderDuck Testing
- [ ] Run all validated queries on ThunderDuck
- [ ] Categorize and document discrepancies
- [ ] Implement fixes for critical issues

### Day 5: Final Validation & Report
- [ ] Re-run all queries after fixes
- [ ] Generate compatibility matrix
- [ ] Document workarounds and limitations

## Query Categories by Complexity

### Simple (Direct Translation)
- Basic SELECT, WHERE, GROUP BY
- Simple joins
- Standard aggregations

### Medium (Require Careful Handling)
- Window functions
- Complex joins with multiple conditions
- CASE/WHEN expressions
- Date arithmetic

### Complex (May Need Workarounds)
- Correlated subqueries
- ROLLUP/CUBE operations
- Complex EXISTS/NOT EXISTS patterns
- Recursive CTEs (if present)

## Success Criteria

### Step 1 Success (Spark Validation)
- [ ] 100% of queries implemented in DataFrame API
- [ ] 95%+ validation against SQL references
- [ ] Clear documentation of any deviations

### Step 2 Success (ThunderDuck Compatibility)
- [ ] 90%+ queries produce identical results
- [ ] All discrepancies documented with root cause
- [ ] Workarounds provided for incompatible queries

## Tools and Utilities

### Result Comparison Utility
```python
def create_comparison_report(query_num, spark_result, thunderduck_result):
    """Generate detailed comparison report"""
    report = {
        "query": query_num,
        "status": "PASS/FAIL",
        "row_count": {
            "spark": spark_result.count(),
            "thunderduck": thunderduck_result.count()
        },
        "schema_match": compare_schemas(spark_result, thunderduck_result),
        "data_differences": find_data_differences(spark_result, thunderduck_result),
        "performance": {
            "spark_time_ms": spark_execution_time,
            "thunderduck_time_ms": thunderduck_execution_time
        }
    }
    return report
```

### Automated Test Runner
```python
class TpcdsTestRunner:
    def run_all_tests(self):
        results = []
        for query_num in range(1, 101):
            try:
                # Step 1: Validate DataFrame against SQL
                sql_validation = self.validate_dataframe_implementation(query_num)

                if sql_validation.passed:
                    # Step 2: Test on ThunderDuck
                    thunderduck_result = self.test_on_thunderduck(query_num)
                    results.append(thunderduck_result)
                else:
                    results.append({"query": query_num, "status": "IMPLEMENTATION_ERROR"})

            except Exception as e:
                results.append({"query": query_num, "status": "EXCEPTION", "error": str(e)})

        return self.generate_report(results)
```

## Expected Challenges & Solutions

### Challenge 1: Complex SQL Patterns
**Problem**: Some TPC-DS queries use advanced SQL features difficult to express in DataFrame API
**Solution**:
- Use `spark.sql()` for specific subqueries
- Combine DataFrame and SQL approaches
- Document patterns for future reference

### Challenge 2: Performance Timeouts
**Problem**: Large queries may timeout during validation
**Solution**:
- Increase timeout thresholds
- Use sampling for initial validation
- Optimize DataFrame operations

### Challenge 3: Precision Differences
**Problem**: Floating point calculations may differ slightly
**Solution**:
- Use configurable epsilon for comparisons
- Round results to reasonable precision
- Document acceptable variations

## Deliverables

### Code Artifacts
1. `/workspace/tests/integration/tpcds_dataframe/queries.py` - All 100 implementations
2. `/workspace/tests/integration/tpcds_dataframe/validator.py` - Validation framework
3. `/workspace/tests/integration/tpcds_dataframe/comparison.py` - Comparison utilities

### Documentation
1. `TPCDS_DATAFRAME_IMPLEMENTATION.md` - Implementation notes
2. `VALIDATION_RESULTS.md` - Detailed results from both steps
3. `COMPATIBILITY_MATRIX.md` - ThunderDuck compatibility summary

### Reports
1. Step 1: DataFrame vs SQL validation report
2. Step 2: ThunderDuck vs Spark comparison report
3. Performance benchmarks
4. Discrepancy analysis with root causes

## Conclusion

This two-step approach ensures:
1. **Correctness**: Our DataFrame implementations are validated against known-good SQL
2. **Compatibility**: ThunderDuck differences are isolated from implementation issues
3. **Clarity**: Clear separation of concerns makes debugging easier
4. **Documentation**: Comprehensive records for future reference

By the end of Week 17, we'll have definitive proof of ThunderDuck's DataFrame API compatibility and a clear understanding of any limitations.

---
**Created**: October 30, 2025
**Status**: PLANNED
**Owner**: ThunderDuck Team