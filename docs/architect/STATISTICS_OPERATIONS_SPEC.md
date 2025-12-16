# Spark Statistics Operations Specification

**Author**: Claude (Research Agent)
**Date**: 2025-12-16
**Status**: Draft for Implementation Planning

## Overview

This document specifies the behavior and implementation details for Spark's statistical operations that are exposed through the DataFrame API. These operations are defined in the Spark Connect protocol as `Stat*` relation types (lines 90-97 in `relations.proto`).

All operations are accessed via the `df.stat` accessor or directly on DataFrame (e.g., `df.summary()`, `df.describe()`).

---

## 1. StatSummary (`df.summary()`)

### PySpark API

```python
DataFrame.summary(*statistics: str) -> DataFrame
```

**Parameters**:
- `statistics` (optional): Variable number of statistic names to compute. If not provided, computes default set.

**Available statistics**:
- `count` - Number of non-null values
- `mean` - Arithmetic mean (numeric columns only)
- `stddev` - Standard deviation (numeric columns only)
- `min` - Minimum value
- `max` - Maximum value
- `25%`, `50%`, `75%` - Percentiles (numeric columns only)
- `count_distinct` - Count of distinct values
- `approx_count_distinct` - Approximate count of distinct values

**Default behavior** (no arguments): Computes `count`, `mean`, `stddev`, `min`, `25%`, `50%`, `75%`, and `max`.

### Return Type

Returns a **DataFrame** (not a scalar value).

### Schema/Output Format

**Schema**:
- First column: `summary` (STRING) - contains the statistic name
- Subsequent columns: One column per input DataFrame column with same name and type STRING

**Example**:

Input DataFrame:
```
+----+------+--------+
| age| name | height |
+----+------+--------+
|  25| Alice|   165.5|
|  30|   Bob|   178.2|
|  28| Carol|   170.0|
+----+------+--------+
```

Output of `df.summary()`:
```
+-------+------------------+------+------------------+
|summary|               age|  name|            height|
+-------+------------------+------+------------------+
|  count|                 3|     3|                 3|
|   mean|27.666666666666668|  null|171.23333333333332|
| stddev| 2.516611478423583|  null| 6.409577068506485|
|    min|                25| Alice|             165.5|
|    25%|                25|  null|             165.5|
|    50%|                28|  null|             170.0|
|    75%|                30|  null|             178.2|
|    max|                30| Carol|             178.2|
+-------+------------------+------+------------------+
```

**Note**: Non-numeric columns return `null` for numeric statistics (mean, stddev, percentiles).

### Expected Behavior

1. **Column selection**: Operates on all columns in the DataFrame
2. **Null handling**: Nulls are excluded from calculations
3. **String columns**: Only `count`, `min`, and `max` are computed (lexicographic ordering)
4. **Type conversion**: All output values are returned as STRING type
5. **Percentiles**: Use approximate quantile algorithm (similar to `approxQuantile`)
6. **Edge cases**:
   - Empty DataFrame: Returns schema with 0 rows
   - All nulls in a column: Statistics return `null` (as string "null")

### Protocol Definition

From `relations.proto` (lines 615-634):

```protobuf
message StatSummary {
  Relation input = 1;           // Required: The input relation
  repeated string statistics = 2; // Optional: Statistics to compute
}
```

### DuckDB Implementation

Use `UNPIVOT` to transform columns to rows, then compute aggregates:

```sql
-- Step 1: For each numeric column, compute statistics
WITH stats AS (
  SELECT
    'count' AS summary,
    CAST(COUNT(age) AS VARCHAR) AS age,
    CAST(COUNT(name) AS VARCHAR) AS name,
    CAST(COUNT(height) AS VARCHAR) AS height
  FROM input_table

  UNION ALL

  SELECT
    'mean' AS summary,
    CAST(AVG(age) AS VARCHAR) AS age,
    NULL AS name,
    CAST(AVG(height) AS VARCHAR) AS height
  FROM input_table

  UNION ALL

  SELECT
    'stddev' AS summary,
    CAST(STDDEV_SAMP(age) AS VARCHAR) AS age,
    NULL AS name,
    CAST(STDDEV_SAMP(height) AS VARCHAR) AS height
  FROM input_table

  UNION ALL

  SELECT
    'min' AS summary,
    CAST(MIN(age) AS VARCHAR) AS age,
    CAST(MIN(name) AS VARCHAR) AS name,
    CAST(MIN(height) AS VARCHAR) AS height
  FROM input_table

  UNION ALL

  SELECT
    '25%' AS summary,
    CAST(QUANTILE_CONT(age, 0.25) AS VARCHAR) AS age,
    NULL AS name,
    CAST(QUANTILE_CONT(height, 0.25) AS VARCHAR) AS height
  FROM input_table

  -- Continue for 50%, 75%, max...
)
SELECT * FROM stats ORDER BY
  CASE summary
    WHEN 'count' THEN 1
    WHEN 'mean' THEN 2
    WHEN 'stddev' THEN 3
    WHEN 'min' THEN 4
    WHEN '25%' THEN 5
    WHEN '50%' THEN 6
    WHEN '75%' THEN 7
    WHEN 'max' THEN 8
  END;
```

**Alternative approach**: Use DuckDB's `SUMMARIZE` as a starting point, then transform output.

---

## 2. StatDescribe (`df.describe()`)

### PySpark API

```python
DataFrame.describe(*cols: str) -> DataFrame
```

**Parameters**:
- `cols` (optional): Column names to compute statistics for. If not provided, uses all numeric/string columns.

### Return Type

Returns a **DataFrame**.

### Schema/Output Format

**Schema**: Identical to `summary()`:
- First column: `summary` (STRING) - contains the statistic name
- Subsequent columns: One per selected column, type STRING

**Computed statistics** (fixed set):
1. `count` - Number of non-null values
2. `mean` - Arithmetic mean (numeric only)
3. `stddev` - Standard deviation (numeric only)
4. `min` - Minimum value
5. `max` - Maximum value

**Example**:

Input DataFrame:
```
+----+------+--------+
| age| name | height |
+----+------+--------+
|  25| Alice|   165.5|
|  30|   Bob|   178.2|
|  28| Carol|   170.0|
+----+------+--------+
```

Output of `df.describe()` or `df.describe("age", "height")`:
```
+-------+------------------+------------------+
|summary|               age|            height|
+-------+------------------+------------------+
|  count|                 3|                 3|
|   mean|27.666666666666668|171.23333333333332|
| stddev| 2.516611478423583| 6.409577068506485|
|    min|                25|             165.5|
|    max|                30|             178.2|
+-------+------------------+------------------+
```

### Expected Behavior

1. **Default columns**: If no columns specified, operates on all numeric and string columns
2. **Null handling**: Nulls are excluded from calculations
3. **String columns**: Computes `count`, `min`, `max` (lexicographic ordering); `mean` and `stddev` are `null`
4. **Type conversion**: All output values are STRING type
5. **Edge cases**:
   - Empty DataFrame: Returns schema with 0 rows
   - Non-existent column: Throws error
   - All nulls: Returns count=0, other stats as `null`

### Protocol Definition

From `relations.proto` (lines 636-645):

```protobuf
message StatDescribe {
  Relation input = 1;      // Required: The input relation
  repeated string cols = 2; // Optional: Columns to compute statistics on
}
```

### DuckDB Implementation

Similar to `summary()`, but with fixed statistics:

```sql
SELECT 'count' AS summary,
       CAST(COUNT(age) AS VARCHAR) AS age,
       CAST(COUNT(height) AS VARCHAR) AS height
FROM input_table

UNION ALL

SELECT 'mean' AS summary,
       CAST(AVG(age) AS VARCHAR) AS age,
       CAST(AVG(height) AS VARCHAR) AS height
FROM input_table

UNION ALL

SELECT 'stddev' AS summary,
       CAST(STDDEV_SAMP(age) AS VARCHAR) AS age,
       CAST(STDDEV_SAMP(height) AS VARCHAR) AS height
FROM input_table

UNION ALL

SELECT 'min' AS summary,
       CAST(MIN(age) AS VARCHAR) AS age,
       CAST(MIN(height) AS VARCHAR) AS height
FROM input_table

UNION ALL

SELECT 'max' AS summary,
       CAST(MAX(age) AS VARCHAR) AS age,
       CAST(MAX(height) AS VARCHAR) AS height
FROM input_table;
```

### Difference from `summary()`

- `describe()`: Fixed set of 5 statistics (count, mean, stddev, min, max)
- `summary()`: Configurable statistics including percentiles and count_distinct
- Both return same output format (first column named "summary", then data columns)

---

## 3. StatCrosstab (`df.stat.crosstab()`)

### PySpark API

```python
DataFrame.crosstab(col1: str, col2: str) -> DataFrame
# or
DataFrame.stat.crosstab(col1: str, col2: str) -> DataFrame
```

**Parameters**:
- `col1` (required): Name of the first column (becomes rows)
- `col2` (required): Name of the second column (becomes columns)

### Return Type

Returns a **DataFrame** (contingency table).

### Schema/Output Format

**Schema**:
- First column: `{col1}_{col2}` (STRING) - distinct values from col1
- Subsequent columns: One column per distinct value in col2, named with that value, type BIGINT (count)

**Example**:

Input DataFrame:
```
+---+---+
| c1| c2|
+---+---+
|  1| 11|
|  1| 11|
|  3| 10|
|  4|  8|
|  4|  8|
+---+---+
```

Output of `df.crosstab("c1", "c2")`:
```
+-----+---+---+---+
|c1_c2| 10| 11|  8|
+-----+---+---+---+
|    1|  0|  2|  0|
|    3|  1|  0|  0|
|    4|  0|  0|  2|
+-----+---+---+---+
```

### Expected Behavior

1. **Distinct values**: Computes distinct values for both columns
2. **Frequency counting**: Counts occurrences of each (col1, col2) pair
3. **Zero filling**: Pairs with no occurrences show count of 0
4. **Column ordering**: Columns are sorted by col2 values (typically ascending)
5. **Row ordering**: Rows are sorted by col1 values
6. **Limitations**:
   - Number of distinct values per column should be < 10,000
   - Maximum 1,000,000 non-zero pair frequencies
7. **Null handling**: Null values are treated as a distinct value

### Protocol Definition

From `relations.proto` (lines 647-663):

```protobuf
message StatCrosstab {
  Relation input = 1; // Required: The input relation
  string col1 = 2;    // Required: First column (rows)
  string col2 = 3;    // Required: Second column (columns)
}
```

### DuckDB Implementation

Use `PIVOT` operation:

```sql
-- DuckDB PIVOT syntax
PIVOT input_table
  ON col2
  USING COUNT(*) AS count
  GROUP BY col1
  ORDER BY col1;

-- Alternative: Manual approach with CASE statements
WITH distinct_col2 AS (
  SELECT DISTINCT col2 FROM input_table ORDER BY col2
),
crosstab AS (
  SELECT
    col1,
    SUM(CASE WHEN col2 = 10 THEN 1 ELSE 0 END) AS "10",
    SUM(CASE WHEN col2 = 11 THEN 1 ELSE 0 END) AS "11",
    SUM(CASE WHEN col2 = 8 THEN 1 ELSE 0 END) AS "8"
  FROM input_table
  GROUP BY col1
  ORDER BY col1
)
SELECT
  CAST(col1 AS VARCHAR) || '_' || 'col2' AS c1_c2,
  "10", "11", "8"
FROM crosstab;
```

**Note**: DuckDB's `PIVOT` is the most direct mapping. Need to dynamically generate column list from distinct values.

---

## 4. StatFreqItems (`df.stat.freqItems()`)

### PySpark API

```python
DataFrame.freqItems(cols: List[str], support: Optional[float] = None) -> DataFrame
# or
DataFrame.stat.freqItems(cols: List[str], support: Optional[float] = None) -> DataFrame
```

**Parameters**:
- `cols` (required): List of column names to find frequent items in
- `support` (optional): Minimum frequency threshold (default: 0.01 = 1%). Must be > 0.0001.

### Return Type

Returns a **DataFrame** with one row.

### Schema/Output Format

**Schema**:
- One column per input column, named `{colname}_freqItems`
- Type: ARRAY of the original column's type

**Example**:

Input DataFrame:
```
+---+---+
| c1| c2|
+---+---+
|  1| 11|
|  1| 11|
|  3| 10|
|  4|  8|
|  4|  8|
+---+---+
```

Output of `df.freqItems(["c1", "c2"])`:
```
+------------+------------+
|c1_freqItems|c2_freqItems|
+------------+------------+
|   [4, 1, 3]| [8, 11, 10]|
+------------+------------+
```

### Expected Behavior

1. **Algorithm**: Uses Karp-Schenker-Papadimitriou frequent element count algorithm
   - Reference: https://doi.org/10.1145/762471.762473
2. **False positives**: May include items that are not actually frequent (approximate)
3. **False negatives**: Will not miss truly frequent items
4. **Null handling**: Nulls are excluded from frequency counting
5. **Support threshold**:
   - Items appearing with frequency >= support are included
   - Default support = 0.01 (1%)
   - Minimum support = 0.0001 (0.01%)
6. **Array order**: No guaranteed order within the result arrays
7. **Edge cases**:
   - Empty DataFrame: Returns empty arrays
   - All values unique: May return empty arrays (if support threshold not met)

### Protocol Definition

From `relations.proto` (lines 720-733):

```protobuf
message StatFreqItems {
  Relation input = 1;        // Required: The input relation
  repeated string cols = 2;   // Required: Column names
  optional double support = 3; // Optional: Minimum frequency (default: 0.01)
}
```

### DuckDB Implementation

Approximate with simple frequency counting (exact, not approximate like Spark):

```sql
-- For column c1
WITH freq_c1 AS (
  SELECT c1, COUNT(*) * 1.0 / (SELECT COUNT(*) FROM input_table) AS frequency
  FROM input_table
  WHERE c1 IS NOT NULL
  GROUP BY c1
  HAVING frequency >= 0.01  -- support threshold
),
freq_c2 AS (
  SELECT c2, COUNT(*) * 1.0 / (SELECT COUNT(*) FROM input_table) AS frequency
  FROM input_table
  WHERE c2 IS NOT NULL
  GROUP BY c2
  HAVING frequency >= 0.01
)
SELECT
  ARRAY_AGG(c1) AS c1_freqItems,
  ARRAY_AGG(c2) AS c2_freqItems
FROM (
  SELECT (SELECT ARRAY_AGG(c1) FROM freq_c1) AS c1,
         (SELECT ARRAY_AGG(c2) FROM freq_c2) AS c2
);

-- Or using LIST aggregation (DuckDB syntax)
SELECT
  LIST(c1) AS c1_freqItems
FROM (
  SELECT c1
  FROM input_table
  WHERE c1 IS NOT NULL
  GROUP BY c1
  HAVING COUNT(*) * 1.0 / (SELECT COUNT(*) FROM input_table) >= 0.01
);
```

**Note**: This is an exact implementation. Spark uses an approximate streaming algorithm for efficiency on large datasets. For ThunderDuck with DuckDB's columnar storage, exact counting is likely fast enough.

---

## 5. StatSampleBy (`df.stat.sampleBy()`)

### PySpark API

```python
DataFrame.sampleBy(col: Union[str, Column],
                   fractions: Dict[Any, float],
                   seed: Optional[int] = None) -> DataFrame
# or
DataFrame.stat.sampleBy(col: Union[str, Column],
                        fractions: Dict[Any, float],
                        seed: Optional[int] = None) -> DataFrame
```

**Parameters**:
- `col` (required): Column that defines strata (groups)
- `fractions` (required): Dictionary mapping stratum values to sampling fractions [0.0, 1.0]
- `seed` (optional): Random seed for reproducibility

### Return Type

Returns a **DataFrame** (sampled subset of input).

### Schema/Output Format

**Schema**: Same as input DataFrame (all columns preserved).

**Example**:

Input DataFrame:
```
+------+---+
|  key | val|
+------+---+
|     A|  1|
|     A|  2|
|     A|  3|
|     A|  4|
|     B|  5|
|     B|  6|
|     C|  7|
|     C|  8|
+------+---+
```

Output of `df.sampleBy("key", {"A": 0.5, "B": 1.0}, seed=42)`:
```
+------+---+
|  key | val|
+------+---+
|     A|  1|  -- ~50% of A rows
|     A|  3|
|     B|  5|  -- 100% of B rows
|     B|  6|
+------+---+
```
(C rows not sampled because not in fractions dict)

### Expected Behavior

1. **Stratified sampling**: Samples each stratum (distinct value in `col`) independently
2. **Without replacement**: Each row can be selected at most once
3. **Fraction semantics**:
   - 0.0 = no rows selected
   - 1.0 = all rows selected
   - 0.5 = approximately half the rows selected
4. **Missing strata**: If a stratum value is not in `fractions` dict, fraction is treated as 0.0 (excluded)
5. **Seed**: If provided, sampling is deterministic and reproducible
6. **Null handling**: Null values in the stratification column are treated as a distinct stratum

### Protocol Definition

From `relations.proto` (lines 736-764):

```protobuf
message StatSampleBy {
  Relation input = 1;              // Required: The input relation
  Expression col = 2;              // Required: Stratification column
  repeated Fraction fractions = 3; // Required: Sampling fractions
  optional int64 seed = 5;         // Optional: Random seed

  message Fraction {
    Expression.Literal stratum = 1; // Required: Stratum value
    double fraction = 2;            // Required: Fraction [0, 1]
  }
}
```

### DuckDB Implementation

Use `RANDOM()` with seed for sampling:

```sql
-- Set seed for reproducibility
SELECT setseed(0.42);  -- Normalize seed to [0, 1]

-- Sample by strata
WITH stratified AS (
  SELECT *,
         RANDOM() AS rand
  FROM input_table
)
SELECT key, val
FROM stratified
WHERE (key = 'A' AND rand < 0.5)   -- 50% of A
   OR (key = 'B' AND rand < 1.0)   -- 100% of B
ORDER BY key, val;

-- Alternative: Use ROW_NUMBER for exact counts
WITH numbered AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY key ORDER BY RANDOM()) AS rn,
         COUNT(*) OVER (PARTITION BY key) AS total
  FROM input_table
)
SELECT key, val
FROM numbered
WHERE (key = 'A' AND rn <= CAST(total * 0.5 AS INTEGER))
   OR (key = 'B' AND rn <= CAST(total * 1.0 AS INTEGER));
```

**Note**: DuckDB's `RANDOM()` is seedable via `setseed()`. The seed must be normalized to [0, 1] range.

---

## 6. StatCov (`df.stat.cov()`)

### PySpark API

```python
DataFrame.cov(col1: str, col2: str) -> float
# or
DataFrame.stat.cov(col1: str, col2: str) -> float
```

**Parameters**:
- `col1` (required): Name of the first numeric column
- `col2` (required): Name of the second numeric column

### Return Type

Returns a **scalar DOUBLE value** (not a DataFrame).

### Expected Behavior

1. **Computation**: Calculates sample covariance (not population covariance)
   - Formula: `cov(X,Y) = Σ((xi - x̄)(yi - ȳ)) / (n - 1)`
   - Uses Bessel's correction (dividing by n-1)
2. **Null handling**: Rows where either column is null are excluded
3. **Type requirements**: Both columns must be numeric
4. **Edge cases**:
   - Less than 2 non-null pairs: Returns NaN
   - Empty DataFrame: Returns NaN
5. **Result interpretation**:
   - Positive: Variables tend to increase together
   - Negative: One increases as the other decreases
   - Zero: No linear relationship

### Protocol Definition

From `relations.proto` (lines 665-676):

```protobuf
message StatCov {
  Relation input = 1; // Required: The input relation
  string col1 = 2;    // Required: First column
  string col2 = 3;    // Required: Second column
}
```

### DuckDB Implementation

```sql
SELECT COVAR_SAMP(col1, col2) AS covariance
FROM input_table;

-- Equivalent manual calculation:
SELECT
  SUM((col1 - avg_col1) * (col2 - avg_col2)) / (COUNT(*) - 1) AS covariance
FROM input_table,
     (SELECT AVG(col1) AS avg_col1, AVG(col2) AS avg_col2
      FROM input_table
      WHERE col1 IS NOT NULL AND col2 IS NOT NULL) AS avgs
WHERE col1 IS NOT NULL AND col2 IS NOT NULL;
```

**Note**: DuckDB's `COVAR_SAMP()` computes sample covariance (matches Spark). `COVAR_POP()` would be population covariance (not what Spark uses).

### Implementation Note

Since this returns a scalar, not a DataFrame, the implementation will need to:
1. Execute the DuckDB query
2. Extract the single result value
3. Return it as part of the response (likely via a different response type than relations)

---

## 7. StatCorr (`df.stat.corr()`)

### PySpark API

```python
DataFrame.corr(col1: str, col2: str, method: str = "pearson") -> float
# or
DataFrame.stat.corr(col1: str, col2: str, method: str = "pearson") -> float
```

**Parameters**:
- `col1` (required): Name of the first numeric column
- `col2` (required): Name of the second numeric column
- `method` (optional): Correlation method, currently only "pearson" supported (default: "pearson")

### Return Type

Returns a **scalar DOUBLE value** (not a DataFrame).

### Expected Behavior

1. **Computation**: Calculates Pearson correlation coefficient
   - Formula: `corr(X,Y) = cov(X,Y) / (σx * σy)`
   - Range: [-1, 1]
2. **Null handling**: Rows where either column is null are excluded
3. **Type requirements**: Both columns must be numeric
4. **Method parameter**: Currently only "pearson" is supported (Spark limitation)
5. **Edge cases**:
   - Less than 2 non-null pairs: Returns NaN
   - Zero standard deviation in either variable: Returns NaN
   - Empty DataFrame: Returns NaN
6. **Result interpretation**:
   - 1.0: Perfect positive correlation
   - 0.0: No linear correlation
   - -1.0: Perfect negative correlation

### Protocol Definition

From `relations.proto` (lines 678-695):

```protobuf
message StatCorr {
  Relation input = 1;         // Required: The input relation
  string col1 = 2;            // Required: First column
  string col2 = 3;            // Required: Second column
  optional string method = 4; // Optional: Method (default: "pearson")
}
```

### DuckDB Implementation

```sql
SELECT CORR(col1, col2) AS correlation
FROM input_table;

-- DuckDB's CORR() computes Pearson correlation coefficient

-- Equivalent manual calculation:
SELECT
  COVAR_SAMP(col1, col2) / (STDDEV_SAMP(col1) * STDDEV_SAMP(col2)) AS correlation
FROM input_table
WHERE col1 IS NOT NULL AND col2 IS NOT NULL;
```

**Note**: DuckDB's `CORR()` function computes Pearson correlation by default (matches Spark).

### Implementation Note

Like `cov()`, this returns a scalar, not a DataFrame. For Spearman or Kendall correlation (if added in future):
- Spearman: Compute Pearson correlation on ranks
- Kendall: More complex, may require UDF or external implementation

---

## 8. StatApproxQuantile (`df.stat.approxQuantile()`)

### PySpark API

```python
DataFrame.approxQuantile(col: Union[str, List[str]],
                         probabilities: List[float],
                         relativeError: float) -> Union[List[float], List[List[float]]]
# or
DataFrame.stat.approxQuantile(col: Union[str, List[str]],
                              probabilities: List[float],
                              relativeError: float) -> Union[List[float], List[List[float]]]
```

**Parameters**:
- `col` (required): Column name(s) - single string or list of strings
- `probabilities` (required): List of quantile probabilities in [0, 1]
  - 0.0 = minimum, 0.5 = median, 1.0 = maximum
- `relativeError` (required): Relative target precision (>= 0)
  - 0.0 = exact quantiles (expensive)
  - Higher values allow more approximation

### Return Type

Returns **Python list(s)**, NOT a DataFrame:
- Single column: Returns `List[float]` (one value per probability)
- Multiple columns: Returns `List[List[float]]` (outer list per column, inner list per probability)

### Expected Behavior

1. **Algorithm**: Greenwald-Khanna approximate quantile algorithm
   - Reference: "Space-efficient Online Computation of Quantile Summaries"
2. **Deterministic bounds**:
   - For probability `p` and error `err`, returned value `x` satisfies:
   - `floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)`
3. **Null handling**: Null values are ignored
4. **All-null column**: Returns empty list `[]`
5. **Relative error**:
   - 0.0 = Exact quantiles (may be slow on large data)
   - 0.01 = Within 1% of true quantile
   - Values > 1.0 accepted but behave same as 1.0
6. **Edge cases**:
   - Empty DataFrame: Returns empty list
   - Single value: All quantiles return that value

### Protocol Definition

From `relations.proto` (lines 697-718):

```protobuf
message StatApproxQuantile {
  Relation input = 1;               // Required: The input relation
  repeated string cols = 2;          // Required: Column names
  repeated double probabilities = 3; // Required: Quantile probabilities [0, 1]
  double relative_error = 4;         // Required: Relative error (>= 0)
}
```

### DuckDB Implementation

```sql
-- Single column, multiple quantiles
SELECT QUANTILE_CONT(col1, [0.25, 0.5, 0.75]) AS quantiles
FROM input_table
WHERE col1 IS NOT NULL;

-- Multiple columns
SELECT
  QUANTILE_CONT(col1, [0.25, 0.5, 0.75]) AS col1_quantiles,
  QUANTILE_CONT(col2, [0.25, 0.5, 0.75]) AS col2_quantiles
FROM input_table;

-- For exact quantiles (relativeError = 0)
SELECT
  QUANTILE_CONT(col1, 0.5) AS median
FROM input_table
WHERE col1 IS NOT NULL;

-- DuckDB also has QUANTILE_DISC for discrete quantiles (actual values)
-- But Spark uses continuous interpolation, so use QUANTILE_CONT
```

**Note**:
- DuckDB's `QUANTILE_CONT()` uses interpolation (matches Spark's behavior)
- DuckDB computes exact quantiles (no approximation), which is acceptable for ThunderDuck since DuckDB is fast
- The `relativeError` parameter can be ignored or used as a hint to skip exact computation if error tolerance is high

### Implementation Note

This operation returns a scalar list value, not a DataFrame. The response must be formatted as:
- Single column: JSON array of doubles
- Multiple columns: JSON array of arrays

Example return values:
```python
df.approxQuantile("age", [0.25, 0.5, 0.75], 0.0)
# Returns: [25.0, 28.0, 30.0]

df.approxQuantile(["age", "height"], [0.25, 0.75], 0.0)
# Returns: [[25.0, 30.0], [165.5, 178.2]]
```

---

## Implementation Strategy

### Relation Types

These operations are encoded as `Relation` types in the Spark Connect protocol. The handler pattern should be:

1. **Detect relation type** in `RelationConverter` or similar dispatcher
2. **Extract parameters** from protobuf message
3. **Generate DuckDB SQL** based on operation and parameters
4. **Execute query** using QueryExecutor
5. **Return results**:
   - For DataFrame results: Return as Arrow batches (standard flow)
   - For scalar results (cov, corr, approxQuantile): Return as special response type

### Code Structure

Suggested Java class structure:

```java
public class StatisticsOperationHandler {

    // DataFrame-returning operations
    public VectorSchemaRoot handleStatSummary(StatSummary op, Session session);
    public VectorSchemaRoot handleStatDescribe(StatDescribe op, Session session);
    public VectorSchemaRoot handleStatCrosstab(StatCrosstab op, Session session);
    public VectorSchemaRoot handleStatFreqItems(StatFreqItems op, Session session);
    public VectorSchemaRoot handleStatSampleBy(StatSampleBy op, Session session);

    // Scalar-returning operations
    public double handleStatCov(StatCov op, Session session);
    public double handleStatCorr(StatCorr op, Session session);
    public List<Object> handleStatApproxQuantile(StatApproxQuantile op, Session session);
}
```

### Testing Strategy

1. **Differential tests**: Compare ThunderDuck output against PySpark for each operation
2. **Edge cases**: Empty DataFrames, all-null columns, single-value columns
3. **Type handling**: Test with different column types (int, double, string, etc.)
4. **Null handling**: Verify null values are handled correctly
5. **Exact matching**: Ensure output schemas match Spark exactly (column names, types, order)

---

## References

### PySpark Documentation
- [DataFrame.summary()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.summary.html)
- [DataFrame.describe()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.describe.html)
- [DataFrame.crosstab()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crosstab.html)
- [DataFrame.freqItems()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.freqItems.html)
- [DataFrame.sampleBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sampleBy.html)
- [DataFrame.cov()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cov.html)
- [DataFrame.corr()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.corr.html)
- [DataFrame.approxQuantile()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html)

### Academic References
- Frequent Elements Algorithm: [Karp, Schenker, Papadimitriou (2003)](https://doi.org/10.1145/762471.762473)
- Approximate Quantiles: Greenwald-Khanna algorithm in "Space-efficient Online Computation of Quantile Summaries"

### Related Documentation
- [Databricks: Statistical and Mathematical Functions](https://www.databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html)
- DuckDB Aggregate Functions: https://duckdb.org/docs/sql/functions/aggregates

---

## Appendix: Key Considerations

### 1. Type Conversions

All statistical operations convert results to STRING when returning DataFrames (e.g., `summary()`, `describe()`). This allows mixed-type results in a single column.

### 2. Null Handling

Nulls are consistently excluded from statistical calculations but may be counted in `count()` operations depending on semantics.

### 3. Performance Implications

- **Quantile calculations**: Can be expensive on large datasets (DuckDB is fast but exact)
- **Crosstab**: Limited to 10K distinct values per column (protocol constraint)
- **FreqItems**: Approximate algorithm in Spark, but exact counting is fine for ThunderDuck

### 4. Spark Compatibility

ThunderDuck must match Spark's behavior exactly:
- Same output schema (column names, types, order)
- Same statistical formulas (sample vs population)
- Same null handling
- Same edge case behavior (NaN, empty results, etc.)

### 5. DuckDB Advantages

DuckDB's columnar storage and vectorized execution make these operations fast:
- Aggregates (COUNT, AVG, STDDEV) are highly optimized
- PIVOT is native and efficient
- Quantile computation is exact and fast

---

**End of Specification**
