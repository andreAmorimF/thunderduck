# Statistical Functions: Spark vs DuckDB Analysis

## Executive Summary

Three statistical function families have formula mismatches between Spark and DuckDB:

| Function | Root Cause | Severity | Fix Complexity |
|----------|-----------|----------|----------------|
| **percentile** | Wrong DuckDB function mapping (`quantile_disc` instead of `quantile_cont`) | HIGH - wrong values | LOW - one-line mapping change |
| **kurtosis** | DuckDB uses sample/bias-corrected; Spark uses population/uncorrected | MEDIUM - different formula | LOW - remap to `kurtosis_pop` |
| **skewness** | DuckDB uses sample/bias-corrected; Spark uses population/uncorrected | MEDIUM - different formula | MEDIUM - no DuckDB built-in match |

---

## 1. Percentile (`percentile`)

### Spark's Formula

Spark's `percentile(col, p)` computes the **exact percentile with linear interpolation**:

1. Compute position: `pos = p * (N - 1)` where N = count of values
2. If `pos` is integral, return value at that index
3. If fractional: `result = lower + (upper - lower) * (pos - floor(pos))`

This is the standard C=1 linear interpolation method (same as NumPy's default, R's type=7).

### DuckDB's Functions

DuckDB has two quantile functions:

| Function | Method | Description |
|----------|--------|-------------|
| `quantile_disc(x, p)` | Discrete | Returns actual value at nearest rank. Aliased as `quantile`. |
| `quantile_cont(x, p)` | Continuous | Linear interpolation: `RN = (N-1)*p`, interpolate between `floor(RN)` and `ceil(RN)` |

**DuckDB's `quantile_cont` interpolation** (from `QuantileInterpolator<false>`):
```cpp
RN = (double)(n - 1) * q.dbl;
FRN = floor(RN);
CRN = ceil(RN);
// If FRN != CRN:
result = lo + (hi - lo) * (RN - FRN);
```

This is **identical** to Spark's formula.

### Current Bug

Thunderduck maps `percentile` to `quantile` (which is `quantile_disc`):
```java
// FunctionRegistry.java:1000
DIRECT_MAPPINGS.put("percentile", "quantile");
```

This gives the **wrong answer** because `quantile_disc` returns the nearest rank value without interpolation.

### Fix (Both Modes)

Simply change the mapping:
```java
DIRECT_MAPPINGS.put("percentile", "quantile_cont");
```

No extension function needed. `quantile_cont` uses the identical linear interpolation formula as Spark.

**Complexity: LOW** -- one-line change in FunctionRegistry.

---

## 2. Kurtosis (`kurtosis`)

### Spark's Formula

Spark's `kurtosis()` is defined in `CentralMomentAgg.scala`:

```
kurtosis = n * m4 / (m2 * m2) - 3.0
```

Where m2 and m4 are **un-normalized** central moment sums (NOT divided by n):
- `m2 = SUM((x_i - mean)^2)`
- `m4 = SUM((x_i - mean)^4)`

Substituting population moments (`mu_2 = m2/n`, `mu_4 = m4/n`):
```
kurtosis = n * (n * mu_4) / (n * mu_2)^2 - 3
         = n^2 * mu_4 / (n^2 * mu_2^2) - 3
         = mu_4 / mu_2^2 - 3
```

This is the **population excess kurtosis** (Fisher's definition, no bias correction). NULL when n=0; NaN (or null, depending on config) when m2=0.

### DuckDB's Functions

DuckDB has two kurtosis functions:

| Function | Formula | Type |
|----------|---------|------|
| `kurtosis(x)` | `(n-1)*((n+1)*m4/(m2*m2) - 3*(n-1)) / ((n-2)*(n-3))` | **Sample** excess kurtosis (bias-corrected) |
| `kurtosis_pop(x)` | `m4 / (m2 * m2) - 3` | **Population** excess kurtosis (no bias correction) |

Where DuckDB computes m2 and m4 as population moments (divided by n):
```cpp
m4 = (1/n) * (sum_four - 4*sum_cub*sum/n + 6*sum_sqr*sum^2/n^2 - 3*sum^4/n^3)
m2 = (1/n) * (sum_sqr - sum^2/n)
```

### Analysis

**DuckDB `kurtosis_pop` = Spark `kurtosis`** (both compute population excess kurtosis).

DuckDB `kurtosis` uses the sample/bias-corrected formula, which is different.

### Edge Case Differences

| Condition | Spark | DuckDB `kurtosis_pop` |
|-----------|-------|----------------------|
| n = 0 | NULL | NULL (n <= 1 -> NULL) |
| n = 1 | NULL (m2=0 -> divideByZero) | NULL (n <= 1) |
| n = 2, m2 > 0 | Computes value | Computes value |
| m2 = 0 | NULL or NaN (config) | NULL |

Minor difference: Spark may return NaN when `m2=0` if `legacyStatisticalAggregate=true`, while DuckDB always returns NULL. In practice, the default Spark config (since 3.x) returns NULL for divide-by-zero, matching DuckDB.

### Fix

**Relaxed mode**: Remap `kurtosis` to `kurtosis_pop`:
```java
DIRECT_MAPPINGS.put("kurtosis", "kurtosis_pop");
```

**Strict mode**: Same -- `kurtosis_pop` already exists in DuckDB core. No extension function needed.

**Complexity: LOW** -- one-line change in FunctionRegistry.

---

## 3. Skewness (`skewness`)

### Spark's Formula

Spark's `skewness()` from `CentralMomentAgg.scala`:

```
skewness = sqrt(n) * m3 / sqrt(m2 * m2 * m2)
         = sqrt(n) * m3 / m2^(3/2)
```

Where m2 and m3 are un-normalized sums. Substituting population moments:
```
skewness = sqrt(n) * (n * mu_3) / (n * mu_2)^(3/2)
         = sqrt(n) * n * mu_3 / (n^(3/2) * mu_2^(3/2))
         = mu_3 / mu_2^(3/2)
```

This is the **population skewness** (g_1, biased estimator, no correction).

### DuckDB's Formula

DuckDB has only one skewness function: `skewness(x)`, which computes the **sample skewness** (G_1, bias-corrected):

```cpp
temp1 = sqrt(n * (n - 1)) / (n - 2);
result = temp1 * m3_pop / m2_pop^(3/2);
```

Where `m3_pop` and `m2_pop` are population central moments (computed inline from sums).

The correction factor is `sqrt(n*(n-1)) / (n-2)`.

### Relationship

```
DuckDB_skewness = sqrt(n*(n-1)) / (n-2) * Spark_skewness
```

For n=100: correction factor = 1.0153 (1.5% difference)
For n=10: correction factor = 1.0607 (6% difference)
For n=5: correction factor = 1.4907 (49% difference)

**There is no `skewness_pop` function in DuckDB.** Unlike kurtosis (which has `kurtosis_pop`), DuckDB only exposes the sample/bias-corrected skewness.

### Edge Case Differences

| Condition | Spark | DuckDB `skewness` |
|-----------|-------|-------------------|
| n = 0 | NULL | NULL (n <= 2) |
| n = 1 | NULL (m2=0 -> divideByZero) | NULL (n <= 2) |
| n = 2 | Computes value | NULL (n <= 2) |
| m2 = 0 | NULL or NaN (config) | NaN |

Notable: Spark returns a value for n=2, DuckDB returns NULL. DuckDB returns NaN when variance is zero, Spark returns NULL (default config).

### Fix Options

#### Option A: Pure-SQL workaround (Relaxed mode)

The population skewness can be computed as a SQL expression, but it requires access to individual row values, not just aggregates. This can NOT be done with a simple function remapping.

**Approach**: Use a CTE/subquery pattern:
```sql
-- For a simple ungrouped case:
SELECT
  CASE
    WHEN COUNT(x) = 0 THEN NULL
    WHEN VAR_POP(x) = 0 THEN NULL
    ELSE (SUM(POWER((x - sub.mean_x) / sub.stddev_x, 3))) / COUNT(x)
  END AS skewness
FROM t,
  (SELECT AVG(x) AS mean_x, STDDEV_POP(x) AS stddev_x FROM t) sub
```

But this is extremely complex for general use:
1. Requires rewriting the aggregate into a subquery
2. Grouped queries make this much harder
3. Window function variants are essentially impossible

**Not practical for relaxed mode.**

#### Option B: DuckDB extension function `spark_skewness()` (Strict mode)

Add a new aggregate function `spark_skewness()` to the Thunderduck DuckDB extension. This is the clean solution.

**Implementation approach**: Copy DuckDB's existing `SkewnessOperation` and modify the Finalize method to use the population formula instead of the sample formula:

```cpp
// Change from:
double temp1 = std::sqrt(n * (n - 1)) / (n - 2);
target = temp1 * temp * (...) / div;

// To:
target = temp * (...) / div;
// (simply remove the temp1 correction factor)
```

The accumulation logic (sum, sum_sqr, sum_cub) is identical. Only the Finalize differs.

**Complexity: LOW-MEDIUM** -- follow the same pattern as `spark_sum`/`spark_avg` in the existing extension. The aggregate state and update operations are identical to DuckDB's built-in `skewness`; only the finalize step changes.

#### Option C: SQL correction factor (Relaxed mode alternative)

Since the relationship is a simple multiplicative factor:
```
spark_skew = duckdb_skew * (n - 2) / sqrt(n * (n - 1))
```

We could map `skewness(x)` to a SQL expression:
```sql
skewness(x) * (COUNT(x) - 2) / SQRT(COUNT(x) * (COUNT(x) - 1))
```

But this requires knowing `n` (the count), which means:
1. For simple aggregations: we'd need to add COUNT to the query
2. For grouped aggregations: each group has different n
3. This would require deep SQL generation changes to inject COUNT alongside skewness

**Feasible but non-trivial for relaxed mode.** Could be done if the SQL generator emits a wrapper expression around `skewness()`.

### Recommended Approach

1. **Strict mode**: Add `spark_skewness()` extension function (most correct, cleanest)
2. **Relaxed mode**: Use the correction factor approach `skewness(x) * (count(x) - 2) / sqrt(count(x) * (count(x) - 1))` or accept the difference as a known limitation

---

## 4. Percentile_Approx (`percentile_approx`)

### Spark's Implementation

Spark's `percentile_approx` uses the Greenwald-Khanna (G-K) algorithm via `QuantileSummaries`. It takes an accuracy parameter (default 10000).

### DuckDB's Implementation

DuckDB's `approx_quantile` uses T-Digest (a different approximate quantile algorithm). It does NOT accept an accuracy parameter.

### Current Mapping

```java
CUSTOM_TRANSLATORS.put("percentile_approx", args -> {
    return "approx_quantile(" + args[0] + ", " + args[1] + ")";
});
```

The 3rd argument (accuracy) is silently dropped.

### Analysis

Both algorithms produce approximate results. The values will differ somewhat due to different algorithms, but both are "approximate" by design. The accuracy parameter mismatch means users can't control precision the same way.

### Fix

**Relaxed mode**: Keep current mapping -- approximate results are acceptable by definition. Document that accuracy parameter is ignored.

**Strict mode**: Not feasible to match exactly -- different algorithms produce inherently different approximations. Could implement G-K in the extension, but this is extremely high complexity for marginal benefit.

**Complexity: N/A (relaxed) / VERY HIGH (strict, not recommended)**

---

## 5. Summary Table

| Function | Spark Formula | DuckDB Formula | Difference | Relaxed Mode Strategy | Strict Mode Strategy | Complexity |
|----------|--------------|----------------|-----------|----------------------|---------------------|------------|
| `percentile` | Linear interpolation: `lo + (hi-lo)*(pos-floor(pos))` where `pos = p*(N-1)` | `quantile_disc`: nearest rank (current mapping) / `quantile_cont`: same linear interpolation | **Wrong function mapped** | Remap to `quantile_cont` | Remap to `quantile_cont` | **LOW** |
| `kurtosis` | Population excess: `mu_4/mu_2^2 - 3` | `kurtosis`: sample excess (bias-corrected) / `kurtosis_pop`: population excess | **Bias correction** | Remap to `kurtosis_pop` | Remap to `kurtosis_pop` | **LOW** |
| `skewness` | Population: `mu_3/mu_2^(3/2)` | `skewness`: sample (bias-corrected by `sqrt(n(n-1))/(n-2)`) | **Bias correction, no pop variant in DuckDB** | Accept diff OR correction factor SQL | Extension: `spark_skewness()` | **MEDIUM** |
| `percentile_approx` | G-K algorithm with accuracy param | T-Digest (different algorithm) | Different algorithms | Keep current (approx is approx) | Not feasible | **N/A** |

---

## 6. Detailed DuckDB Source Analysis

### 6.1 Kurtosis Source (`kurtosis.cpp`)

**State**: Accumulates `n`, `sum`, `sum_sqr`, `sum_cub`, `sum_four`.

**Population moments** (computed in Finalize):
```cpp
m4 = (1/n) * (sum_four - 4*sum_cub*sum/n + 6*sum_sqr*sum^2/n^2 - 3*sum^4/n^3)
m2 = (1/n) * (sum_sqr - sum^2/n)
```

**`kurtosis_pop` (NoBiasCorrection)**:
```cpp
target = m4 / (m2 * m2) - 3;
```
Returns NULL when n <= 1 or m2 = 0.

**`kurtosis` (BiasCorrection)**:
```cpp
target = (n - 1) * ((n + 1) * m4 / (m2 * m2) - 3 * (n - 1)) / ((n - 2) * (n - 3));
```
Returns NULL when n <= 3 or m2 = 0.

**Registration**:
- `kurtosis` -> `KurtosisFlagBiasCorrection` (sample/corrected)
- `kurtosis_pop` -> `KurtosisFlagNoBiasCorrection` (population/uncorrected)

### 6.2 Skewness Source (`skew.cpp`)

**State**: Accumulates `n`, `sum`, `sum_sqr`, `sum_cub`.

**Finalize**:
```cpp
double n = state.n;
double temp = 1.0 / n;
auto p = pow(temp * (sum_sqr - sum * sum * temp), 3);  // variance_pop^3
double div = sqrt(p);                                     // stddev_pop^3
double temp1 = sqrt(n * (n - 1)) / (n - 2);              // bias correction factor
target = temp1 * temp * (sum_cub - 3*sum_sqr*sum*temp + 2*pow(sum,3)*temp*temp) / div;
```

This simplifies to:
```
target = sqrt(n*(n-1))/(n-2) * m3_pop / m2_pop^(3/2)
```

**No `skewness_pop` variant exists** in DuckDB.

Returns NULL when n <= 2. Returns NaN when variance = 0.

### 6.3 Quantile Source (`quantile.cpp`)

**`quantile_cont` interpolation** (`QuantileInterpolator<false>`):
```cpp
RN = (double)(n - 1) * q.dbl;   // position in sorted array
FRN = floor(RN);                  // lower index
CRN = ceil(RN);                   // upper index
// If FRN == CRN: return value[FRN]
// Else: return lo + (hi - lo) * (RN - FRN)   // linear interpolation
```

**`quantile_disc` interpolation** (`QuantileInterpolator<true>`):
```cpp
// Returns value at index: max(1, n - floor(n - n*q)) - 1
// No interpolation
```

**InterpolateOperator**:
```cpp
template <typename TARGET_TYPE>
static inline TARGET_TYPE Operation(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
    const auto delta = static_cast<double>(hi - lo);
    return lo + static_cast<TARGET_TYPE>(delta * d);
}
```
Standard linear interpolation: `lo + (hi - lo) * d`.

---

## 7. Extension Feasibility Assessment

### Existing Extension Patterns

The Thunderduck DuckDB extension (`thdck_spark_funcs`) currently implements:
- **Scalar**: `spark_decimal_div` (decimal division with Spark rounding)
- **Aggregate**: `spark_sum` (DECIMAL + integer paths), `spark_avg` (DECIMAL path)

Aggregate function pattern uses:
1. State struct with accumulator fields
2. Operation struct with Initialize/Operation/ConstantOperation/Combine/Finalize
3. Bind function for type resolution
4. Factory function creating AggregateFunctionSet
5. Registration in `LoadInternal()`

### Adding `spark_skewness()`

**Effort**: LOW-MEDIUM (estimated 2-4 hours)

The implementation would:
1. Copy DuckDB's `SkewState` and `SkewnessOperation` structures
2. Modify `Finalize` to remove the `sqrt(n*(n-1))/(n-2)` correction factor
3. Use population formula instead: `target = m3_pop / m2_pop^(3/2)`
4. Handle edge cases to match Spark (NULL when n=0 or n=1 or m2=0)
5. Register as `spark_skewness` in the extension

**No new accumulation logic needed** -- the state and update operations are identical to DuckDB's built-in. Only Finalize changes.

### Adding `spark_kurtosis()`

**Not needed** -- DuckDB's `kurtosis_pop` already matches Spark exactly. Just remap.

### Adding `spark_percentile()`

**Not needed** -- DuckDB's `quantile_cont` already matches Spark exactly. Just remap.

---

## 8. Recommended Action Plan

### Phase 1: Quick Wins (Relaxed + Strict mode)

1. **Fix `percentile` mapping**: Change `"quantile"` to `"quantile_cont"` in FunctionRegistry
2. **Fix `kurtosis` mapping**: Change `"kurtosis"` to `"kurtosis_pop"` in FunctionRegistry

Both changes are one-line fixes that work in both relaxed and strict modes.

### Phase 2: Skewness Extension (Strict mode)

3. **Add `spark_skewness()` to extension**: New aggregate function with population skewness formula
4. **Update FunctionRegistry**: Map `skewness` to `spark_skewness` when in strict mode, keep `skewness` mapping in relaxed mode (accept the bias-correction difference as known limitation)

### Phase 3: Optional Relaxed Mode Skewness

5. **SQL correction factor**: For relaxed mode, consider generating `skewness(x) * (count(x) - 2) / sqrt(count(x) * (count(x) - 1))` -- but this adds complexity to SQL generation for grouped/window contexts.

### Items NOT Recommended

- Implementing G-K algorithm for `percentile_approx` in the extension (too complex, approximate by nature)
- Adding `spark_kurtosis()` extension function (unnecessary, `kurtosis_pop` suffices)

---

## 9. References

- DuckDB kurtosis source: `/workspace/thunderduck-duckdb-extension/duckdb/extension/core_functions/aggregate/distributive/kurtosis.cpp`
- DuckDB skewness source: `/workspace/thunderduck-duckdb-extension/duckdb/extension/core_functions/aggregate/distributive/skew.cpp`
- DuckDB quantile source: `/workspace/thunderduck-duckdb-extension/duckdb/extension/core_functions/aggregate/holistic/quantile.cpp`
- DuckDB interpolation: `/workspace/thunderduck-duckdb-extension/duckdb/src/include/duckdb/common/operator/interpolate.hpp`
- Thunderduck extension: `/workspace/thunderduck-duckdb-extension/src/thdck_spark_funcs_extension.cpp`
- Thunderduck aggregate patterns: `/workspace/thunderduck-duckdb-extension/src/include/spark_aggregates.hpp`
- Thunderduck function mapping: `/workspace/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`
- Spark CentralMomentAgg: `github.com/apache/spark/.../CentralMomentAgg.scala`
- Spark Percentile: `github.com/apache/spark/.../Percentile.scala`
