# M46: Lambda Function and Higher-Order Function Implementation

**Date:** 2025-12-17
**Status:** Complete
**Expression Coverage:** 56% → 75% (12/16 expressions)

## Summary

Implemented LambdaFunction, UnresolvedNamedLambdaVariable, and CallFunction expressions to enable higher-order array operations in Thunderduck. This brings expression coverage from 56% to 75%.

## Implementation Details

### New Expression Classes

1. **LambdaExpression** (`core/src/main/java/com/thunderduck/expression/LambdaExpression.java`)
   - Represents anonymous functions for HOFs
   - Generates DuckDB Python-style syntax: `lambda x: x + 1`
   - Supports 1-3 parameters
   - Factory methods: `of(param, body)`, `of(param1, param2, body)`

2. **LambdaVariableExpression** (`core/src/main/java/com/thunderduck/expression/LambdaVariableExpression.java`)
   - Represents lambda parameter references within lambda body
   - Emits plain identifiers (no quoting)

### ExpressionConverter Changes

Added to `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`:

- **Lambda scope tracking**: Stack-based scoping for nested lambda variable handling
- **LAMBDA_FUNCTION case**: Extracts parameters, pushes scope, converts body, creates LambdaExpression
- **UNRESOLVED_NAMED_LAMBDA_VARIABLE case**: Creates LambdaVariableExpression
- **CALL_FUNCTION case**: Creates FunctionCall with mapped function name
- **Higher-order function handling**: Special emulation for exists, forall, aggregate, zip_with, map HOFs

### Higher-Order Function Mappings

| Spark Function | DuckDB Translation | Status |
|----------------|-------------------|--------|
| `transform(arr, f)` | `list_transform(arr, f)` | Full |
| `filter(arr, f)` | `list_filter(arr, f)` | Full |
| `exists(arr, f)` | `list_bool_or(list_transform(arr, f))` | Full |
| `forall(arr, f)` | `list_bool_and(list_transform(arr, f))` | Full |
| `aggregate(arr, init, f)` | `list_reduce(list_prepend(init, arr), f)` | Full |
| `zip_with(a, b, f)` | `list_zip(a, b)` | Partial |
| `map_filter(m, f)` | `map_from_entries(list_filter(...))` | Partial |
| `transform_keys(m, f)` | `map_from_entries(list_transform(...))` | Partial |
| `transform_values(m, f)` | `map_from_entries(list_transform(...))` | Partial |

### FunctionRegistry Updates

Added to `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`:

- `mapFunctionName()` method for function name mapping
- HOF mappings: `transform` → `list_transform`, `filter` → `list_filter`, `aggregate` → `list_reduce`

## Key Technical Decisions

### DuckDB Lambda Syntax
DuckDB 1.3.0+ uses Python-style lambda syntax (`lambda x: x + 1`), not arrow syntax (`x -> x + 1`). Arrow syntax was deprecated in 1.3.0 and removed in 1.7.0.

### exists/forall Emulation
DuckDB doesn't have direct `list_any`/`list_all` functions for lambda predicates. Implemented using:
- `exists(arr, f)` → `list_bool_or(list_transform(arr, f))`
- `forall(arr, f)` → `list_bool_and(list_transform(arr, f))`

### aggregate Initial Value Handling
DuckDB `list_reduce` doesn't take an initial value. Solved by prepending initial value to the array:
- `aggregate(arr, init, f)` → `list_reduce(list_prepend(init, arr), f)`

### Lambda Variable Scoping
Implemented stack-based scoping for nested lambda support:
```java
private final Stack<Set<String>> lambdaScopes = new Stack<>();
```
Each lambda pushes a new scope, converting body with scope active, then pops.

## Known Limitations

### zip_with (Partial)
Returns the zipped list but doesn't apply the lambda function. Full support would require:
- Lambda body rewriting to access struct fields (`p[1]`, `p[2]`)
- Or index-based emulation with `generate_series`

### Map HOFs (Partial)
Basic structure implemented but lambda body rewriting for `(k, v)` parameters to `e.key`, `e.value` struct access not yet supported. Works for simple cases where the lambda can be passed through.

### createDataFrame with Arrays
`spark.createDataFrame()` serializes arrays as JSON strings, not proper LIST types. Tests use SQL-created temp views instead:
```python
spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [1, 2, 3] AS arr")
```

## Test Results

18 E2E tests created in `tests/integration/test_lambda_functions.py`:

| Test Class | Tests | Status |
|------------|-------|--------|
| TestTransformFunction | 3 | All Pass |
| TestFilterFunction | 4 | All Pass |
| TestExistsFunction | 2 | All Pass |
| TestForallFunction | 2 | All Pass |
| TestAggregateFunction | 3 | All Pass |
| TestNestedLambdas | 2 | All Pass |
| TestCombinedOperations | 2 | All Pass |

## Files Changed

### Created
- `core/src/main/java/com/thunderduck/expression/LambdaExpression.java`
- `core/src/main/java/com/thunderduck/expression/LambdaVariableExpression.java`
- `tests/integration/test_lambda_functions.py`
- `docs/pending_design/LAMBDA_AND_CALL_FUNCTION_SPEC.md`

### Modified
- `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`
- `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`

## Metrics

- **Lines of Code**: ~600 (expressions + converter + tests)
- **Expression Coverage**: 56% → 75%
- **E2E Tests Added**: 18
- **Time to Implement**: ~3 hours

## Next Steps

Potential future enhancements:
1. Full `zip_with` support with lambda body rewriting
2. Full map HOF support with struct field access rewriting
3. `UnresolvedExtractValue` for `col["key"]` and `col.field` syntax
4. Complex literal types (Array, Map, Struct)
