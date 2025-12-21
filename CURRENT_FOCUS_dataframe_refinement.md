# Current Focus: DataFrame API Refinement and Cleanup

## Status: In Progress

## Overview

With SparkSQL pass-through removed, focus shifts to refining the DataFrame API implementation:
1. Remove accumulated cruft from iterative test-fixing
2. Establish principled schema inference and nullability handling
3. Fix remaining DataFrame test failures
4. Update unit tests for consistency

---

## Priority 1: Refactoring - COMPLETED

### TypeInferenceEngine Created

`TypeInferenceEngine.java` (598 lines) consolidates type inference logic:
- `resolveType(expr, schema)` - Main type resolution
- `resolveNullable(expr, schema)` - Nullability resolution
- `promoteNumericTypes(left, right)` - Numeric type promotion
- `promoteDecimalDivision(dividend, divisor)` - Decimal division per Spark rules
- `resolveAggregateReturnType(function, argType)` - Aggregate type inference

All logical plan nodes (WithColumns, Project, Aggregate) now delegate to this engine.

---

## Priority 2: Type Preservation Fixes

### Decimal Division - FIXED ✓

**Commits**:
- `dc59d72` - Initial Decimal division type preservation
- (pending) - Fix Q98 Decimal scale calculation

**What was fixed**:
- Added `promoteDecimalDivision()` using Spark's two-step formula with precision loss adjustment
- Added `promoteDecimalMultiplication()` for proper Decimal * Integer arithmetic
- Division of Decimal/Decimal now returns correct Decimal type
- Added CAST wrapper in SQL generation for divisions to force Spark-compatible types
- **Q98 now passes**

**Technical details**:
- Spark's division formula has two steps:
  1. Calculate initial: `scale = max(6, s1 + p2 + 1)`, `precision = p1 - s1 + s2 + scale`
  2. If precision > 38, apply precision loss adjustment: `scale = max(6, 38 - intDigits)`
- Integer literals promoted to Decimal based on actual value (100 → Decimal(3,0))
- CAST wrapper added in SQLGenerator.visitWithColumns() for division expressions

### CASE WHEN Type Preservation - PARTIALLY FIXED

**Changes made**:
- Extended `RawSQLExpression` with optional `DataType` parameter
- Updated `convertCaseWhen()` to infer type from THEN/ELSE branches

**Remaining issue (Q99, Q62)**:
- CASE WHEN branches contain `UnresolvedColumn` which returns `StringType` as placeholder
- Type inference at conversion time doesn't have schema access
- `SUM(StringType)` defaults to `DoubleType`
- Need schema-aware type resolution for CASE WHEN

**Root cause**: Architectural - column types aren't known at ExpressionConverter time

**Potential solutions**:
1. Create proper `CaseWhenExpression` class storing branch expressions
2. Handle in `TypeInferenceEngine.resolveType()` with schema lookup
3. Defer CASE WHEN type resolution to schema inference phase

---

## Priority 3: Differential Test Results (2025-12-21)

### Summary
- **Passed**: 174
- **Failed**: 172
- **Skipped**: 228

### TPC-DS DataFrame Status

| Query | Status | Issue |
|-------|--------|-------|
| Q3, Q7, Q13, Q15, Q19, Q26, Q32, Q37, Q41, Q42, Q45, Q48, Q50, Q52, Q55, Q71, Q82, Q91, Q92, Q96, **Q98** | PASS | - |
| Q9, Q12, Q17, Q20, Q25, Q29, Q40, Q43, Q62, Q84, Q85 | FAIL | Various type/data issues |
| Q99 | FAIL | CASE WHEN returns DoubleType instead of DecimalType |

### Failure Categories

1. ~~**Decimal precision/scale** (Q98)~~ - **FIXED**

2. **CASE WHEN type inference** (Q99, Q62)
   - Column references unresolved at conversion time
   - Need schema-aware type resolution

3. **Other failures** (172 total)
   - Complex types (arrays, maps, structs)
   - Interval arithmetic
   - Pivot/unpivot operations
   - Temp views
   - Using joins

---

## Priority 4: Next Steps

### Immediate (Type Fixes)
1. ~~**Fix Decimal division scale**~~ - **DONE** (Q98 passes)
2. **Schema-aware CASE WHEN** - Create CaseWhenExpression with deferred type resolution

### Future Work
- Complex type handling improvements
- Interval arithmetic support
- Pivot/unpivot operations

---

## Files Modified (This Session)

| File | Changes |
|------|---------|
| `core/.../types/TypeInferenceEngine.java` | Added `promoteDecimalDivision()`, `promoteDecimalMultiplication()`, precision loss adjustment |
| `core/.../expression/RawSQLExpression.java` | Added optional type field |
| `core/.../generator/SQLGenerator.java` | Added CAST wrapper for decimal divisions in `visitWithColumns()` |
| `connect-server/.../ExpressionConverter.java` | Updated `convertCaseWhen()` type inference |
| `tests/.../types/TypeInferenceEngineTest.java` | NEW - 15 unit tests (updated expectations) |
| `tests/.../expression/RawSQLExpressionTest.java` | NEW - 12 unit tests |

---

## Architecture Notes

### Type Resolution Flow

```
Spark Connect Protocol
    ↓
ExpressionConverter (no schema access)
    ↓
Logical Plan Nodes (WithColumns, Project, Aggregate)
    ↓
TypeInferenceEngine.resolveType(expr, schema) ← Schema available here
    ↓
Schema Inference
```

### CASE WHEN Problem

```
F.when(condition, column_ref).otherwise(0)
    ↓
ExpressionConverter.convertCaseWhen()
    ↓
column_ref.dataType() → StringType (UnresolvedColumn placeholder)
    ↓
RawSQLExpression(sql, StringType)
    ↓
SUM(RawSQLExpression) → resolveAggregateReturnType("SUM", StringType) → DoubleType
```

**Fix needed**: Defer type resolution to TypeInferenceEngine where schema is available.
