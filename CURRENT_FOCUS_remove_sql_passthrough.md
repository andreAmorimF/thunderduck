# Current Focus: Remove SparkSQL Pass-Through

## Status: COMPLETED

## Overview

Removing raw SQL pass-through functionality (`spark.sql()` calls) until a proper SparkSQL parser is integrated. The DataFrame API remains fully functional.

## Rationale

- Raw SQL pass-through without parsing is fragile
- No schema inference, type checking, or validation for raw SQL
- SQL-based tests are failing because there's no SQL parser
- Better to have clean DataFrame API support than broken SQL support
- SQL parser research completed - recommends Spark Catalyst (see `docs/SQL_PARSER_RESEARCH.md`)

## Changes Made

### 1. README.md Updated
- [x] Added SparkSQL disclaimer at top
- [x] Updated Quick Start to use DataFrame API examples only
- [x] Updated test counts to reflect DataFrame-only tests
- [x] Updated test groups documentation
- [x] Updated E2E testing section

### 2. Protocol Handler Changes
- [x] `SparkConnectServiceImpl.java` - Throw `UnsupportedOperationException` for SQL commands
- [x] `RelationConverter.java` - Throw exception for SQL relation type

### 3. Test Updates
- [x] Disable SQL-based integration tests (TPC-H SQL, TPC-DS SQL)
- [x] Keep DataFrame API tests enabled

## What We're Keeping

- **SQLRelation class** - Used internally for generated SQL (Drop, NAFill, etc.)
- **RawSQLExpression** - Used for CASE WHEN, intervals, arrays, maps, structs in DataFrame API
- **SchemaInferrer** - Used for NA operations
- **Nullable inference logic** - Used for all schema inference
- **All DataFrame API tests** - These should continue passing

## Error Message for Users

When users try `spark.sql()`:
```
UnsupportedOperationException: spark.sql() is not yet supported.
Please use DataFrame API instead. SQL support will be added in a future release.
```

## Next Steps After This

1. Implement SparkSQL parser using Spark Catalyst (as per research)
2. Re-enable SQL-based tests
3. Update documentation

## Files Modified

| File | Status | Changes |
|------|--------|---------|
| `README.md` | Done | Added disclaimers, updated examples |
| `SparkConnectServiceImpl.java` | Done | Reject SQL commands with UnsupportedOperationException |
| `RelationConverter.java` | Done | Reject SQL relation type |
| `test_tpcds_differential.py` | Done | Skip all SQL test classes |
| `test_differential_v2.py` | Done | Skip SQL test classes |

## Test Results After Changes

- **Window tests**: 35/35 passing (100%)
- **TPC-DS DataFrame tests**: 20/33 passing (existing issues, not regressions)
- **SQL tests**: All skipped as expected
