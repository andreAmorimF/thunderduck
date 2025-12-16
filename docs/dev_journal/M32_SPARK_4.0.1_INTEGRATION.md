# M32: Spark 4.0.1 Integration

**Date**: 2025-12-15
**Status**: Complete

## Summary

Integrated community contribution (PR #2) upgrading Thunderduck from Spark 3.5.3 to Spark 4.0.1. Cherry-picked 3 commits from external contributor and resolved all compatibility issues.

## PR Details

- **Source**: https://github.com/lastrk/thunderduck/pull/2
- **Contributor**: andreAmorimF
- **Commits**: 3 (bump spark, add protos, update readme)
- **Files Changed**: 21 (+1225/-58 lines)

## Key Changes

### Version Upgrades
- Java 11 → 17 (required by Spark 4.0)
- Spark 3.5.3 → 4.0.1
- PySpark client 3.5.3 → 4.0.1

### Spark Connect Protocol Changes
- SQL command extraction updated for Spark 4.0.1 backward compatibility:
  ```java
  // Spark 4.0.1: 'sql' field deprecated, replaced with 'input' relation
  if (sqlCommand.hasInput() && sqlCommand.getInput().hasSql()) {
      sql = sqlCommand.getInput().getSql().getQuery();
  } else if (!sqlCommand.getSql().isEmpty()) {
      sql = sqlCommand.getSql();  // Fallback for older clients
  }
  ```
- New proto files: `ml.proto`, `ml_common.proto` for ML pipeline support
- Auto-extract protos from spark-connect JAR during build

### TableFormat Change (Required Fix)
PR changed `NamedTable` handling from `TableFormat.PARQUET` to `TableFormat.TABLE`:
```java
// RelationConverter.java:194
return new TableScan(tableName, TableScan.TableFormat.TABLE, null);
```

This caused schema analysis regression - `TABLE` format returned null schema, breaking `groupBy()` and other operations that validate column names.

## Regressions Fixed

### 1. Join.inferSchema() NullPointerException (Commit 2a24cca)
- **Error**: `Cannot invoke "StructType.fields()" because schema is null`
- **Cause**: Multi-table joins with null child schemas crashed
- **Fix**: Added null checks in `Join.inferSchema()`, returns null to trigger DuckDB-based fallback

### 2. Empty Schema After Column Operations
- **Error**: `df.columns` returns `[]` after `drop()`, `withColumn()`, `toDF()`
- **Cause**: `SQLRelation` returns empty schema, PySpark validates columns via `_schema`
- **Fix**: Added empty schema check in `analyzePlan()`:
  ```java
  if (schema == null || schema.fields().isEmpty()) {
      schema = inferSchemaFromDuckDB(sql, session);
  }
  ```

## Integration Strategy

Used cherry-pick instead of merge/rebase since PR was based on M20, main was at M30:
```bash
git fetch origin pull/2/head:pr-2
git cherry-pick c9d7eda 070889a e08588d  # All auto-merged cleanly
git merge spark-4.0.1-bump --no-edit     # Fast-forward merge
```

## Files Modified

### Connect Server
- `RelationConverter.java` - TABLE format for NamedTable
- `SparkConnectServiceImpl.java` - SQL command extraction, empty schema handling

### Core
- `Join.java` - Null-safe schema inference

### Proto Files
- Updated 8 existing proto files for Spark 4.0.1 protocol
- Added `ml.proto`, `ml_common.proto`

### Configuration
- `pom.xml` (all modules) - Java 17, Spark 4.0.1 version

## Test Results

| Test Suite | Result |
|------------|--------|
| test_simple_sql.py | 3/3 pass |
| test_tpch_queries.py | 16 pass, 1 xfail |
| test_temp_views.py | 7/7 pass |
| test_tpcds_batch1.py | 100 pass, 2 fail |
| test_dataframe_operations.py | 13 pass, 15 xfail |

## Breaking Changes

- Java 17+ required (was Java 11)
- JVM flags required on ALL platforms: `--add-opens=java.base/java.nio=ALL-UNNAMED`

## Lessons Learned

1. Cherry-pick is cleaner than merge/rebase when PR is far behind main
2. `TABLE` format needs DuckDB-based schema inference (unlike `PARQUET` which has embedded schema)
3. PySpark 4.0.1 validates column names before operations via `_schema` property
4. Always run full `mvn install` when cherry-picking changes across modules
