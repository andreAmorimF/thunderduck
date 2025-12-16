# M36: Empty DataFrame Schema Preservation Fix

**Date:** 2025-12-16
**Status:** Complete

## Summary

Fixed `spark.createDataFrame([], schema)` to preserve column names and types when creating an empty DataFrame. Previously, empty DataFrames created with a schema would lose all column information, resulting in a generic single-column `col0` instead of the expected columns.

## Problem

When calling `spark.createDataFrame([], schema)` with an empty list and a schema, the column names were not preserved:

```python
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])
df = spark.createDataFrame([], schema)
print(df.columns)  # Expected: ['id', 'name'], Actual: ['col0']
```

This broke any subsequent operations that relied on the column names:
```python
df.select("id")  # ERROR: "id" not found
```

## Root Cause

PySpark 4.0+ sends schema information as JSON format in the `LocalRelation` protobuf message:

```json
{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"integer"},{"metadata":{},"name":"name","nullable":true,"type":"string"}],"type":"struct"}
```

The `SQLGenerator.generateEmptyValues()` method had a bug when parsing this JSON: it used `indexOf('}')` to find the end of each field object, but this found the `}` of the nested `metadata:{}` object instead of the actual field object boundary.

**Before (broken):**
```java
int objEnd = fieldsArray.indexOf('}', objStart);  // Found metadata:{} close brace!
```

**The JSON structure:**
```json
{"metadata":{},"name":"id"...}
          ^^ indexOf('}') stopped here instead of the outer }
```

## Solution

Fixed the JSON parsing to properly track brace depth when finding field object boundaries:

```java
// Parse each field object - handle nested braces
int objStart = 0;
while ((objStart = fieldsArray.indexOf('{', objStart)) >= 0) {
    // Find matching close brace (accounting for nested objects)
    int braceDepth = 1;
    int objEnd = objStart + 1;
    while (objEnd < fieldsArray.length() && braceDepth > 0) {
        char c = fieldsArray.charAt(objEnd);
        if (c == '{') braceDepth++;
        else if (c == '}') braceDepth--;
        objEnd++;
    }
    if (braceDepth != 0) break; // Unbalanced braces

    String fieldObj = fieldsArray.substring(objStart, objEnd);
    // Extract name and type from field object...
    objStart = objEnd;
}
```

## Test Results

**Before fix:**
```python
>>> schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
>>> df = spark.createDataFrame([], schema)
>>> df.columns
['col0']  # WRONG
```

**After fix:**
```python
>>> schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
>>> df = spark.createDataFrame([], schema)
>>> df.columns
['id', 'name']  # CORRECT
>>> df.schema
StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])
```

**Generated SQL:**
```sql
SELECT CAST(NULL AS INTEGER) AS "id", CAST(NULL AS VARCHAR) AS "name" WHERE FALSE
```

## Files Modified

- `core/src/main/java/com/thunderduck/generator/SQLGenerator.java`
  - Fixed `parseJSONSchema()` method to handle nested JSON objects with brace depth tracking

## Type Mapping

The fix also properly maps PySpark types to DuckDB types:
- `integer` → `INTEGER`
- `string` → `VARCHAR`
- `long` → `BIGINT`
- `double` → `DOUBLE`
- `float` → `FLOAT`
- `boolean` → `BOOLEAN`
- `date` → `DATE`
- `timestamp` → `TIMESTAMP`
- `binary` → `BLOB`
- `short` → `SMALLINT`
- `byte` → `TINYINT`

## Key Learnings

1. **Nested JSON requires careful parsing** - Simple `indexOf()` doesn't work when JSON objects contain nested objects like `metadata:{}`
2. **PySpark 4.0 changes schema format** - Schemas are now sent as JSON, not DDL format
3. **Empty DataFrames still need schema** - Even with 0 rows, the schema must be preserved for operations like `select()`, `withColumn()`, etc.
