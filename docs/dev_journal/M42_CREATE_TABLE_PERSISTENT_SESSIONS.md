# M42: CREATE TABLE with Per-Session Persistent Databases

**Date:** 2025-12-16
**Status:** Complete

## Summary

Implemented `spark.catalog.createTable()` for internal tables using per-session persistent DuckDB databases. Tables now persist across session reconnects while maintaining isolation between different sessions. Database files are automatically cleaned up when sessions expire or the server shuts down.

## What Was Built

### Per-Session Persistent Storage Architecture

Each Spark Connect session now gets its own on-disk DuckDB database file:
- Location: `./thunderduck_sessions/{session_id}.duckdb`
- Configurable via `duckdb.sessions.dir` system property
- Tables persist across reconnects with the same session ID
- Different sessions are completely isolated
- Files automatically cleaned up on session expiry or server shutdown

### CREATE TABLE Implementation

Added support for `spark.catalog.createTable()` with the following capabilities:
- Internal tables (no path/source) stored directly in DuckDB
- Full schema support (all Spark data types mapped to DuckDB)
- Schema qualification (current database used as DuckDB schema)
- External tables (with path/source) return UNIMPLEMENTED for now

### Type Mapping (Spark to DuckDB)

| Spark Type | DuckDB Type |
|------------|-------------|
| BooleanType | BOOLEAN |
| ByteType | TINYINT |
| ShortType | SMALLINT |
| IntegerType | INTEGER |
| LongType | BIGINT |
| FloatType | FLOAT |
| DoubleType | DOUBLE |
| StringType | VARCHAR |
| BinaryType | BLOB |
| DateType | DATE |
| TimestampType | TIMESTAMP |
| DecimalType(p,s) | DECIMAL(p,s) |
| ArrayType(T) | T[] |
| MapType(K,V) | MAP(K, V) |
| StructType | STRUCT(...) |

## Key Design Decisions

1. **Per-Session vs Shared Database**: Chose per-session databases over a shared catalog to maintain session isolation. This matches Spark's behavior where temp tables are session-scoped.

2. **Automatic Cleanup**: Database files are cleaned up in three scenarios:
   - When a session is explicitly closed
   - When the server shuts down
   - When orphaned files are found on server startup (from crashed sessions)

3. **Internal Tables Only**: External tables (with `path` or `source` specified) are deferred to a future implementation. This keeps the scope focused.

4. **Void Result**: `createTable()` returns void (empty Arrow batch), matching Spark's behavior.

## Files Created/Modified

### New Files
```
connect-server/src/main/java/com/thunderduck/connect/converter/SparkDataTypeConverter.java
docs/dev_journal/M42_CREATE_TABLE_PERSISTENT_SESSIONS.md
```

### Modified Files
```
core/src/main/java/com/thunderduck/runtime/DuckDBRuntime.java
  - Added createPersistent(String dbPath) factory method

connect-server/src/main/java/com/thunderduck/connect/session/Session.java
  - Added constructor accepting sessions directory path
  - Added databasePath field for cleanup tracking
  - Added cleanup() method to delete database files
  - Added getDatabasePath() and isPersistent() methods

connect-server/src/main/java/com/thunderduck/connect/session/SessionManager.java
  - Added sessionsDir field and initialization
  - Sessions now created with persistent databases
  - Added cleanupOrphanedSessions() for startup cleanup
  - Modified shutdown() to cleanup all session files

connect-server/src/main/java/com/thunderduck/connect/service/CatalogOperationHandler.java
  - Added CREATE_TABLE case in switch statement
  - Implemented handleCreateTable() method

connect-server/src/main/java/com/thunderduck/connect/service/StreamingResultHandler.java
  - Added streamVoidResult() for void-returning catalog operations

connect-server/src/main/resources/connect-server.properties
  - Added duckdb.sessions.dir configuration property
```

## Test Results

```
============================================================
TEST: CREATE TABLE Functionality
============================================================
1. Creating table 'test_products' with schema:
   - id: IntegerType() (nullable=False)
   - name: StringType() (nullable=True)
   - price: DoubleType() (nullable=True)
   - active: BooleanType() (nullable=True)
   -> Table created successfully!

2. Verifying table exists...
   -> tableExists('test_products'): True

3. Listing tables...
   -> Tables: ['test_products']

4. Listing columns for 'test_products'...
   - id: INTEGER (nullable=False)
   - name: VARCHAR (nullable=True)
   - price: DOUBLE (nullable=True)
   - active: BOOLEAN (nullable=True)

5. Querying table structure via SQL...
   -> Columns from SQL: ['id', 'name', 'price', 'active']

SUCCESS: All CREATE TABLE tests passed!
============================================================
```

## Usage

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True)
])

# Create table (persists for this session)
spark.catalog.createTable("my_table", schema=schema)

# Verify table exists
assert spark.catalog.tableExists("my_table")

# Table survives reconnect with same session ID
spark.stop()
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
assert spark.catalog.tableExists("my_table")  # Still there!
```

## Lessons Learned

1. **DuckDB JDBC URL Format**: Persistent databases use `jdbc:duckdb:/path/file.duckdb` (no `:memory:` prefix).

2. **Quote Escaping**: Identifier quoting needed careful handling to avoid double-quoting when building qualified names like `"schema"."table"`.

3. **WAL Files**: DuckDB creates `.wal` files alongside the main database file that also need cleanup.

## Not In Scope (Future Work)

- External tables with `path`/`source` (parquet, csv, etc.)
- Table descriptions/comments
- Table properties/options
- DROP TABLE operation
- Multiple catalogs
- INSERT statements (separate issue)

---

**Related:**
- M41: Catalog Operations (Phase 4A)
- [Catalog Operations Architecture](../architect/CATALOG_OPERATIONS.md)
- [Gap Analysis](../../CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md)
