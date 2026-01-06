# Type Mapping: DuckDB to Spark Connect

This document describes how Thunderduck maps DuckDB types to Spark types for transmission via the Spark Connect protocol.

## Integer Type Mapping

| DuckDB Type | Bits | Spark Type | Bits | Notes |
|-------------|------|------------|------|-------|
| TINYINT | 8 | ByteType | 8 | Direct mapping |
| SMALLINT | 16 | ShortType | 16 | Direct mapping |
| INTEGER | 32 | IntegerType | 32 | Direct mapping |
| BIGINT | 64 | LongType | 64 | Direct mapping |
| **HUGEINT** | **128** | **LongType** | **64** | **Lossy - see below** |

### HUGEINT (int128) Limitation

DuckDB supports 128-bit integers (`HUGEINT`/`INT128`) which Spark and the Spark Connect protocol do not support. The maximum integer type in Spark Connect is 64-bit `Long`.

**When HUGEINT appears:**
- `SUM()` of integer columns returns HUGEINT in DuckDB to prevent overflow
- Thunderduck explicitly casts `SUM()` results to `BIGINT` in SQL generation
- Any remaining HUGEINT values are mapped to `LongType` in schema inference

**Overflow behavior:**
- Values exceeding ±9,223,372,036,854,775,807 (2^63-1) will overflow
- This matches Spark's behavior - users would hit the same overflow in real Spark
- No data loss for typical workloads; overflow only occurs with extremely large aggregations

## Aggregate Function Return Types

| Function | Input Type | DuckDB Returns | Thunderduck Returns | Spark Returns |
|----------|-----------|----------------|---------------------|---------------|
| SUM | Integer types | HUGEINT | BIGINT (cast) | BIGINT |
| SUM | DECIMAL(p,s) | DECIMAL(38,s) | DECIMAL(p+10,s) | DECIMAL |
| SUM | DOUBLE | DOUBLE | DOUBLE | DOUBLE |
| COUNT | Any | BIGINT | BIGINT | BIGINT |
| AVG | Integer | DOUBLE | DOUBLE | DOUBLE |
| MIN/MAX | Any | Same as input | Same as input | Same as input |

## Implementation Details

### SQL Generation (FunctionRegistry.java)

```java
// SUM is cast to BIGINT to match Spark semantics
CUSTOM_TRANSLATORS.put("sum", args ->
    "CAST(SUM(" + args[0] + ") AS BIGINT)");

// SUM DISTINCT also needs the cast
CUSTOM_TRANSLATORS.put("sum_distinct", args ->
    "CAST(SUM(DISTINCT " + args + ") AS BIGINT)");
```

### Schema Inference (SchemaInferrer.java)

```java
case "HUGEINT":
case "INT128":
    // Fallback for any HUGEINT values that escape SQL-level casting
    return LongType.get();
```

## Unsigned Integer Mapping

DuckDB supports unsigned integers which Spark does not. These are widened to the next larger signed type:

| DuckDB Type | Spark Type | Notes |
|-------------|------------|-------|
| UTINYINT | ShortType | 8-bit unsigned → 16-bit signed |
| USMALLINT | IntegerType | 16-bit unsigned → 32-bit signed |
| UINTEGER | LongType | 32-bit unsigned → 64-bit signed |
| UBIGINT | LongType | 64-bit unsigned → 64-bit signed (lossy for values > 2^63) |

## Spark Connect Protocol Constraints

The Spark Connect protocol (`types.proto`) defines these integer types:

```protobuf
message DataType {
  oneof kind {
    Byte byte = 4;      // 8-bit signed
    Short short = 5;    // 16-bit signed
    Integer integer = 6; // 32-bit signed
    Long long = 7;      // 64-bit signed
    // No Int128/HugeInt/BigInteger defined
  }
}
```

This is a protocol-level limitation that cannot be changed without modifying:
1. Spark Connect proto definitions
2. All Spark Connect client libraries (PySpark, Scala, etc.)
3. Arrow serialization layer

## References

- `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java` - SQL function translation
- `core/src/main/java/com/thunderduck/schema/SchemaInferrer.java` - DuckDB type mapping
- `core/src/main/java/com/thunderduck/types/TypeMapper.java` - Spark ↔ DuckDB type mapping
- `connect-server/src/main/proto/spark/connect/types.proto` - Spark Connect type definitions
