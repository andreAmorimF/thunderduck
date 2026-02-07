# Spark Compatibility Extension Architecture

**Date:** 2026-02-07 | **Status:** Approved

---

## Motivation

DuckDB is a high-performance analytical engine, but its numerical semantics diverge from Apache Spark in several critical areas:

| Operation | Spark Behavior | DuckDB Behavior | Impact |
|-----------|---------------|-----------------|--------|
| Decimal division | Returns `DECIMAL` with `ROUND_HALF_UP` | Casts to `DOUBLE`, loses precision | Data correctness |
| `AVG(DECIMAL)` | Returns `DECIMAL(p+4, s+4)` | Returns `DOUBLE` | Type mismatch |
| `EXTRACT(YEAR)` | Returns `INTEGER` | Returns `BIGINT` | Type mismatch |
| `SUM(CASE int)` | Returns `BIGINT` | Returns `DECIMAL(38,0)` | Type mismatch |
| Integer overflow | Throws `ArithmeticException` | Silently wraps | Silent data corruption |

These differences cannot be fixed by SQL generation alone (e.g., wrapping in `CAST`) because they are fundamental to how DuckDB's execution engine processes values internally.

## Architectural Decision

**Build a DuckDB C/C++ extension (`spark_compat`) that implements Spark-precise semantics as native DuckDB functions.** The extension is:

- **Optional**: Thunderduck works without it using vanilla DuckDB functions (relaxed mode)
- **Bundled**: Pre-compiled binaries are packaged as JAR resources, extracted and loaded at runtime
- **Per-function**: Each Spark-incompatible operation gets a dedicated extension function
- **Zero-config**: When present, the server detects and loads it automatically

### Two Compatibility Modes

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark DataFrame API                       │
│                  (PySpark / Scala client)                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  Translation Engine                          │
│                                                              │
│   FunctionRegistry checks:                                   │
│     Is spark_compat extension loaded?                        │
│       YES → map to spark_decimal_div(), spark_avg(), ...     │
│       NO  → map to vanilla DuckDB /, avg(), ...              │
│                                                              │
└─────────────────────────┬───────────────────────────────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
┌──────────────────────┐  ┌──────────────────────┐
│   Relaxed Mode       │  │   Strict Mode         │
│   (no extension)     │  │   (extension loaded)  │
│                      │  │                        │
│   Vanilla DuckDB     │  │   spark_compat ext     │
│   functions          │  │   functions             │
│                      │  │                        │
│   ≈85% Spark compat  │  │   ~100% Spark compat   │
│   Fast, lightweight  │  │   Exact Spark semantics │
└──────────────────────┘  └──────────────────────┘
```

### Relaxed Mode (default, no extension)

- Uses vanilla DuckDB functions for all operations
- Achieves ~85% Spark compatibility (sufficient for most analytics workloads)
- No native compilation required
- Maximum portability

### Strict Mode (extension loaded)

- Extension functions replace vanilla DuckDB equivalents where semantics differ
- Exact Spark type coercion, rounding, overflow, and nullability behavior
- Required for workloads that depend on precise numeric reproducibility
- Enables 100% differential test pass rate

## Extension Module: `duckdb_ext/`

### Current Functions

| Extension Function | Replaces | What It Does |
|-------------------|----------|--------------|
| `spark_decimal_div(a, b)` | `a / b` (decimal) | Spark 4.1 decimal division with `ROUND_HALF_UP`, 256-bit intermediate arithmetic, correct `DECIMAL(p,s)` result type |

### Planned Functions

| Extension Function | Replaces | Purpose |
|-------------------|----------|---------|
| `spark_decimal_avg(col)` | `AVG(col)` | Returns `DECIMAL(p+4, s+4)` instead of `DOUBLE` |
| `spark_decimal_sum(col)` | `SUM(col)` | Returns `DECIMAL(min(p+10,38), s)` instead of `DECIMAL(38,s)` |
| `spark_extract_int(part, date)` | `EXTRACT(part FROM date)` | Returns `INTEGER` instead of `BIGINT` |
| `spark_checked_add(a, b)` | `a + b` (integer) | Throws on overflow instead of wrapping |
| `spark_checked_multiply(a, b)` | `a * b` (integer) | Throws on overflow instead of wrapping |

### Build System

The extension uses CMake (DuckDB's standard extension build system):

```bash
cd duckdb_ext

# Build for current platform
GEN=ninja make release

# Output: build/release/extension/thdck_spark_funcs/thdck_spark_funcs.duckdb_extension
```

Cross-compilation for multiple platforms:
```bash
# Linux AMD64
PLATFORM=linux_amd64 GEN=ninja make release

# Linux ARM64 (e.g., AWS Graviton)
PLATFORM=linux_arm64 GEN=ninja make release

# macOS ARM64 (Apple Silicon)
PLATFORM=osx_arm64 GEN=ninja make release
```

### Version Constraint

The extension **must** be compiled against the exact same DuckDB version used by `duckdb_jdbc` in the Maven build. DuckDB enforces a strict version check at `LOAD` time.

Current versions:
- `duckdb_jdbc`: **1.4.3.0**
- Extension DuckDB submodule: Must be `v1.4.3`

## Maven Integration

### Maven Profile: `build-extension`

The extension build is **integrated into Maven via a profile**. When activated, it automatically builds and bundles the extension for the current platform.

#### Usage

```bash
# Without extension (relaxed mode, default)
mvn clean package -DskipTests

# With extension (strict mode) - builds and bundles automatically
mvn clean package -DskipTests -Pbuild-extension
```

The `build-extension` profile:
1. Detects the current platform (e.g., `linux_amd64`, `osx_arm64`)
2. Runs `make release` in `duckdb_ext/` with `GEN=ninja`
3. Copies the compiled `.duckdb_extension` to `core/src/main/resources/extensions/<platform>/`
4. Includes it in the final JAR during packaging

**Prerequisites**: CMake, Ninja, and a C++ compiler (GCC 9+ or Clang 10+)

#### Resource Layout

```
core/src/main/resources/
└── extensions/
    ├── linux_amd64/
    │   └── thdck_spark_funcs.duckdb_extension
    ├── linux_arm64/
    │   └── thdck_spark_funcs.duckdb_extension
    ├── osx_arm64/
    │   └── thdck_spark_funcs.duckdb_extension
    └── osx_amd64/
        └── thdck_spark_funcs.duckdb_extension
```

**Note**: Each build produces an extension for the **current platform only**. Multi-platform JARs require building on each target platform separately.

#### Manual Build Workflow (Alternative)

For development or troubleshooting:

```bash
# 1. Build extension manually
cd duckdb_ext && GEN=ninja make release && cd ..

# 2. Copy to resources (platform-specific)
PLATFORM=linux_amd64  # or osx_arm64, linux_arm64, etc.
mkdir -p core/src/main/resources/extensions/$PLATFORM
cp duckdb_ext/build/release/extension/thdck_spark_funcs/thdck_spark_funcs.duckdb_extension \
   core/src/main/resources/extensions/$PLATFORM/

# 3. Rebuild to include extension in JAR
mvn clean package -DskipTests
```

#### CI/CD Integration

For release builds, a CI matrix builds the extension for all target platforms:

```yaml
strategy:
  matrix:
    include:
      - os: ubuntu-latest
        arch: amd64
      - os: ubuntu-latest
        arch: arm64
      - os: macos-latest
        arch: arm64

steps:
  - name: Build with extension
    run: mvn clean package -DskipTests -Pbuild-extension
```

Each platform artifact is built independently and includes the extension for that platform.

## Runtime Loading

### Detection and Loading (in `DuckDBRuntime.java`)

```
DuckDBRuntime constructor
    │
    ├── Create JDBC connection
    │     └── Set allow_unsigned_extensions=true (connection property)
    │
    ├── configureConnection()
    │     └── SET memory_limit, threads, etc.
    │
    └── loadBundledExtensions()  ← NEW
          │
          ├── PRAGMA platform → e.g., "linux_amd64"
          ├── Check classpath: /extensions/linux_amd64/thdck_spark_funcs.duckdb_extension
          │     ├── Found → extract to temp file, LOAD, set extensionLoaded=true

          │     └── Not found → log INFO, continue without extension
          └── FunctionRegistry.setExtensionAvailable(extensionLoaded)
```

### Graceful Degradation

- If extension binaries are **not bundled**: server starts normally, uses vanilla DuckDB functions
- If extension **fails to load** (version mismatch, platform mismatch): logs a warning, continues without extension
- Extension availability is queryable at runtime via `FunctionRegistry.isExtensionAvailable()`

### FunctionRegistry Integration

When the extension is loaded, `FunctionRegistry` swaps specific function mappings:

```java
// Without extension (relaxed):
"divide" → "(({0}) / ({1}))"

// With extension (strict):
"divide" → "spark_decimal_div({0}, {1})"
```

The translation engine checks `FunctionRegistry.isExtensionAvailable()` and selects the appropriate mapping. This is a compile-time decision per query — no runtime branching in the hot path.

## Testing Strategy

### Unit Tests (Java)

Test that `FunctionRegistry` produces correct SQL for both modes:

```java
@Test void testDivisionWithoutExtension() {
    FunctionRegistry.setExtensionAvailable(false);
    assertEquals("((a) / (b))", FunctionRegistry.translate("divide", "a", "b"));
}

@Test void testDivisionWithExtension() {
    FunctionRegistry.setExtensionAvailable(true);
    assertEquals("spark_decimal_div(a, b)", FunctionRegistry.translate("divide", "a", "b"));
}
```

### Differential Tests (pytest)

The existing differential test suite validates end-to-end correctness:

- **Relaxed mode** (`--strict-schema` off): Tests pass with vanilla DuckDB (values correct, types may differ)
- **Strict mode** (`--strict-schema` on): Tests pass only when extension is loaded (exact type + value match)

### Extension Unit Tests (SQL)

The `duckdb_ext/test/sql/` directory contains DuckDB SQL tests that validate the extension in isolation:

```sql
-- Spark precision rules
SELECT spark_decimal_div(CAST(1 AS DECIMAL(10,2)), CAST(3 AS DECIMAL(10,2)));
-- Expected: 0.333333 (DECIMAL(21,6)), not 0.3333333333 (DOUBLE)
```

## File Summary

| File | Role |
|------|------|
| `duckdb_ext/CMakeLists.txt` | Extension build configuration |
| `duckdb_ext/src/thdck_spark_funcs_extension.cpp` | Extension entry point, function registration |
| `duckdb_ext/src/include/spark_precision.hpp` | Spark 4.1 decimal type rules |
| `duckdb_ext/src/include/decimal_division.hpp` | ROUND_HALF_UP division with 256-bit arithmetic |
| `duckdb_ext/src/include/wide_integer.hpp` | 256-bit integer support |
| `duckdb_ext/docs/thunderduck-integration.md` | Runtime integration guide |
| `core/.../runtime/DuckDBRuntime.java` | Extension loading at connection creation |
| `core/.../functions/FunctionRegistry.java` | Conditional function mapping |
