# Module Organization and Code Structure Design

## Executive Summary

This document defines the complete code organization, module structure, package hierarchy, and class responsibilities for the thunderduck project. The architecture follows clean separation of concerns with 5 core modules.

**Design Principles:**
- **Modularity**: Clear module boundaries with minimal coupling
- **Testability**: Each module independently testable
- **Maintainability**: Single responsibility, clear interfaces
- **Extensibility**: Easy to add new formats, functions, optimizations

## 1. Module Architecture

```
thunderduck-parent (reactor POM)
├── core                    # Translation engine, logical plan, SQL generation
├── formats                 # Format readers (Parquet, Delta, Iceberg)
├── api                     # Spark-compatible DataFrame API
├── tests                   # Comprehensive test suite
└── benchmarks              # Performance benchmarks
```

### Module Dependencies

```
┌──────────┐
│benchmarks│
└────┬─────┘
     │
     ▼
┌────────┐     ┌─────────┐
│  api   │────▶│ formats │
└───┬────┘     └────┬────┘
    │               │
    │               │
    └───────┬───────┘
            ▼
        ┌──────┐
        │ core │
        └──────┘
```

## 2. Core Module

### Package Structure

```
com.thunderduck.core/
├── logical/                    # Logical plan representation
│   ├── LogicalPlan.java        # Base class
│   ├── LeafNode.java           # Leaf node base
│   ├── UnaryNode.java          # Unary operator base
│   ├── BinaryNode.java         # Binary operator base
│   ├── TableScan.java          # Table scan node
│   ├── Project.java            # Projection node
│   ├── Filter.java             # Filter node
│   ├── Aggregate.java          # Aggregation node
│   ├── Join.java               # Join node
│   ├── Sort.java               # Sort node
│   ├── Limit.java              # Limit node
│   └── Union.java              # Union node
├── expression/                 # Expression representation
│   ├── Expression.java         # Base expression
│   ├── Literal.java            # Literal values
│   ├── ColumnReference.java    # Column references
│   ├── BinaryExpression.java   # Binary operations
│   ├── UnaryExpression.java    # Unary operations
│   ├── Function.java           # Function calls
│   ├── AggregateExpression.java # Aggregate functions
│   └── WindowExpression.java   # Window functions
├── types/                      # Type system
│   ├── TypeMapper.java         # Spark → DuckDB type mapping
│   ├── DataType.java           # Type representation
│   ├── StructType.java         # Struct type
│   ├── ArrayType.java          # Array type
│   └── MapType.java            # Map type
├── functions/                  # Function registry
│   ├── FunctionRegistry.java  # Function mapping registry
│   ├── ScalarFunctions.java   # Scalar function mappings
│   ├── AggregateFunctions.java # Aggregate function mappings
│   └── WindowFunctions.java   # Window function mappings
├── sql/                        # SQL generation
│   ├── SQLGenerator.java      # Main SQL generator
│   ├── DuckDBDialect.java     # DuckDB SQL dialect
│   ├── ExpressionTranslator.java # Expression → SQL
│   └── SQLFormatter.java      # SQL formatting utilities
├── optimizer/                  # Query optimization
│   ├── LogicalOptimizer.java  # Optimization coordinator
│   ├── FilterPushdown.java    # Filter pushdown optimization
│   ├── ColumnPruning.java     # Column pruning optimization
│   └── FilterFusion.java      # Filter fusion optimization
└── execution/                  # DuckDB execution
    ├── DuckDBConnection.java  # Connection wrapper
    ├── QueryExecutor.java     # Query execution
    ├── ArrowInterop.java      # Arrow data conversion
    └── ExtensionManager.java  # DuckDB extension management
```

### Key Classes

#### LogicalPlan.java
```java
package com.thunderduck.core.logical;

import com.thunderduck.core.types.StructType;
import java.util.List;

/**
 * Base class for all logical plan nodes.
 * Represents a tree of operations to be translated to SQL.
 */
public abstract class LogicalPlan {
    protected List<LogicalPlan> children;
    protected StructType schema;

    /**
     * Get the output schema of this plan node.
     */
    public abstract StructType schema();

    /**
     * Get child nodes.
     */
    public List<LogicalPlan> children() {
        return children;
    }

    /**
     * Accept visitor for traversal.
     */
    public abstract <T> T accept(LogicalPlanVisitor<T> visitor);

    /**
     * Create a transformed copy with new children.
     */
    public abstract LogicalPlan withNewChildren(List<LogicalPlan> newChildren);
}
```

#### SQLGenerator.java
```java
package com.thunderduck.core.sql;

import com.thunderduck.core.logical.LogicalPlan;
import com.thunderduck.core.functions.FunctionRegistry;
import com.thunderduck.core.types.TypeMapper;

/**
 * Translates logical plans to DuckDB SQL.
 */
public class SQLGenerator {
    private final FunctionRegistry functionRegistry;
    private final TypeMapper typeMapper;
    private final ExpressionTranslator expressionTranslator;

    public SQLGenerator() {
        this.functionRegistry = new FunctionRegistry();
        this.typeMapper = new TypeMapper();
        this.expressionTranslator = new ExpressionTranslator(functionRegistry);
    }

    /**
     * Generate SQL from a logical plan.
     */
    public String generateSQL(LogicalPlan plan) {
        return plan.accept(new SQLGeneratorVisitor(expressionTranslator));
    }
}
```

#### TypeMapper.java
```java
package com.thunderduck.core.types;

import org.apache.spark.sql.types.*;

/**
 * Maps Spark data types to DuckDB SQL types.
 */
public class TypeMapper {
    /**
     * Convert Spark DataType to DuckDB type string.
     */
    public String toDuckDBType(DataType sparkType) {
        if (sparkType instanceof IntegerType) {
            return "INTEGER";
        } else if (sparkType instanceof LongType) {
            return "BIGINT";
        } else if (sparkType instanceof DecimalType dt) {
            return String.format("DECIMAL(%d,%d)", dt.precision(), dt.scale());
        } else if (sparkType instanceof ArrayType at) {
            String elementType = toDuckDBType(at.elementType());
            return elementType + "[]";
        } else if (sparkType instanceof StructType st) {
            return toStructTypeSQL(st);
        }
        // ... other type mappings
    }

    private String toStructTypeSQL(StructType structType) {
        StringBuilder sb = new StringBuilder("STRUCT(");
        StructField[] fields = structType.fields();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields[i].name())
              .append(" ")
              .append(toDuckDBType(fields[i].dataType()));
        }
        sb.append(")");
        return sb.toString();
    }
}
```

## 3. Formats Module

### Package Structure

```
com.thunderduck.formats/
├── parquet/
│   ├── ParquetReader.java      # Parquet file reader
│   ├── ParquetWriter.java      # Parquet file writer
│   ├── ParquetOptions.java     # Parquet read/write options
│   └── ParquetSchema.java      # Schema handling
├── delta/
│   ├── DeltaReader.java        # Delta Lake reader
│   ├── DeltaMetadata.java      # Transaction log metadata
│   ├── DeltaVersion.java       # Version handling
│   └── DeltaOptions.java       # Delta read options
├── iceberg/
│   ├── IcebergReader.java      # Iceberg table reader
│   ├── IcebergMetadata.java    # Iceberg metadata
│   ├── IcebergSnapshot.java    # Snapshot handling
│   └── IcebergOptions.java     # Iceberg read options
└── common/
    ├── TableFormat.java        # Format enumeration
    ├── TableMetadata.java      # Generic table metadata
    └── FormatDetector.java     # Auto-detect file format
```

### Key Classes

#### ParquetReader.java
```java
package com.thunderduck.formats.parquet;

import com.thunderduck.core.logical.TableScan;
import com.thunderduck.core.execution.DuckDBConnection;

/**
 * Reads Parquet files using DuckDB's native Parquet reader.
 */
public class ParquetReader {
    private final DuckDBConnection connection;

    public ParquetReader(DuckDBConnection connection) {
        this.connection = connection;
    }

    /**
     * Read Parquet file(s) from path.
     * Supports glob patterns (e.g., "data/*.parquet").
     */
    public TableScan read(String path, ParquetOptions options) {
        String sql = buildReadSQL(path, options);
        return new TableScan(sql, TableFormat.PARQUET, inferSchema(path));
    }

    private String buildReadSQL(String path, ParquetOptions options) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        if (options.getColumns() != null) {
            sql.append(String.join(", ", options.getColumns()));
        } else {
            sql.append("*");
        }

        sql.append(" FROM read_parquet('").append(path).append("')");

        if (options.getFilter() != null) {
            sql.append(" WHERE ").append(options.getFilter());
        }

        return sql.toString();
    }
}
```

#### DeltaReader.java
```java
package com.thunderduck.formats.delta;

import com.thunderduck.core.logical.TableScan;
import com.thunderduck.core.execution.DuckDBConnection;

/**
 * Reads Delta Lake tables using DuckDB's delta extension.
 */
public class DeltaReader {
    private final DuckDBConnection connection;

    public DeltaReader(DuckDBConnection connection) {
        this.connection = connection;
        ensureDeltaExtension();
    }

    private void ensureDeltaExtension() {
        connection.execute("INSTALL delta");
        connection.execute("LOAD delta");
    }

    /**
     * Read Delta table at specified version.
     */
    public TableScan readVersion(String path, long version) {
        String sql = String.format(
            "SELECT * FROM delta_scan('%s', version => %d)",
            path, version
        );
        return new TableScan(sql, TableFormat.DELTA, inferSchema(path));
    }

    /**
     * Read Delta table as of timestamp.
     */
    public TableScan readTimestamp(String path, String timestamp) {
        String sql = String.format(
            "SELECT * FROM delta_scan('%s', timestamp => '%s')",
            path, timestamp
        );
        return new TableScan(sql, TableFormat.DELTA, inferSchema(path));
    }
}
```

## 4. API Module

### Package Structure

```
com.thunderduck.api/
├── session/
│   ├── SparkSession.java       # Spark session implementation
│   ├── ThunderduckSession.java # Session builder
│   └── SessionConfig.java      # Configuration options
├── dataset/
│   ├── DataFrame.java          # DataFrame implementation
│   ├── Dataset.java            # Typed Dataset
│   ├── Row.java                # Row representation
│   └── Column.java             # Column expression builder
├── reader/
│   ├── DataFrameReader.java    # Reader API
│   └── ReadOptions.java        # Read options
├── writer/
│   ├── DataFrameWriter.java    # Writer API
│   └── WriteOptions.java       # Write options
└── functions/
    ├── Functions.java          # SQL functions (static methods)
    └── Window.java             # Window specification builder
```

### Key Classes

#### SparkSession.java
```java
package com.thunderduck.api.session;

import com.thunderduck.api.reader.DataFrameReader;
import com.thunderduck.core.execution.DuckDBConnection;

/**
 * Main entry point for thunderduck functionality.
 * Compatible with Spark's SparkSession API.
 */
public class SparkSession implements AutoCloseable {
    private final DuckDBConnection connection;
    private final SessionConfig config;

    private SparkSession(DuckDBConnection connection, SessionConfig config) {
        this.connection = connection;
        this.config = config;
    }

    /**
     * Create a DataFrameReader for reading data.
     */
    public DataFrameReader read() {
        return new DataFrameReader(connection);
    }

    /**
     * Execute SQL query.
     */
    public DataFrame sql(String sqlText) {
        return new DataFrame(connection.executeQuery(sqlText));
    }

    /**
     * Close session and release resources.
     */
    @Override
    public void close() {
        connection.close();
    }

    /**
     * Builder for creating SparkSession.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SessionConfig config = new SessionConfig();

        public Builder appName(String name) {
            config.setAppName(name);
            return this;
        }

        public Builder config(String key, String value) {
            config.set(key, value);
            return this;
        }

        public SparkSession getOrCreate() {
            DuckDBConnection conn = new DuckDBConnection(config);
            return new SparkSession(conn, config);
        }
    }
}
```

#### DataFrame.java
```java
package com.thunderduck.api.dataset;

import com.thunderduck.core.logical.LogicalPlan;
import com.thunderduck.core.execution.DuckDBConnection;
import com.thunderduck.api.writer.DataFrameWriter;

/**
 * Distributed collection of data organized into named columns.
 * Compatible with Spark's DataFrame API.
 */
public class DataFrame {
    private final LogicalPlan logicalPlan;
    private final DuckDBConnection connection;

    public DataFrame(LogicalPlan plan, DuckDBConnection connection) {
        this.logicalPlan = plan;
        this.connection = connection;
    }

    /**
     * Select columns.
     */
    public DataFrame select(String... columns) {
        List<Expression> projections = Arrays.stream(columns)
            .map(name -> new ColumnReference(name))
            .collect(Collectors.toList());

        LogicalPlan newPlan = new Project(logicalPlan, projections);
        return new DataFrame(newPlan, connection);
    }

    /**
     * Filter rows using SQL expression.
     */
    public DataFrame filter(String condition) {
        Expression filterExpr = parseExpression(condition);
        LogicalPlan newPlan = new Filter(logicalPlan, filterExpr);
        return new DataFrame(newPlan, connection);
    }

    /**
     * Group by columns.
     */
    public GroupedData groupBy(String... columns) {
        return new GroupedData(this, Arrays.asList(columns));
    }

    /**
     * Join with another DataFrame.
     */
    public DataFrame join(DataFrame other, Column condition, String joinType) {
        LogicalPlan newPlan = new Join(
            logicalPlan,
            other.logicalPlan,
            condition.getExpression(),
            JoinType.valueOf(joinType.toUpperCase())
        );
        return new DataFrame(newPlan, connection);
    }

    /**
     * Collect results to driver.
     */
    public List<Row> collect() {
        return connection.execute(logicalPlan);
    }

    /**
     * Get DataFrameWriter for writing data.
     */
    public DataFrameWriter write() {
        return new DataFrameWriter(this, connection);
    }
}
```

## 5. Tests Module

### Package Structure

```
com.thunderduck.tests/
├── unit/                       # Unit tests
│   ├── core/
│   │   ├── TypeMapperTest.java
│   │   ├── SQLGeneratorTest.java
│   │   └── LogicalPlanTest.java
│   ├── formats/
│   │   ├── ParquetReaderTest.java
│   │   ├── DeltaReaderTest.java
│   │   └── IcebergReaderTest.java
│   └── api/
│       ├── DataFrameTest.java
│       └── DataFrameReaderTest.java
├── integration/                # Integration tests
│   ├── EndToEndWorkflowIT.java
│   ├── ConcurrencyIT.java
│   └── FormatIntegrationIT.java
├── differential/               # Spark comparison tests
│   ├── BaseDifferentialTest.java
│   ├── SelectFilterDiffTest.java
│   ├── AggregationDiffTest.java
│   └── JoinDiffTest.java
└── testdata/                   # Test data generators
    ├── DataGenerator.java
    ├── ParquetGenerator.java
    └── SchemaGenerator.java
```

## 6. Benchmarks Module

### Package Structure

```
com.thunderduck.benchmarks/
├── micro/                      # Micro-benchmarks
│   ├── ParquetReadBenchmark.java
│   ├── SQLGenerationBenchmark.java
│   └── TypeMappingBenchmark.java
├── tpch/                       # TPC-H queries
│   ├── TPCHBenchmark.java
│   ├── TPCHDataGenerator.java
│   └── queries/
│       ├── Query1.java
│       ├── Query3.java
│       └── ... (all 22 queries)
├── tpcds/                      # TPC-DS queries
│   ├── TPCDSBenchmark.java
│   ├── TPCDSDataGenerator.java
│   └── queries/
└── common/
    ├── BenchmarkBase.java
    └── BenchmarkRunner.java
```

## 7. Cross-Cutting Concerns

### Logging Strategy

```java
// Use SLF4J throughout
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLGenerator {
    private static final Logger log = LoggerFactory.getLogger(SQLGenerator.class);

    public String generateSQL(LogicalPlan plan) {
        log.debug("Generating SQL for plan: {}", plan.getClass().getSimpleName());

        try {
            String sql = plan.accept(new SQLGeneratorVisitor());
            log.trace("Generated SQL: {}", sql);
            return sql;
        } catch (Exception e) {
            log.error("Failed to generate SQL for plan", e);
            throw new SQLGenerationException("SQL generation failed", e);
        }
    }
}
```

### Error Handling

```java
// Custom exception hierarchy
package com.thunderduck.core.exception;

public class ThunderduckException extends RuntimeException {
    public ThunderduckException(String message) { super(message); }
    public ThunderduckException(String message, Throwable cause) { super(message, cause); }
}

public class SQLGenerationException extends ThunderduckException { }
public class TypeMappingException extends ThunderduckException { }
public class UnsupportedOperationException extends ThunderduckException { }
```

### Configuration Management

```java
package com.thunderduck.api.session;

import java.util.Properties;

public class SessionConfig {
    private final Properties properties = new Properties();

    // Standard Spark configs
    public static final String APP_NAME = "spark.app.name";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";

    // thunderduck specific configs
    public static final String DUCKDB_THREADS = "thunderduck.duckdb.threads";
    public static final String DUCKDB_MEMORY_LIMIT = "thunderduck.duckdb.memory.limit";
    public static final String ENABLE_OPTIMIZATION = "thunderduck.optimizer.enabled";

    public void set(String key, String value) {
        properties.setProperty(key, value);
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
```

## 8. Build-Time Code Generation

### Function Registry Code Generation

```java
// Generate function mappings from CSV
// scripts/generate-functions.java

public class FunctionRegistryGenerator {
    public static void main(String[] args) throws Exception {
        // Read spark-to-duckdb-functions.csv
        List<FunctionMapping> mappings = readFunctionMappings();

        // Generate FunctionRegistry.java
        generateJavaClass(mappings, "FunctionRegistry.java");
    }
}
```

## 9. Versioning and Compatibility

### API Version Compatibility

```java
package com.thunderduck.api;

/**
 * Version information for thunderduck.
 */
public final class Version {
    public static final String THUNDERDUCK_VERSION = "0.1.0-SNAPSHOT";
    public static final String SPARK_API_VERSION = "3.5";
    public static final String DUCKDB_VERSION = "1.1.3";

    /**
     * Check if Spark API version is compatible.
     */
    public static boolean isSparkVersionCompatible(String version) {
        return version.startsWith("3.5");
    }
}
```

## 10. Documentation Structure

```
docs/
├── coder/                      # Design documents
│   ├── 01_Build_Infrastructure_Design.md
│   ├── 02_Testing_Infrastructure_Design.md
│   ├── 03_Module_Organization_Design.md
│   ├── 04_CI_CD_Integration_Design.md
│   └── 05_Data_Generation_Design.md
├── api/                        # API documentation
│   ├── dataframe-api.md
│   ├── reader-api.md
│   └── writer-api.md
├── developer/                  # Developer guides
│   ├── getting-started.md
│   ├── contributing.md
│   └── architecture.md
└── javadoc/                    # Generated Javadoc
```

## Summary

This module organization provides:

- **Clear boundaries**: Each module has a single responsibility
- **Loose coupling**: Modules depend on interfaces, not implementations
- **High cohesion**: Related functionality grouped together
- **Testability**: Each module independently testable
- **Extensibility**: Easy to add new formats, functions, optimizations
- **Maintainability**: Clear package structure and naming conventions

**Module Size Targets:**
- `core`: ~50 classes, 10K LOC
- `formats`: ~20 classes, 3K LOC
- `api`: ~30 classes, 5K LOC
- `tests`: ~100 test classes, 8K LOC
- `benchmarks`: ~30 benchmark classes, 3K LOC

**Total Project Size**: ~230 classes, ~29K LOC
