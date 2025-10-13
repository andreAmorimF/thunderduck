# Data Generation Pipeline Design for catalyst2sql

## Executive Summary

This document defines comprehensive data generation strategies for testing and benchmarking, including synthetic data generation, TPC-H/TPC-DS benchmark data, and format-specific test data (Parquet, Delta, Iceberg).

**Key Components:**
- **Synthetic Data Generator**: Configurable schema-based data generation
- **TPC-H Pipeline**: Industry-standard OLAP benchmark
- **TPC-DS Pipeline**: Complex decision support benchmark
- **Format Converters**: Generate Parquet, Delta, Iceberg tables
- **Data Quality**: Deterministic, reproducible, realistic distributions

## 1. Data Generation Architecture

```
┌─────────────────────────────────────────────────┐
│  Data Generation Coordinator                    │
│  - Schema definition                            │
│  - Distribution configuration                   │
│  - Volume planning                              │
└────────────┬────────────────────────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌──────────┐     ┌─────────────┐
│Synthetic │     │  Benchmark  │
│Generator │     │  Generator  │
│- Schema  │     │  - TPC-H    │
│- Volume  │     │  - TPC-DS   │
└────┬─────┘     └──────┬──────┘
     │                  │
     ▼                  ▼
┌─────────────────────────────────┐
│  Format Writers                 │
│  - Parquet                      │
│  - Delta Lake                   │
│  - Iceberg                      │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Output Storage                 │
│  - Local filesystem             │
│  - S3 (optional)                │
│  - HDFS (optional)              │
└─────────────────────────────────┘
```

## 2. Synthetic Data Generator

### Schema-Based Generation

```java
// testdata/src/main/java/com/catalyst2sql/testdata/SyntheticDataGenerator.java
package com.catalyst2sql.testdata;

import org.apache.spark.sql.types.*;
import java.util.Random;
import java.util.List;

/**
 * Generates synthetic data based on schema definitions.
 * Supports configurable distributions, nullability, and cardinality.
 */
public class SyntheticDataGenerator {
    private final Random random;
    private final long seed;

    public SyntheticDataGenerator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    /**
     * Generate data for a given schema.
     */
    public List<Row> generate(StructType schema, long numRows, DataProfile profile) {
        List<Row> rows = new ArrayList<>();

        for (long i = 0; i < numRows; i++) {
            Object[] values = new Object[schema.fields().length];

            for (int j = 0; j < schema.fields().length; j++) {
                StructField field = schema.fields()[j];
                values[j] = generateValue(field, profile.getFieldProfile(field.name()));
            }

            rows.add(RowFactory.create(values));
        }

        return rows;
    }

    private Object generateValue(StructField field, FieldProfile profile) {
        // Handle nullability
        if (field.nullable() && random.nextDouble() < profile.nullProbability()) {
            return null;
        }

        DataType type = field.dataType();

        if (type instanceof IntegerType) {
            return generateInteger(profile);
        } else if (type instanceof LongType) {
            return generateLong(profile);
        } else if (type instanceof DoubleType) {
            return generateDouble(profile);
        } else if (type instanceof StringType) {
            return generateString(profile);
        } else if (type instanceof DateType) {
            return generateDate(profile);
        } else if (type instanceof TimestampType) {
            return generateTimestamp(profile);
        } else if (type instanceof DecimalType) {
            return generateDecimal((DecimalType) type, profile);
        } else if (type instanceof ArrayType) {
            return generateArray((ArrayType) type, profile);
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    // Distribution-aware generators
    private int generateInteger(FieldProfile profile) {
        Distribution dist = profile.distribution();

        return switch (dist.type()) {
            case UNIFORM -> random.nextInt(dist.max() - dist.min()) + dist.min();
            case NORMAL -> (int) (random.nextGaussian() * dist.stddev() + dist.mean());
            case ZIPF -> generateZipf(dist.alpha(), dist.max());
            case EXPONENTIAL -> (int) (-Math.log(1 - random.nextDouble()) * dist.lambda());
        };
    }

    private long generateLong(FieldProfile profile) {
        Distribution dist = profile.distribution();

        return switch (dist.type()) {
            case UNIFORM -> random.nextLong(dist.maxLong() - dist.minLong()) + dist.minLong();
            case NORMAL -> (long) (random.nextGaussian() * dist.stddev() + dist.mean());
            case SEQUENTIAL -> dist.sequence().next();
        };
    }

    private double generateDouble(FieldProfile profile) {
        Distribution dist = profile.distribution();

        return switch (dist.type()) {
            case UNIFORM -> random.nextDouble() * (dist.max() - dist.min()) + dist.min();
            case NORMAL -> random.nextGaussian() * dist.stddev() + dist.mean();
            case EXPONENTIAL -> -Math.log(1 - random.nextDouble()) * dist.lambda();
        };
    }

    private String generateString(FieldProfile profile) {
        StringPattern pattern = profile.stringPattern();

        return switch (pattern.type()) {
            case ALPHA -> generateAlphaString(pattern.length());
            case ALPHANUMERIC -> generateAlphanumericString(pattern.length());
            case NUMERIC -> generateNumericString(pattern.length());
            case UUID -> UUID.randomUUID().toString();
            case EMAIL -> generateEmail();
            case PHONE -> generatePhoneNumber();
            case CATEGORY -> pattern.categories().get(random.nextInt(pattern.categories().size()));
            case TEMPLATE -> fillTemplate(pattern.template());
        };
    }

    // Zipf distribution (common in real-world data)
    private int generateZipf(double alpha, int max) {
        double sum = 0.0;
        for (int i = 1; i <= max; i++) {
            sum += 1.0 / Math.pow(i, alpha);
        }

        double randomValue = random.nextDouble() * sum;
        double cumulativeSum = 0.0;

        for (int i = 1; i <= max; i++) {
            cumulativeSum += 1.0 / Math.pow(i, alpha);
            if (cumulativeSum >= randomValue) {
                return i;
            }
        }

        return max;
    }
}
```

### Data Profiles

```java
// testdata/src/main/java/com/catalyst2sql/testdata/DataProfile.java
package com.catalyst2sql.testdata;

/**
 * Defines statistical properties for generated data.
 */
public class DataProfile {
    private Map<String, FieldProfile> fieldProfiles;

    public static DataProfile realistic() {
        return new DataProfile()
            .field("id", FieldProfile.sequential(1))
            .field("customer_id", FieldProfile.uniform(1, 100000))
            .field("order_date", FieldProfile.dateRange("2020-01-01", "2024-12-31"))
            .field("amount", FieldProfile.normal(100.0, 50.0))
            .field("category", FieldProfile.categories("Electronics", "Clothing", "Food", "Books"))
            .field("status", FieldProfile.zipf(1.2, "Active", "Pending", "Completed", "Cancelled"))
            .nullable("customer_id", 0.05);  // 5% null rate
    }

    public static DataProfile forTesting() {
        return new DataProfile()
            .field("id", FieldProfile.sequential(1))
            .field("value", FieldProfile.uniform(0, 1000))
            .field("name", FieldProfile.template("user_{id}"))
            .nullable("value", 0.1);  // 10% null rate
    }

    public FieldProfile getFieldProfile(String fieldName) {
        return fieldProfiles.getOrDefault(fieldName, FieldProfile.defaults());
    }
}

public class FieldProfile {
    private Distribution distribution;
    private StringPattern stringPattern;
    private double nullProbability;

    public static FieldProfile sequential(long start) {
        return new FieldProfile()
            .distribution(Distribution.sequential(start));
    }

    public static FieldProfile uniform(int min, int max) {
        return new FieldProfile()
            .distribution(Distribution.uniform(min, max));
    }

    public static FieldProfile normal(double mean, double stddev) {
        return new FieldProfile()
            .distribution(Distribution.normal(mean, stddev));
    }

    public static FieldProfile zipf(double alpha, String... values) {
        return new FieldProfile()
            .stringPattern(StringPattern.zipf(alpha, values));
    }

    public static FieldProfile categories(String... values) {
        return new FieldProfile()
            .stringPattern(StringPattern.category(values));
    }

    public static FieldProfile dateRange(String start, String end) {
        return new FieldProfile()
            .distribution(Distribution.dateRange(start, end));
    }
}
```

### Usage Example

```java
// Generate synthetic customer data
StructType schema = new StructType()
    .add("customer_id", DataTypes.LongType, false)
    .add("name", DataTypes.StringType, false)
    .add("email", DataTypes.StringType, false)
    .add("registration_date", DataTypes.DateType, false)
    .add("total_purchases", DataTypes.IntegerType, false)
    .add("last_login", DataTypes.TimestampType, true);

DataProfile profile = DataProfile.realistic();

SyntheticDataGenerator generator = new SyntheticDataGenerator(42);
List<Row> data = generator.generate(schema, 1_000_000, profile);

// Write to Parquet
SparkSession spark = SparkSession.builder().getOrCreate();
Dataset<Row> df = spark.createDataFrame(data, schema);
df.write().mode("overwrite").parquet("data/customers.parquet");
```

## 3. TPC-H Data Generation

### TPC-H Pipeline

```java
// benchmarks/src/main/java/com/catalyst2sql/benchmarks/datagen/TPCHDataGenerator.java
package com.catalyst2sql.benchmarks.datagen;

/**
 * Generates TPC-H benchmark data using dbgen.
 * Supports multiple scale factors (1, 10, 100, 1000).
 */
public class TPCHDataGenerator {
    private static final String DBGEN_PATH = "benchmarks/tpch-dbgen";
    private static final String[] TABLES = {
        "customer", "lineitem", "nation", "orders",
        "part", "partsupp", "region", "supplier"
    };

    /**
     * Generate TPC-H data at specified scale factor.
     *
     * @param scaleFactor Scale factor (1 = 1GB, 10 = 10GB, etc.)
     * @param outputPath Output directory
     * @return Generated data statistics
     */
    public static TPCHDataStats generate(int scaleFactor, String outputPath) throws Exception {
        // 1. Clone dbgen if not present
        ensureDbgenInstalled();

        // 2. Generate .tbl files
        generateTblFiles(scaleFactor);

        // 3. Convert to Parquet
        convertToParquet(scaleFactor, outputPath);

        // 4. Validate data
        return validateData(outputPath);
    }

    private static void ensureDbgenInstalled() throws Exception {
        Path dbgenPath = Paths.get(DBGEN_PATH);

        if (!Files.exists(dbgenPath)) {
            System.out.println("Cloning TPC-H dbgen...");

            ProcessBuilder pb = new ProcessBuilder(
                "git", "clone",
                "https://github.com/databricks/tpch-dbgen.git",
                DBGEN_PATH
            );
            pb.inheritIO();
            Process process = pb.start();
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("Failed to clone dbgen");
            }

            // Build dbgen
            pb = new ProcessBuilder("make", "-C", DBGEN_PATH);
            pb.inheritIO();
            process = pb.start();
            exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("Failed to build dbgen");
            }
        }
    }

    private static void generateTblFiles(int scaleFactor) throws Exception {
        System.out.println("Generating TPC-H data at scale factor " + scaleFactor + "...");

        ProcessBuilder pb = new ProcessBuilder(
            "./dbgen",
            "-s", String.valueOf(scaleFactor),
            "-T", "a",  // Generate all tables
            "-f"         // Force overwrite
        );
        pb.directory(new File(DBGEN_PATH));
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Failed to generate TPC-H data");
        }
    }

    private static void convertToParquet(int scaleFactor, String outputPath) throws Exception {
        System.out.println("Converting TPC-H data to Parquet format...");

        SparkSession spark = SparkSession.builder()
            .appName("TPCHDataConverter")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        for (String table : TABLES) {
            System.out.println("Converting table: " + table);

            String tblFile = DBGEN_PATH + "/" + table + ".tbl";
            String parquetPath = outputPath + "/tpch_sf" + scaleFactor + "/" + table;

            Dataset<Row> df = spark.read()
                .option("delimiter", "|")
                .option("inferSchema", "false")
                .schema(getSchemaForTable(table))
                .csv(tblFile);

            df.write()
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(parquetPath);

            System.out.println("  Rows: " + df.count());
        }

        spark.stop();
    }

    private static StructType getSchemaForTable(String table) {
        return switch (table) {
            case "customer" -> new StructType()
                .add("c_custkey", DataTypes.LongType)
                .add("c_name", DataTypes.StringType)
                .add("c_address", DataTypes.StringType)
                .add("c_nationkey", DataTypes.LongType)
                .add("c_phone", DataTypes.StringType)
                .add("c_acctbal", DataTypes.createDecimalType(15, 2))
                .add("c_mktsegment", DataTypes.StringType)
                .add("c_comment", DataTypes.StringType);

            case "lineitem" -> new StructType()
                .add("l_orderkey", DataTypes.LongType)
                .add("l_partkey", DataTypes.LongType)
                .add("l_suppkey", DataTypes.LongType)
                .add("l_linenumber", DataTypes.IntegerType)
                .add("l_quantity", DataTypes.createDecimalType(15, 2))
                .add("l_extendedprice", DataTypes.createDecimalType(15, 2))
                .add("l_discount", DataTypes.createDecimalType(15, 2))
                .add("l_tax", DataTypes.createDecimalType(15, 2))
                .add("l_returnflag", DataTypes.StringType)
                .add("l_linestatus", DataTypes.StringType)
                .add("l_shipdate", DataTypes.DateType)
                .add("l_commitdate", DataTypes.DateType)
                .add("l_receiptdate", DataTypes.DateType)
                .add("l_shipinstruct", DataTypes.StringType)
                .add("l_shipmode", DataTypes.StringType)
                .add("l_comment", DataTypes.StringType);

            // ... other tables

            default -> throw new IllegalArgumentException("Unknown table: " + table);
        };
    }

    private static TPCHDataStats validateData(String outputPath) throws Exception {
        System.out.println("Validating TPC-H data...");

        SparkSession spark = SparkSession.builder()
            .appName("TPCHDataValidator")
            .master("local[*]")
            .getOrCreate();

        TPCHDataStats stats = new TPCHDataStats();

        for (String table : TABLES) {
            String path = outputPath + "/" + table;
            Dataset<Row> df = spark.read().parquet(path);

            long rowCount = df.count();
            long sizeBytes = Files.walk(Paths.get(path))
                .filter(Files::isRegularFile)
                .mapToLong(p -> p.toFile().length())
                .sum();

            stats.addTable(table, rowCount, sizeBytes);

            System.out.println("  " + table + ": " + rowCount + " rows, " +
                             (sizeBytes / 1024 / 1024) + " MB");
        }

        spark.stop();
        return stats;
    }
}
```

### TPC-H Data Statistics

```java
public class TPCHDataStats {
    private Map<String, TableStats> tableStats = new HashMap<>();

    public static class TableStats {
        public final long rowCount;
        public final long sizeBytes;

        public TableStats(long rowCount, long sizeBytes) {
            this.rowCount = rowCount;
            this.sizeBytes = sizeBytes;
        }
    }

    public void addTable(String table, long rowCount, long sizeBytes) {
        tableStats.put(table, new TableStats(rowCount, sizeBytes));
    }

    public long getTotalRows() {
        return tableStats.values().stream()
            .mapToLong(s -> s.rowCount)
            .sum();
    }

    public long getTotalSizeBytes() {
        return tableStats.values().stream()
            .mapToLong(s -> s.sizeBytes)
            .sum();
    }
}
```

## 4. TPC-DS Data Generation

```java
// benchmarks/src/main/java/com/catalyst2sql/benchmarks/datagen/TPCDSDataGenerator.java
package com.catalyst2sql.benchmarks.datagen;

/**
 * Generates TPC-DS benchmark data using dsdgen.
 * More complex than TPC-H with 24 tables.
 */
public class TPCDSDataGenerator {
    private static final String DSDGEN_PATH = "benchmarks/tpcds-kit/tools";

    public static TPCDSDataStats generate(int scaleFactor, String outputPath) throws Exception {
        // Similar structure to TPC-H but with dsdgen tool
        ensureDsdgenInstalled();
        generateDatFiles(scaleFactor);
        convertToParquet(scaleFactor, outputPath);
        return validateData(outputPath);
    }

    // Implementation similar to TPC-H but with 24 tables
}
```

## 5. Format-Specific Generators

### Delta Lake Table Generator

```java
// testdata/src/main/java/com/catalyst2sql/testdata/DeltaTableGenerator.java
package com.catalyst2sql.testdata;

import io.delta.tables.DeltaTable;

/**
 * Generates Delta Lake tables with multiple versions for time travel testing.
 */
public class DeltaTableGenerator {

    public static void generateWithVersions(String tablePath, int versions) throws Exception {
        SparkSession spark = SparkSession.builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();

        // Initial data
        Dataset<Row> df = generateInitialData(1000);
        df.write().format("delta").mode("overwrite").save(tablePath);

        // Create multiple versions
        for (int v = 1; v < versions; v++) {
            Dataset<Row> updates = generateUpdates(100, v);

            DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);
            deltaTable.as("target")
                .merge(updates.as("source"), "target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

            System.out.println("Created version " + v);
        }

        spark.stop();
    }
}
```

### Iceberg Table Generator

```java
// testdata/src/main/java/com/catalyst2sql/testdata/IcebergTableGenerator.java
package com.catalyst2sql.testdata;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.*;

/**
 * Generates Iceberg tables with snapshots for testing.
 */
public class IcebergTableGenerator {

    public static void generateWithSnapshots(String tablePath, int snapshots) throws Exception {
        // Create Iceberg table with schema evolution
        // Generate multiple snapshots for time travel
    }
}
```

## 6. Data Generation CLI

### Command-Line Tool

```java
// testdata/src/main/java/com/catalyst2sql/testdata/DataGeneratorCLI.java
package com.catalyst2sql.testdata;

import picocli.CommandLine;

/**
 * Command-line tool for data generation.
 */
@CommandLine.Command(name = "datagen", mixinStandardHelpOptions = true)
public class DataGeneratorCLI implements Runnable {

    @CommandLine.Parameters(index = "0", description = "Generator type: synthetic, tpch, tpcds")
    private String generatorType;

    @CommandLine.Option(names = {"-s", "--scale"}, description = "Scale factor")
    private int scaleFactor = 1;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output path")
    private String outputPath = "data";

    @CommandLine.Option(names = {"-f", "--format"}, description = "Output format: parquet, delta, iceberg")
    private String format = "parquet";

    @Override
    public void run() {
        try {
            switch (generatorType.toLowerCase()) {
                case "synthetic":
                    generateSynthetic();
                    break;
                case "tpch":
                    TPCHDataGenerator.generate(scaleFactor, outputPath);
                    break;
                case "tpcds":
                    TPCDSDataGenerator.generate(scaleFactor, outputPath);
                    break;
                default:
                    System.err.println("Unknown generator type: " + generatorType);
                    System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataGeneratorCLI()).execute(args);
        System.exit(exitCode);
    }
}
```

### Usage

```bash
# Generate TPC-H at scale factor 10
java -jar testdata.jar tpch --scale 10 --output data/tpch_sf10

# Generate synthetic data
java -jar testdata.jar synthetic --rows 1000000 --output data/synthetic

# Generate Delta Lake table
java -jar testdata.jar synthetic --format delta --versions 5
```

## 7. Data Validation

### Validation Framework

```java
// testdata/src/main/java/com/catalyst2sql/testdata/DataValidator.java
package com.catalyst2sql.testdata;

/**
 * Validates generated data for correctness and consistency.
 */
public class DataValidator {

    public static ValidationResult validate(String dataPath, DataProfile profile) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        Dataset<Row> df = spark.read().parquet(dataPath);

        ValidationResult result = new ValidationResult();

        // Check row count
        long actualRows = df.count();
        result.addCheck("Row count", actualRows > 0);

        // Check nullability
        for (String field : profile.getFields()) {
            FieldProfile fp = profile.getFieldProfile(field);
            double actualNullRate = df.filter(col(field).isNull()).count() / (double) actualRows;
            double expectedNullRate = fp.nullProbability();

            result.addCheck(
                field + " null rate",
                Math.abs(actualNullRate - expectedNullRate) < 0.05  // 5% tolerance
            );
        }

        // Check value ranges
        // Check distributions
        // Check foreign key constraints (for TPC-H/TPC-DS)

        return result;
    }
}
```

## Summary

This data generation pipeline provides:

- **Synthetic Data**: Configurable schema-based generation with realistic distributions
- **TPC-H Benchmark**: Industry-standard OLAP workload (8 tables, 22 queries)
- **TPC-DS Benchmark**: Complex decision support workload (24 tables, 99 queries)
- **Format Support**: Parquet, Delta Lake, Iceberg
- **Data Profiles**: Realistic, testing, minimal configurations
- **Validation**: Automated data quality checks
- **CLI Tool**: Easy-to-use command-line interface

**Data Generation Targets:**
- Unit tests: 100-10K rows
- Integration tests: 100K-1M rows
- Benchmarks: 1M-100M rows (TPC-H SF 1-100)
- Performance tests: 100M-1B rows (TPC-H SF 100-1000)

**Next Steps:**
1. Implement synthetic data generator
2. Integrate TPC-H dbgen
3. Integrate TPC-DS dsdgen
4. Build format converters
5. Create CLI tool
6. Add data validation
