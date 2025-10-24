# Testing Infrastructure Design for thunderduck

## Executive Summary

This document defines a comprehensive testing strategy with four testing tiers: unit tests, integration tests, differential testing (against real Spark), and performance benchmarks. The goal is 80%+ code coverage with automated test data generation.

**Key Components:**
- **Unit Tests**: Fast, isolated tests with JUnit 5
- **Integration Tests**: End-to-end tests with DuckDB
- **Differential Tests**: Result comparison with real Spark 3.5
- **Performance Benchmarks**: JMH micro-benchmarks + TPC-H/TPC-DS
- **Test Data Generation**: Automated data generation for all formats

## 1. Testing Pyramid

```
                  ▲
                 / \
                /   \
               /  E2E \ (Benchmarks: TPC-H/TPC-DS)
              /-------\
             / Integ.  \ (Integration: Full workflows)
            /-----------\
           / Differential\ (Spark comparison tests)
          /---------------\
         /   Unit Tests    \ (Fast, isolated, 80%+ coverage)
        /___________________\
```

### Test Distribution Target

| Test Type | Count | Execution Time | Coverage |
|-----------|-------|----------------|----------|
| Unit tests | 500+ | 2-3 minutes | 80%+ |
| Integration tests | 100+ | 5-10 minutes | End-to-end workflows |
| Differential tests | 50+ | 10-15 minutes | Spark compatibility |
| Benchmarks | 20+ | 30-60 minutes | Performance validation |

## 2. Unit Testing Framework

### JUnit 5 Configuration

```java
// test/java/com/thunderduck/core/TypeMapperTest.java
package com.thunderduck.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

@DisplayName("Type Mapper Tests")
public class TypeMapperTest {

    @Test
    @DisplayName("Should map Spark IntegerType to DuckDB INTEGER")
    public void testIntegerMapping() {
        TypeMapper mapper = new TypeMapper();
        String duckdbType = mapper.toDuckDBType(DataTypes.IntegerType);
        assertThat(duckdbType).isEqualTo("INTEGER");
    }

    @ParameterizedTest
    @CsvSource({
        "BYTE, TINYINT",
        "SHORT, SMALLINT",
        "INTEGER, INTEGER",
        "LONG, BIGINT",
        "FLOAT, FLOAT",
        "DOUBLE, DOUBLE"
    })
    @DisplayName("Should map numeric types correctly")
    public void testNumericMappings(String sparkType, String expectedDuckDB) {
        TypeMapper mapper = new TypeMapper();
        DataType type = parseSparkType(sparkType);
        String result = mapper.toDuckDBType(type);
        assertThat(result).isEqualTo(expectedDuckDB);
    }

    @Test
    @DisplayName("Should map Decimal with precision and scale")
    public void testDecimalMapping() {
        TypeMapper mapper = new TypeMapper();
        DecimalType decimalType = DataTypes.createDecimalType(10, 2);
        String result = mapper.toDuckDBType(decimalType);
        assertThat(result).isEqualTo("DECIMAL(10,2)");
    }

    @Test
    @DisplayName("Should map nested struct types")
    public void testStructMapping() {
        TypeMapper mapper = new TypeMapper();
        StructType structType = new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType)
            .add("amount", DataTypes.createDecimalType(10, 2));

        String result = mapper.toDuckDBType(structType);
        assertThat(result).isEqualTo("STRUCT(id INTEGER, name VARCHAR, amount DECIMAL(10,2))");
    }
}
```

### Test Organization

```
tests/src/test/java/com/thunderduck/
├── core/
│   ├── TypeMapperTest.java           # Type mapping tests
│   ├── FunctionRegistryTest.java     # Function translation tests
│   ├── SQLGeneratorTest.java         # SQL generation tests
│   └── LogicalPlanTest.java          # Logical plan construction
├── formats/
│   ├── ParquetReaderTest.java        # Parquet reading tests
│   ├── DeltaReaderTest.java          # Delta Lake tests
│   └── IcebergReaderTest.java        # Iceberg tests
├── api/
│   ├── DataFrameTest.java            # DataFrame API tests
│   ├── DataFrameReaderTest.java      # Reader API tests
│   └── DataFrameWriterTest.java      # Writer API tests
├── integration/
│   ├── EndToEndWorkflowIT.java       # Full workflows
│   ├── PerformanceIT.java            # Performance tests
│   └── ConcurrencyIT.java            # Concurrent access
└── differential/
    ├── SelectFilterDiffTest.java     # Basic operations
    ├── AggregationDiffTest.java      # Aggregations
    ├── JoinDiffTest.java             # Joins
    └── WindowFunctionDiffTest.java   # Window functions
```

### Test Utilities

```java
// BaseTest.java
public abstract class BaseTest {
    protected static DuckDBConnection conn;
    protected static SparkSession spark;

    @BeforeAll
    public static void setupTestEnvironment() throws SQLException {
        // Initialize DuckDB
        conn = DriverManager.getConnection("jdbc:duckdb:");
        conn.setAutoCommit(false);

        // Initialize test Spark session (for differential tests)
        spark = SparkSession.builder()
            .appName("thunderduck-test")
            .master("local[*]")
            .getOrCreate();
    }

    @AfterAll
    public static void teardownTestEnvironment() throws SQLException {
        if (conn != null) conn.close();
        if (spark != null) spark.stop();
    }

    protected DataFrame createTestDataFrame(String tableName, int rows) {
        // Helper to create test data
    }

    protected void assertSchemaEquals(StructType expected, StructType actual) {
        // Compare Spark schemas
    }

    protected void assertDataEquals(Dataset<Row> expected, Dataset<Row> actual) {
        // Compare data row-by-row
    }
}
```

## 3. Integration Testing

### Integration Test Structure

```java
// integration/EndToEndWorkflowIT.java
@Tag("integration")
@DisplayName("End-to-End Workflow Tests")
public class EndToEndWorkflowIT extends BaseTest {

    @Test
    @DisplayName("Should read Parquet, filter, aggregate, and write results")
    public void testCompleteWorkflow() throws Exception {
        // 1. Generate test data
        generateTestParquet("input.parquet", 10000);

        // 2. Execute DataFrame operations
        DataFrame df = spark.read().parquet("input.parquet")
            .filter("age > 25")
            .groupBy("department")
            .agg(sum("salary").as("total_salary"), avg("age").as("avg_age"))
            .orderBy("total_salary", desc());

        // 3. Write results
        df.write().parquet("output.parquet");

        // 4. Verify results
        DataFrame result = spark.read().parquet("output.parquet");
        assertThat(result.count()).isGreaterThan(0);
        assertThat(result.columns()).containsExactly("department", "total_salary", "avg_age");
    }

    @Test
    @DisplayName("Should handle Delta Lake time travel")
    public void testDeltaTimeTravel() throws Exception {
        // Create Delta table with versions
        createDeltaTableWithVersions("delta_table", 5);

        // Read version 3
        DataFrame df = spark.read()
            .format("delta")
            .option("versionAsOf", "3")
            .load("delta_table");

        assertThat(df.count()).isEqualTo(expectedRowsForVersion3());
    }

    @Test
    @DisplayName("Should execute complex join operations")
    public void testComplexJoins() throws Exception {
        // Prepare test tables
        DataFrame customers = createCustomersTable(1000);
        DataFrame orders = createOrdersTable(5000);
        DataFrame products = createProductsTable(100);

        // Execute multi-way join
        DataFrame result = orders
            .join(customers, orders.col("customer_id").equalTo(customers.col("id")))
            .join(products, orders.col("product_id").equalTo(products.col("id")))
            .select("customer_name", "product_name", "order_amount")
            .filter("order_amount > 100");

        assertThat(result.count()).isGreaterThan(0);
    }
}
```

### Testcontainers Integration

```java
// For testing S3 interactions
@Testcontainers
public class S3IntegrationIT {

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer()
        .withServices(LocalStackContainer.Service.S3);

    @Test
    public void testReadFromS3() {
        // Configure S3 credentials for DuckDB
        String s3Endpoint = localstack.getEndpointOverride(LocalStackContainer.Service.S3);
        configureDuckDBForS3(s3Endpoint);

        // Upload test data
        uploadTestDataToS3("test-bucket", "data.parquet");

        // Read from S3
        DataFrame df = spark.read().parquet("s3://test-bucket/data.parquet");
        assertThat(df.count()).isGreaterThan(0);
    }
}
```

## 4. Differential Testing (Spark Comparison)

### Differential Test Framework

```java
// differential/BaseDifferentialTest.java
public abstract class BaseDifferentialTest {
    protected SparkSession realSpark;      // Real Apache Spark
    protected SparkSession thunderduck;   // Our implementation

    @BeforeEach
    public void setupSessions() {
        // Real Spark
        realSpark = SparkSession.builder()
            .appName("real-spark")
            .master("local[*]")
            .getOrCreate();

        // thunderduck implementation
        thunderduck = ThunderduckSession.builder()
            .appName("thunderduck")
            .getOrCreate();
    }

    protected void assertQueryResultsMatch(String parquetPath,
                                          Consumer<DataFrame> operations) {
        // Execute on real Spark
        Dataset<Row> sparkResult = operations.apply(
            realSpark.read().parquet(parquetPath)
        ).collect();

        // Execute on thunderduck
        Dataset<Row> c2sResult = operations.apply(
            thunderduck.read().parquet(parquetPath)
        ).collect();

        // Compare schemas
        assertSchemaEquals(sparkResult.schema(), c2sResult.schema());

        // Compare data (order-insensitive)
        assertDataEquals(sparkResult, c2sResult);
    }
}
```

### Differential Test Examples

```java
// differential/SelectFilterDiffTest.java
public class SelectFilterDiffTest extends BaseDifferentialTest {

    @Test
    @DisplayName("Differential: Select and Filter")
    public void testSelectFilter() {
        assertQueryResultsMatch("test.parquet", df ->
            df.select("name", "age", "salary")
              .filter("age > 30 AND salary > 50000")
        );
    }

    @Test
    @DisplayName("Differential: Multiple filters with OR")
    public void testComplexFilter() {
        assertQueryResultsMatch("test.parquet", df ->
            df.filter("(age < 25 OR age > 60) AND department = 'Engineering'")
        );
    }
}

// differential/AggregationDiffTest.java
public class AggregationDiffTest extends BaseDifferentialTest {

    @Test
    @DisplayName("Differential: GroupBy with Aggregations")
    public void testGroupByAgg() {
        assertQueryResultsMatch("test.parquet", df ->
            df.groupBy("department")
              .agg(
                  sum("salary").as("total_salary"),
                  avg("salary").as("avg_salary"),
                  count("*").as("count")
              )
        );
    }

    @Test
    @DisplayName("Differential: Window Functions")
    public void testWindowFunctions() {
        Window windowSpec = Window.partitionBy("department").orderBy("salary");

        assertQueryResultsMatch("test.parquet", df ->
            df.withColumn("rank", rank().over(windowSpec))
              .withColumn("row_num", row_number().over(windowSpec))
        );
    }
}
```

### Numerical Consistency Tests

```java
// differential/NumericalConsistencyTest.java
public class NumericalConsistencyTest extends BaseDifferentialTest {

    @Test
    @DisplayName("Integer division should match Java semantics")
    public void testIntegerDivision() {
        DataFrame df = createTestData(Arrays.asList(
            Row.of(10, 3),    // 10 / 3 = 3 (truncation)
            Row.of(-10, 3),   // -10 / 3 = -3 (not floor division)
            Row.of(10, -3),   // 10 / -3 = -3
            Row.of(-10, -3)   // -10 / -3 = 3
        ), schema("a INT, b INT"));

        assertQueryResultsMatch(df, d ->
            d.select(col("a").divide(col("b")).as("result"))
        );
    }

    @Test
    @DisplayName("Decimal arithmetic should maintain precision")
    public void testDecimalPrecision() {
        DataFrame df = createDecimalTestData();

        assertQueryResultsMatch(df, d ->
            d.select(
                col("price").multiply(col("quantity")).as("subtotal"),
                col("subtotal").multiply(col("tax_rate")).as("tax")
            )
        );
    }

    @Test
    @DisplayName("Overflow behavior should match Java")
    public void testIntegerOverflow() {
        DataFrame df = createTestData(Arrays.asList(
            Row.of(Integer.MAX_VALUE, 1),
            Row.of(Integer.MIN_VALUE, -1)
        ), schema("a INT, b INT"));

        assertQueryResultsMatch(df, d ->
            d.select(col("a").plus(col("b")).as("result"))
        );
    }
}
```

## 5. Performance Benchmarking

### JMH Benchmark Configuration

```xml
<!-- benchmarks/pom.xml -->
<project>
    <dependencies>
        <dependency>
            <groupId>org.openjdk.jmh</groupId>
            <artifactId>jmh-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.openjdk.jmh</groupId>
            <artifactId>jmh-generator-annprocess</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <finalName>benchmarks</finalName>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>org.openjdk.jmh.Main</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

### Micro-Benchmarks

```java
// benchmarks/src/main/java/com/thunderduck/benchmarks/ParquetReadBenchmark.java
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ParquetReadBenchmark {

    private SparkSession realSpark;
    private SparkSession thunderduck;
    private String testFile;

    @Setup
    public void setup() throws Exception {
        // Generate 1GB test Parquet file
        testFile = generateParquetFile(1_000_000_000L);

        realSpark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        thunderduck = ThunderduckSession.builder()
            .getOrCreate();
    }

    @Benchmark
    public long benchmarkSparkRead() {
        return realSpark.read().parquet(testFile).count();
    }

    @Benchmark
    public long benchmarkThunderduckRead() {
        return thunderduck.read().parquet(testFile).count();
    }

    @TearDown
    public void teardown() {
        realSpark.stop();
        thunderduck.stop();
        new File(testFile).delete();
    }
}
```

### TPC-H Benchmark Suite

```java
// benchmarks/src/main/java/com/thunderduck/benchmarks/TPCHBenchmark.java
@State(Scope.Benchmark)
public class TPCHBenchmark {

    @Param({"1", "10", "100"})  // Scale factors
    private int scaleFactor;

    private SparkSession session;
    private String dataPath;

    @Setup
    public void setup() throws Exception {
        // Generate TPC-H data
        dataPath = TPCHDataGenerator.generate(scaleFactor);

        session = ThunderduckSession.builder().getOrCreate();

        // Load tables
        loadTPCHTables(session, dataPath);
    }

    @Benchmark
    public void benchmarkQuery1() {
        // TPC-H Q1: Pricing Summary Report
        session.sql(TPCH_Q1).collect();
    }

    @Benchmark
    public void benchmarkQuery3() {
        // TPC-H Q3: Shipping Priority
        session.sql(TPCH_Q3).collect();
    }

    @Benchmark
    public void benchmarkQuery6() {
        // TPC-H Q6: Forecasting Revenue Change
        session.sql(TPCH_Q6).collect();
    }

    // ... other TPC-H queries
}
```

## 6. Test Data Generation

### Parquet Data Generator

```java
// test/java/com/thunderduck/testdata/ParquetGenerator.java
public class ParquetGenerator {

    public static void generateParquet(String path, Schema schema, long rows)
            throws Exception {
        Configuration conf = new Configuration();
        Path file = new Path(path);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(file)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            Random random = new Random(42);  // Deterministic

            for (long i = 0; i < rows; i++) {
                GenericRecord record = new GenericData.Record(schema);

                // Generate data based on schema
                for (Schema.Field field : schema.getFields()) {
                    Object value = generateValue(field.schema(), random);
                    record.put(field.name(), value);
                }

                writer.write(record);
            }
        }
    }

    private static Object generateValue(Schema schema, Random random) {
        return switch (schema.getType()) {
            case INT -> random.nextInt(10000);
            case LONG -> random.nextLong();
            case FLOAT -> random.nextFloat() * 1000;
            case DOUBLE -> random.nextDouble() * 1000;
            case STRING -> "value_" + random.nextInt(1000);
            case BOOLEAN -> random.nextBoolean();
            // ... other types
        };
    }
}
```

### TPC-H Data Generation

```java
// benchmarks/src/main/java/com/thunderduck/benchmarks/datagen/TPCHDataGenerator.java
public class TPCHDataGenerator {

    public static String generate(int scaleFactor) throws Exception {
        String outputPath = "/tmp/tpch_sf" + scaleFactor;

        // Use dbgen from TPC-H toolkit
        ProcessBuilder pb = new ProcessBuilder(
            "./dbgen",
            "-s", String.valueOf(scaleFactor),
            "-T", "a",  // Generate all tables
            "-f"         // Force overwrite
        );
        pb.directory(new File(TPCH_TOOLKIT_PATH));
        Process process = pb.start();
        process.waitFor();

        // Convert .tbl files to Parquet
        convertTblToParquet(outputPath);

        return outputPath;
    }

    private static void convertTblToParquet(String path) throws Exception {
        // Read .tbl files and write as Parquet
        for (String table : TPCH_TABLES) {
            String tblFile = path + "/" + table + ".tbl";
            String parquetFile = path + "/" + table + ".parquet";

            SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

            Dataset<Row> df = spark.read()
                .option("delimiter", "|")
                .schema(getSchemaForTable(table))
                .csv(tblFile);

            df.write()
                .mode("overwrite")
                .parquet(parquetFile);
        }
    }
}
```

### TPC-DS Data Generation

```java
// Similar to TPC-H but using dsdgen tool
public class TPCDSDataGenerator {
    public static String generate(int scaleFactor) {
        // Use dsdgen from TPC-DS toolkit
        // Generate 24 tables
        // Convert to Parquet format
    }
}
```

## 7. Test Execution Strategy

### Local Development

```bash
# Run fast unit tests only
mvn test

# Run all tests including integration
mvn verify

# Run specific test class
mvn test -Dtest=TypeMapperTest

# Run with coverage
mvn verify -Pcoverage

# Run benchmarks
mvn clean install -Pbenchmarks
cd benchmarks
java -jar target/benchmarks.jar
```

### CI/CD Pipeline

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java: [17, 21]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: ${{ matrix.java }}
      - name: Run unit tests
        run: mvn test
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: 17
      - name: Run integration tests
        run: mvn verify

  differential-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: 17
      - name: Generate test data
        run: mvn test-compile exec:java -Dexec.mainClass="com.thunderduck.testdata.DataGenerator"
      - name: Run differential tests
        run: mvn test -Dgroups=differential

  benchmarks:
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: 17
      - name: Run benchmarks
        run: |
          mvn clean install -Pbenchmarks
          cd benchmarks
          java -jar target/benchmarks.jar -rf json -rff results.json
      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
```

## 8. Test Coverage Requirements

### Coverage Targets

| Component | Line Coverage | Branch Coverage |
|-----------|---------------|-----------------|
| Core (logical plan, SQL generation) | 90%+ | 85%+ |
| Type mapping | 95%+ | 90%+ |
| Function registry | 90%+ | 85%+ |
| Format readers | 85%+ | 80%+ |
| API layer | 85%+ | 80%+ |
| **Overall Target** | **85%+** | **80%+** |

### Coverage Reporting

```xml
<plugin>
    <groupId>org.jacoco</groupId>
    <artifactId>jacoco-maven-plugin</artifactId>
    <executions>
        <execution>
            <id>report</id>
            <phase>verify</phase>
            <goals>
                <goal>report</goal>
            </goals>
        </execution>
        <execution>
            <id>check</id>
            <goals>
                <goal>check</goal>
            </goals>
            <configuration>
                <rules>
                    <rule>
                        <element>BUNDLE</element>
                        <limits>
                            <limit>
                                <counter>LINE</counter>
                                <value>COVEREDRATIO</value>
                                <minimum>0.85</minimum>
                            </limit>
                        </limits>
                    </rule>
                </rules>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## 9. Test Infrastructure Summary

### Test Automation Tooling

| Tool | Purpose | Version |
|------|---------|---------|
| JUnit 5 | Unit testing framework | 5.10.0 |
| AssertJ | Fluent assertions | 3.24.2 |
| Testcontainers | Integration testing | 1.19.0 |
| JMH | Performance benchmarking | 1.37 |
| JaCoCo | Code coverage | 0.8.10 |
| dbgen | TPC-H data generation | Latest |
| dsdgen | TPC-DS data generation | Latest |

### Test Data Repositories

- **Unit test data**: Synthetic, small datasets (<1MB)
- **Integration test data**: Medium datasets (10MB-1GB)
- **Benchmark data**: Large datasets (1GB-100GB)
- **Differential test data**: Shared with Spark for comparison

### Success Metrics

- **Test execution time**: <15 minutes for full suite
- **Code coverage**: 85%+ overall
- **Differential test pass rate**: 100%
- **Benchmark performance**: 80-90% of native DuckDB
- **Test reliability**: <1% flakiness

## Next Steps

1. Implement JUnit 5 test framework
2. Create test data generators
3. Build differential testing harness
4. Set up JMH benchmarks
5. Integrate with CI/CD pipeline
6. Generate TPC-H and TPC-DS datasets
