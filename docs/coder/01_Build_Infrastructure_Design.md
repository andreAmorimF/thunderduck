# Build Infrastructure Design for thunderduck

## Executive Summary

This document defines the complete build infrastructure for the thunderduck project, including build tools, dependency management, CI/CD integration, and performance optimization strategies.

**Key Decisions:**
- **Build Tool**: Maven 3.9+ (over Gradle/sbt)
- **Java Version**: Java 17 LTS for Phase 1-3, upgrade to Java 21 for Phase 4+
- **Module Structure**: Multi-module Maven project with 5 core modules
- **Testing Framework**: JUnit 5 with parallel execution
- **Benchmarking**: JMH (Java Microbenchmark Harness) + TPC-H/TPC-DS
- **CI/CD**: GitHub Actions with matrix builds (Intel/ARM)

## 1. Build Tool Selection: Maven

### Rationale for Maven over Gradle/sbt

| Criteria | Maven | Gradle | sbt | Winner |
|----------|-------|--------|-----|--------|
| Java ecosystem integration | Excellent | Very Good | Good | Maven |
| DuckDB/Arrow compatibility | Native | Native | Requires wrappers | Maven/Gradle |
| Build reproducibility | Excellent | Good | Fair | Maven |
| CI/CD simplicity | Excellent | Good | Fair | Maven |
| Learning curve | Low | Medium | High (Scala) | Maven |
| Dependency resolution | Stable | Fast | Slow | Gradle |
| Multi-module support | Excellent | Excellent | Good | Maven/Gradle |
| Documentation | Extensive | Extensive | Limited | Maven/Gradle |

**Final Decision**: Maven for stability, simplicity, and Java ecosystem maturity.

### Maven Version Requirements
- **Minimum**: Maven 3.9.0
- **Recommended**: Maven 3.9.6+
- **Reason**: Better dependency resolution, improved performance

## 2. Project Structure

### Multi-Module Organization

```
thunderduck/
├── pom.xml                           # Parent POM
├── core/                             # Core translation engine
│   ├── pom.xml
│   └── src/
│       ├── main/java/
│       │   └── com/thunderduck/
│       │       ├── logical/          # Logical plan nodes
│       │       ├── sql/              # SQL generation
│       │       ├── types/            # Type mapping
│       │       └── functions/        # Function registry
│       └── test/java/
│           └── com/thunderduck/
│               └── core/
├── formats/                          # Format readers
│   ├── pom.xml
│   └── src/
│       ├── main/java/
│       │   └── com/thunderduck/formats/
│       │       ├── parquet/          # Parquet reader/writer
│       │       ├── delta/            # Delta Lake support
│       │       └── iceberg/          # Iceberg support
│       └── test/java/
├── api/                              # Spark-compatible API
│   ├── pom.xml
│   └── src/
│       ├── main/java/
│       │   └── com/thunderduck/api/
│       │       ├── DataFrame.java
│       │       ├── SparkSession.java
│       │       ├── DataFrameReader.java
│       │       └── DataFrameWriter.java
│       └── test/java/
├── tests/                            # Comprehensive test suite
│   ├── pom.xml
│   └── src/
│       └── test/java/
│           └── com/thunderduck/tests/
│               ├── unit/             # Unit tests
│               ├── integration/      # Integration tests
│               └── differential/     # Spark comparison tests
├── benchmarks/                       # Performance benchmarks
│   ├── pom.xml
│   └── src/
│       └── main/java/
│           └── com/thunderduck/benchmarks/
│               ├── tpch/             # TPC-H queries
│               ├── tpcds/            # TPC-DS queries
│               └── micro/            # Micro-benchmarks
├── docs/                             # Documentation
└── scripts/                          # Build/test scripts
```

### Module Dependencies

```
api → formats → core
        ↓
tests → api + formats + core
benchmarks → api
```

## 3. Parent POM Configuration

### Key Properties

```xml
<project>
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.thunderduck</groupId>
    <artifactId>thunderduck-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>thunderduck</name>
    <description>High-performance Spark DataFrame to DuckDB SQL translation layer</description>

    <properties>
        <!-- Build -->
        <java.version>17</java.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <!-- Core Dependencies -->
        <duckdb.version>1.1.3</duckdb.version>
        <arrow.version>17.0.0</arrow.version>
        <spark.version>3.5.3</spark.version>

        <!-- Testing -->
        <junit.version>5.10.0</junit.version>
        <testcontainers.version>1.19.0</testcontainers.version>
        <assertj.version>3.24.2</assertj.version>

        <!-- Benchmarking -->
        <jmh.version>1.37</jmh.version>

        <!-- Logging -->
        <slf4j.version>2.0.9</slf4j.version>
        <logback.version>1.4.11</logback.version>

        <!-- Build Plugins -->
        <maven-compiler-plugin.version>3.11.0</maven-compiler-plugin.version>
        <maven-surefire-plugin.version>3.0.0</maven-surefire-plugin.version>
        <maven-failsafe-plugin.version>3.0.0</maven-failsafe-plugin.version>
        <maven-assembly-plugin.version>3.6.0</maven-assembly-plugin.version>
        <jacoco-maven-plugin.version>0.8.10</jacoco-maven-plugin.version>
    </properties>

    <modules>
        <module>core</module>
        <module>formats</module>
        <module>api</module>
        <module>tests</module>
        <module>benchmarks</module>
    </modules>
</project>
```

## 4. Dependency Management

### Core Dependencies

```xml
<dependencyManagement>
    <dependencies>
        <!-- DuckDB JDBC Driver -->
        <dependency>
            <groupId>org.duckdb</groupId>
            <artifactId>duckdb_jdbc</artifactId>
            <version>${duckdb.version}</version>
        </dependency>

        <!-- Apache Arrow (Zero-Copy Data Interchange) -->
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-vector</artifactId>
            <version>${arrow.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory-netty</artifactId>
            <version>${arrow.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-jdbc</artifactId>
            <version>${arrow.version}</version>
        </dependency>

        <!-- Spark SQL API (Provided - for interface compatibility) -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.13</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
            <exclusions>
                <!-- Exclude Spark's heavy dependencies -->
                <exclusion>
                    <groupId>org.apache.hadoop</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.apache.parquet</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Testing Dependencies -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-params</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- Full Spark for Differential Testing -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.13</artifactId>
            <version>${spark.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- Testcontainers for Integration Tests -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- JMH Benchmarking -->
        <dependency>
            <groupId>org.openjdk.jmh</groupId>
            <artifactId>jmh-core</artifactId>
            <version>${jmh.version}</version>
        </dependency>
        <dependency>
            <groupId>org.openjdk.jmh</groupId>
            <artifactId>jmh-generator-annprocess</artifactId>
            <version>${jmh.version}</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### DuckDB Extension Management

DuckDB extensions are loaded at runtime, not as Maven dependencies:

```java
// Extension installation happens at runtime
public class ExtensionManager {
    public void installExtensions(Connection conn) throws SQLException {
        // Delta Lake extension
        conn.execute("INSTALL delta");
        conn.execute("LOAD delta");

        // Iceberg extension
        conn.execute("INSTALL iceberg");
        conn.execute("LOAD iceberg");

        // S3/HTTP support
        conn.execute("INSTALL httpfs");
        conn.execute("LOAD httpfs");
    }
}
```

**Note**: No Maven dependencies required for DuckDB extensions.

## 5. Build Plugin Configuration

### Compiler Plugin

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>${maven-compiler-plugin.version}</version>
            <configuration>
                <source>${java.version}</source>
                <target>${java.version}</target>
                <encoding>${project.build.sourceEncoding}</encoding>
                <compilerArgs>
                    <arg>-parameters</arg>
                    <arg>-Xlint:all</arg>
                    <arg>-Werror</arg>
                </compilerArgs>
            </configuration>
        </plugin>
    </plugins>
</build>
```

### Test Execution Plugin

```xml
<!-- Surefire for Unit Tests -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>${maven-surefire-plugin.version}</version>
    <configuration>
        <!-- Parallel execution -->
        <parallel>classes</parallel>
        <threadCount>4</threadCount>
        <perCoreThreadCount>true</perCoreThreadCount>

        <!-- Memory settings -->
        <argLine>
            -Xmx2g
            -XX:+UseG1GC
            -XX:MaxGCPauseMillis=100
        </argLine>

        <!-- Test filtering -->
        <excludedGroups>integration,slow</excludedGroups>

        <!-- Reporting -->
        <reportFormat>plain</reportFormat>
        <consoleOutputReporter>
            <disable>false</disable>
        </consoleOutputReporter>
    </configuration>
</plugin>

<!-- Failsafe for Integration Tests -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-failsafe-plugin</artifactId>
    <version>${maven-failsafe-plugin.version}</version>
    <configuration>
        <includes>
            <include>**/*IT.java</include>
            <include>**/IT*.java</include>
        </includes>
        <argLine>
            -Xmx4g
            -XX:+UseG1GC
        </argLine>
    </configuration>
    <executions>
        <execution>
            <goals>
                <goal>integration-test</goal>
                <goal>verify</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

### Assembly Plugin (Fat JAR)

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>${maven-assembly-plugin.version}</version>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
        <archive>
            <manifest>
                <mainClass>com.thunderduck.Main</mainClass>
            </manifest>
        </archive>
    </configuration>
    <executions>
        <execution>
            <id>make-assembly</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

### Code Coverage Plugin

```xml
<plugin>
    <groupId>org.jacoco</groupId>
    <artifactId>jacoco-maven-plugin</artifactId>
    <version>${jacoco-maven-plugin.version}</version>
    <executions>
        <execution>
            <id>prepare-agent</id>
            <goals>
                <goal>prepare-agent</goal>
            </goals>
        </execution>
        <execution>
            <id>report</id>
            <phase>test</phase>
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
                                <minimum>0.80</minimum>
                            </limit>
                        </limits>
                    </rule>
                </rules>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## 6. Build Profiles

### Development Profiles

```xml
<profiles>
    <!-- Fast build: Skip tests -->
    <profile>
        <id>fast</id>
        <properties>
            <skipTests>true</skipTests>
        </properties>
    </profile>

    <!-- Code coverage -->
    <profile>
        <id>coverage</id>
        <build>
            <plugins>
                <plugin>
                    <groupId>org.jacoco</groupId>
                    <artifactId>jacoco-maven-plugin</artifactId>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- Benchmarks -->
    <profile>
        <id>benchmarks</id>
        <modules>
            <module>benchmarks</module>
        </modules>
    </profile>

    <!-- Release build -->
    <profile>
        <id>release</id>
        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-source-plugin</artifactId>
                    <version>3.3.0</version>
                    <executions>
                        <execution>
                            <id>attach-sources</id>
                            <goals>
                                <goal>jar-no-fork</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-javadoc-plugin</artifactId>
                    <version>3.5.0</version>
                    <executions>
                        <execution>
                            <id>attach-javadocs</id>
                            <goals>
                                <goal>jar</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </profile>
</profiles>
```

## 7. Build Commands

### Common Maven Commands

```bash
# Clean build with tests
mvn clean install

# Fast build (skip tests)
mvn clean install -Pfast

# Run tests with coverage
mvn clean verify -Pcoverage

# Run integration tests
mvn clean verify -DskipUnitTests

# Run benchmarks
mvn clean install -Pbenchmarks
cd benchmarks
java -jar target/benchmarks.jar

# Build release artifacts
mvn clean deploy -Prelease

# Run specific test
mvn test -Dtest=TypeMapperTest

# Debug build
mvn clean install -X -e
```

### Module-Specific Builds

```bash
# Build only core module
mvn clean install -pl core

# Build core and formats
mvn clean install -pl core,formats

# Build with dependencies
mvn clean install -pl api -am
```

## 8. Build Performance Optimization

### Parallel Builds

```bash
# Use all CPU cores
mvn clean install -T 1C

# Use 4 threads
mvn clean install -T 4
```

### Maven Daemon

```bash
# Install mvnd (Maven Daemon)
brew install mvnd  # macOS
sdk install mvnd   # Linux/SDKMAN

# Use mvnd instead of mvn
mvnd clean install  # 2-3x faster
```

### Dependency Resolution Optimization

```xml
<!-- In settings.xml -->
<settings>
    <mirrors>
        <mirror>
            <id>central-mirror</id>
            <mirrorOf>central</mirrorOf>
            <url>https://repo1.maven.org/maven2</url>
        </mirror>
    </mirrors>
</settings>
```

## 9. Continuous Integration

### Build Matrix Strategy

Test on multiple environments:
- **Operating Systems**: Ubuntu 22.04, macOS 13 (ARM64)
- **Java Versions**: 17, 21
- **Architectures**: x86_64 (Intel), aarch64 (ARM/Graviton)

### Resource Requirements

| Build Type | Memory | CPU | Duration |
|------------|--------|-----|----------|
| Unit tests | 2GB | 2 cores | 2-3 min |
| Integration tests | 4GB | 4 cores | 5-10 min |
| Full build + tests | 4GB | 4 cores | 10-15 min |
| Benchmarks | 16GB | 8 cores | 30-60 min |

## 10. Developer Experience

### IDE Integration

**IntelliJ IDEA**:
```xml
<!-- .idea/compiler.xml -->
<project>
    <component name="CompilerConfiguration">
        <bytecodeTargetLevel target="17" />
    </component>
</project>
```

**VS Code**:
```json
// .vscode/settings.json
{
    "java.configuration.updateBuildConfiguration": "automatic",
    "java.compile.nullAnalysis.mode": "automatic",
    "maven.executable.path": "/usr/local/bin/mvn"
}
```

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit

# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: maven-test
        name: Maven Test
        entry: mvn test
        language: system
        pass_filenames: false
```

## 11. Artifact Publishing

### Maven Central Deployment

```xml
<distributionManagement>
    <repository>
        <id>ossrh</id>
        <url>https://oss.sonatype.org/service/local/staging/deploy/maven2/</url>
    </repository>
    <snapshotRepository>
        <id>ossrh</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </snapshotRepository>
</distributionManagement>
```

### Release Process

```bash
# Set release version
mvn versions:set -DnewVersion=0.1.0

# Build and deploy
mvn clean deploy -Prelease

# Create git tag
git tag -a v0.1.0 -m "Release version 0.1.0"
git push origin v0.1.0
```

## 12. Build Monitoring

### Build Metrics

Track:
- Build duration
- Test execution time
- Dependency resolution time
- Code coverage percentage
- Test pass rate

### Alerting

Notify on:
- Build failures
- Coverage drops below 80%
- Test failures
- Dependency vulnerabilities

## Summary

This build infrastructure provides:
- **Fast builds**: Parallel execution, incremental compilation
- **Comprehensive testing**: Unit, integration, differential tests
- **Performance tracking**: JMH benchmarks, TPC-H/TPC-DS
- **Developer-friendly**: Simple Maven commands, IDE integration
- **CI/CD ready**: Matrix builds, automated testing
- **Production quality**: Code coverage, release automation

**Next Steps**:
1. Implement parent POM structure
2. Create module POMs
3. Set up CI/CD pipeline
4. Configure dependency management
5. Implement build automation scripts
