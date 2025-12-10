# Protobuf and Apache Arrow Configuration Guide

**Purpose**: Reference document for resolving protobuf version conflicts and Apache Arrow platform-specific requirements.

---

## Protobuf Version Conflict

### Symptoms
```
java.lang.VerifyError: Bad type on operand stack
Type 'org/apache/spark/connect/proto/Relation' is not assignable to 'com/google/protobuf/AbstractMessage'
```

### Root Cause
The `spark-connect_2.13` dependency includes pre-compiled protobuf classes. When these conflict with our protobuf version, `VerifyError` occurs at runtime.

### Solution
Use `provided` scope for `spark-connect_2.13` to exclude pre-compiled protobuf classes:

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-connect_2.13</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>  <!-- CRITICAL: Must be 'provided' -->
</dependency>
```

**Why this works**: `provided` scope excludes Spark's protobuf classes from the runtime classpath, using only our generated protobuf stubs which are compiled against our protobuf version.

---

## Apache Arrow on ARM64

### Symptoms
```
java.lang.RuntimeException: Failed to initialize MemoryUtil.
You must start Java with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
```

### Affected Platforms
- AWS Graviton (ARM64)
- Apple Silicon (M1/M2/M3)
- Any ARM64 Linux system

### Solution
Add JVM flag when starting the server:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar
```

### For Maven Execution
```bash
export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"
mvn exec:java -pl connect-server
```

---

## Quick Reference

### Running the Server
```bash
# Recommended: Use start script (already configured)
./tests/scripts/start-server.sh

# Direct JAR execution
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar
```

### Building
```bash
# Always clean build when diagnosing issues
mvn clean package -pl connect-server -DskipTests
```

### Common Pitfalls
1. **Stale classes**: Always run `mvn clean` when changing dependencies
2. **Scope confusion**: `compile` scope bundles conflicting classes; use `provided`
3. **Missing JVM flags**: ARM64 requires Arrow memory access flags

---

## Verification

After configuration, verify with:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.sql("SELECT 1").show()  # Should succeed
```

---

*Original issue resolved: November 2025*
