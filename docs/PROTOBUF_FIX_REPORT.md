# ThunderDuck Server Protobuf Issue - Resolution Report

## Date: November 5, 2025

## Issue Summary
The ThunderDuck server was experiencing runtime errors preventing client connections, initially appearing as a compilation error but ultimately revealing two distinct issues with protobuf versions and Apache Arrow on ARM64.

## Problems Identified

### 1. Initial Compilation Error
**Error**: `The method addService(ServerServiceDefinition) is not applicable for the arguments (SparkConnectServiceImpl)`
**Cause**: Stale compiled classes from previous builds
**Solution**: Clean rebuild (`mvn clean compile`)

### 2. Protobuf Version Conflict (VerifyError)
**Error**:
```
java.lang.VerifyError: Bad type on operand stack
Type 'org/apache/spark/connect/proto/Relation' is not assignable to 'com/google/protobuf/AbstractMessage'
```
**Cause**: The `spark-connect_2.13` dependency was set to `compile` scope, including pre-compiled protobuf classes that were compiled with a different protobuf version than we're using (3.23.4).
**Solution**: Changed `spark-connect_2.13` dependency scope from `compile` to `provided` to avoid bundling conflicting protobuf classes.

### 3. Apache Arrow Memory Access on ARM64
**Error**:
```
java.lang.RuntimeException: Failed to initialize MemoryUtil.
You must start Java with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
```
**Cause**: Apache Arrow 17.0.0 requires special JVM flags on ARM64 platforms to access internal Java NIO classes.
**Solution**: Added `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM flag when starting the server.

## Fixes Applied

### 1. Maven POM Update
**File**: `/workspace/connect-server/pom.xml`
```xml
<!-- Changed from compile to provided scope -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-connect_2.13</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>
```

### 2. Server Startup Command
**Command**:
```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar
```

### 3. Start Script Already Updated
**File**: `/workspace/start-server.sh`
- Already contains `MAVEN_OPTS` with the required JVM flags

## Testing Results

✅ Server starts successfully on port 15002
✅ PySpark client connects without errors
✅ Simple SQL queries execute correctly
✅ SQL functions work (arithmetic, string operations)
✅ Results are returned to client properly

## Test Output
```
Testing ThunderDuck server on port 15002...
✅ Connected to ThunderDuck server
✅ Simple query works!
✅ SQL functions work!
✅ String operations work!
==================================================
🎉 ALL TESTS PASSED! ThunderDuck server is working!
==================================================
```

## Lessons Learned

1. **Dependency Scoping**: When using pre-compiled protobuf classes from external JARs, version conflicts can occur. Using `provided` scope and generating our own protobuf classes ensures compatibility.

2. **Platform-Specific Requirements**: Apache Arrow has specific requirements on ARM64 platforms that require JVM flags. This is documented but easy to miss.

3. **Clean Builds Matter**: Stale compiled classes can mask the real issues. Always start with a clean build when diagnosing compilation/runtime errors.

## Next Steps

1. ✅ Server is now operational and ready for Week 17 differential testing
2. ✅ Can proceed with TPC-DS DataFrame API validation
3. Consider adding these JVM flags to all Maven configurations for consistency
4. Update documentation to note ARM64 requirements

## Configuration for Future Reference

### Running the Server
```bash
# Option 1: Using the start script (recommended)
./start-server.sh

# Option 2: Direct JAR execution
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar
```

### Building the Server
```bash
mvn clean package -pl connect-server -DskipTests
```

## Status: RESOLVED ✅

The ThunderDuck server is now fully operational and can handle Spark Connect protocol requests from PySpark clients.