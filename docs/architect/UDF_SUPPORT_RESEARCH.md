# Arrow-Optimized Python UDF Support in Thunderduck

**Date:** 2025-12-30 (Restructured)
**Status:** Research Complete
**Purpose:** Implementing Arrow-optimized Python UDF support via Spark Connect Client API

---

## Executive Summary

### Goal

Enable Python UDF execution in Thunderduck that matches Apache Spark 4.x+ behavior and semantics, using the Spark Connect protocol for UDF registration and Arrow-optimized batch execution.

### Scope

**In Scope (Primary Focus):**
- Arrow-optimized Python UDFs via Spark Connect Client API
- GraalPy for Python execution in JVM
- Java FFM (Panama) for DuckDB native API integration

**Out of Scope (Deferred):**
- Java/Scala UDFs (not supported by Spark Connect Client API)
- Row-based Python UDFs (Arrow-optimized is the default in Spark 4.x)

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ PySpark Client                                                   │
│   spark.udf.register("my_func", lambda x: x*2, IntegerType())   │
│                              ↓                                   │
│   Spark Connect sends PythonUDF (CloudPickle + eval_type)       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Thunderduck JVM                                                  │
│                                                                  │
│   1. Receive CommonInlineUserDefinedFunction protobuf           │
│   2. Deserialize CloudPickle'd function via GraalPy             │
│   3. Register with DuckDB via Java FFM (Panama)                 │
│                                                                  │
│   On query execution (per 2048-row batch):                      │
│   4. DuckDB calls registered scalar function                    │
│   5. FFM upcall → Java → GraalPy executes Python UDF            │
│   6. Arrow zero-copy result back to DuckDB                      │
└─────────────────────────────────────────────────────────────────┘
```

### Key Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Python Runtime | GraalPy | Runs on standard JVM, 4x faster after warmup |
| DuckDB Integration | Java FFM (Panama) | Arrow zero-copy, 2-5x faster than JNI |
| Serialization | CloudPickle | Spark Connect standard |

---

## 1. Arrow-Optimized Python UDFs in Apache Spark 4.x

### 1.1 Spark Connect Protocol

When a user registers a Python UDF via PySpark:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def double_it(x):
    return x * 2

spark.udf.register("double_it", double_it)
```

The Spark Connect client sends a `CommonInlineUserDefinedFunction` protobuf:

```protobuf
message CommonInlineUserDefinedFunction {
  string function_name = 1;
  repeated Expression arguments = 2;
  bool deterministic = 3;
  oneof function {
    PythonUDF py_udf = 4;
    ScalarScalaUDF scalar_scala_udf = 5;
    JavaUDF java_udf = 6;
  }
  bool is_distinct = 7;  // Spark 4.0+
}

message PythonUDF {
  bytes command = 1;           // CloudPickle serialized function
  int32 eval_type = 2;         // SQL_BATCHED_UDF, SQL_ARROW_BATCHED_UDF, etc.
  DataType return_type = 3;
  repeated bytes additional_includes = 4;  // Spark 4.0+
}
```

### 1.2 Arrow-Optimized UDFs (eval_type)

Spark 3.5+ introduced [Arrow-optimized Python UDFs](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35) which are **1.6x faster** than row-based UDFs:

| eval_type | Description | Performance |
|-----------|-------------|-------------|
| `SQL_BATCHED_UDF` | Row-based, one call per row | Baseline |
| `SQL_ARROW_BATCHED_UDF` | Arrow batch, one call per batch | **1.6x faster** |
| `SQL_SCALAR_PANDAS_UDF` | Pandas Series input/output | ~1.6x faster |

**Spark 4.x default**: Arrow-optimized UDFs are the default for scalar UDFs.

### 1.3 CloudPickle Serialization

[CloudPickle](https://github.com/cloudpipe/cloudpickle) serializes Python functions by value, including:
- Function bytecode
- Closure variables
- Referenced globals

**Critical requirement:** Deserialization requires the **same Python version** as serialization.

---

## 2. Thunderduck Implementation Approach

### 2.1 Receiving UDFs via Spark Connect

Handle the `CommonInlineUserDefinedFunction` protobuf in the Spark Connect service:

```java
public class UDFRegistry {
    private final Map<String, RegisteredUDF> udfs = new ConcurrentHashMap<>();
    private final Context graalPyContext;

    public void registerPythonUDF(String name, PythonUDF pyUdf) {
        byte[] pickledFunction = pyUdf.getCommand().toByteArray();
        int evalType = pyUdf.getEvalType();
        DataType returnType = pyUdf.getReturnType();

        // Store for later execution
        udfs.put(name, new RegisteredUDF(pickledFunction, evalType, returnType));

        // Register with DuckDB via FFM
        registerWithDuckDB(name, returnType);
    }
}
```

### 2.2 Python Runtime: GraalPy

**Key Finding: GraalPy runs on ANY standard JVM - no GraalVM required!**

#### Maven Dependencies (v25.0.1)

```xml
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>polyglot</artifactId>
    <version>25.0.1</version>
</dependency>
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>python</artifactId>
    <version>25.0.1</version>
    <type>pom</type>
</dependency>
```

#### GraalPy Maven Plugin (bundles cloudpickle)

```xml
<plugin>
    <groupId>org.graalvm.python</groupId>
    <artifactId>graalpy-maven-plugin</artifactId>
    <version>25.0.1</version>
    <configuration>
        <packages>
            <package>cloudpickle==3.0.0</package>
        </packages>
    </configuration>
</plugin>
```

#### CloudPickle Deserialization

```java
public class PythonUDFExecutor {
    private final Context context;

    public PythonUDFExecutor() {
        this.context = Context.newBuilder("python")
            .allowAllAccess(true)
            .build();
        context.eval("python", "import cloudpickle, base64");
    }

    public Value deserializeUDF(byte[] pickledData) {
        String b64Data = Base64.getEncoder().encodeToString(pickledData);
        Value deserializer = context.eval("python", """
            def unpickle(b64):
                return cloudpickle.loads(base64.b64decode(b64))
            unpickle
        """);
        return deserializer.execute(b64Data);
    }

    public Object executeUDF(Value udfFunction, Object... args) {
        return udfFunction.execute(args);
    }
}
```

#### Performance Characteristics

| Scenario | vs CPython |
|----------|------------|
| Pure Python (after warmup) | **4x faster** |
| With C extensions (NumPy) | Slower (emulation) |
| Short scripts (no warmup) | Slower |

**Memory:** ~50-100MB base + ~5-10MB per context

#### Limitations

- C extensions must be rebuilt for GraalPy (no PyPI wheels)
- Windows: Pure Python only, no C extensions
- Multi-context with C extensions: Linux only, experimental

### 2.3 Integrating with DuckDB Execution

#### DuckDB Vectorized Execution Model

DuckDB processes data in **batches of 2048 rows** (DataChunks):

```
Query: SELECT my_udf(x) FROM table
                    ↓
        DuckDB Query Planner
                    ↓
        Vectorized Execution Engine
                    ↓
    ┌─────────────────────────────────┐
    │  DataChunk (2048 rows)          │
    │  ├── Vector: x values           │
    │  └── Vector: result (empty)     │
    └─────────────────────────────────┘
                    ↓
        Scalar Function Callback
        (receives DataChunk, fills result Vector)
                    ↓
        Continue query execution
```

**Key optimization:** UDFs are called once per **batch** (2048 rows), not per row. This amortizes call overhead.

#### DuckDB Native API Access: Java FFM (Panama)

**Recommendation**: Java FFM (Foreign Function & Memory API) is preferred for Arrow-optimized Python UDFs.

| Option | Arrow Zero-Copy | Build Complexity | Performance | Java Version |
|--------|-----------------|------------------|-------------|--------------|
| **Java FFM (Panama)** | Yes (MemorySegment wraps Arrow) | Low (pure Java) | 2-5x faster than JNI | 22+ |
| JNI Bridge | Manual copy | High (CMake, native) | Baseline | 17+ |

**Why FFM for Arrow-optimized UDFs:**

1. **Arrow zero-copy**: FFM `MemorySegment` can directly wrap Arrow buffers
2. **Pure Java**: No native compilation needed, `jextract` generates bindings from `duckdb.h`
3. **Performance**: 2-5x faster than JNI (based on SQLite benchmarks: 44ms → 23ms)
4. **Callbacks**: `Linker::upcallStub()` supports DuckDB → Java callbacks

#### FFM Implementation Sketch

```java
// Generate bindings from duckdb.h using jextract:
// jextract --output src -t org.duckdb.ffm -l duckdb duckdb.h

public class DuckDBFFMBridge {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup DUCKDB = SymbolLookup.libraryLookup("libduckdb.so", Arena.global());

    // Create upcall stub for UDF callback
    public static MemorySegment createUDFCallback(MethodHandle udfHandler) {
        return LINKER.upcallStub(
            udfHandler,
            FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS),  // (info, input, output)
            Arena.global()
        );
    }

    // Register scalar function with DuckDB
    public static void registerScalarFunction(MemorySegment connection, String name,
                                              MemorySegment callback) {
        // Call duckdb_create_scalar_function()
        // Call duckdb_scalar_function_set_name()
        // Call duckdb_scalar_function_set_function(callback)
        // Call duckdb_register_scalar_function(connection, func)
    }
}
```

#### Arrow Zero-Copy Data Transfer

FFM enables zero-copy Arrow data transfer:

```java
// DuckDB DataChunk → Arrow RecordBatch (zero-copy)
public ArrowRecordBatch dataChunkToArrow(MemorySegment dataChunk) {
    // DuckDB vectors are Arrow-compatible
    // Wrap MemorySegment as Arrow buffer without copying
    MemorySegment vectorData = duckdb_vector_get_data(dataChunk);
    ArrowBuf arrowBuf = new ArrowBuf(vectorData.address(), vectorData.byteSize());
    // ...
}

// Arrow result → DuckDB output vector (zero-copy)
public void arrowToOutputVector(ArrowArray result, MemorySegment outputVector) {
    // Copy Arrow array to DuckDB vector
    // For zero-copy: use shared memory or Arrow C Data Interface
}
```

### 2.4 Implementation Phases

1. **GraalPy integration + CloudPickle deserialization**
   - Add GraalPy dependencies
   - Implement CloudPickle deserialization
   - Test with simple Python functions

2. **Java FFM bindings for DuckDB scalar function registration**
   - Generate bindings with jextract
   - Implement `registerScalarFunction()` via FFM
   - Set up callback mechanism with `upcallStub()`

3. **Arrow zero-copy data transfer**
   - Implement DuckDB DataChunk → Arrow conversion
   - Implement Arrow → DuckDB output vector conversion
   - Optimize for zero-copy where possible

4. **End-to-end Arrow-optimized UDF execution**
   - Handle `CommonInlineUserDefinedFunction` in Spark Connect service
   - Wire up complete data flow
   - Performance testing and optimization

---

## 3. Other Ways of Implementing UDFs

This section documents alternative approaches that are either:
- Not currently prioritized (Java/Scala UDFs)
- Alternative implementations (IPC, Arrow Flight)
- Fallback options (JNI if FFM not viable)

### 3.1 Java/Scala UDFs

**Status:** Not currently supported by Spark Connect Client API.

The Spark Connect protocol includes `ScalarScalaUDF` and `JavaUDF` message types, but the PySpark client does not expose APIs to register them. Supporting these would require:

- Custom protocol extension, or
- Arrow Flight-based UDF execution

#### Direct JVM Execution (if supported)

```java
// Deserialize and load UDF class from Spark Connect
byte[] classBytes = scalarScalaUdf.getSerializedUdfFunction().toByteArray();
CustomClassLoader loader = new CustomClassLoader();
Class<?> udfClass = loader.defineClass(classBytes);
Method applyMethod = udfClass.getMethod("apply", Object[].class);
Object result = applyMethod.invoke(udfInstance, arrowBatch);
```

**Challenges:** Security concerns, classloader complexity, no client API support.

### 3.2 Alternative DuckDB Integration Approaches

#### 3.2.1 DuckDB Extension with IPC

Create a DuckDB extension that communicates with Thunderduck JVM via IPC:

```
┌─────────────────────────────────────┐
│ DuckDB Process                      │
│  thunderduck_udf extension          │
│  - Serialize chunk → Arrow          │
│  - Send via Unix socket             │
│  - Receive result                   │
└───────────────┬─────────────────────┘
                │ Unix Socket / Shared Memory
                ▼
┌─────────────────────────────────────┐
│ Thunderduck JVM (UDF Server)        │
│  - Execute Python/Java UDF          │
│  - Return Arrow result              │
└─────────────────────────────────────┘
```

**Pros:** Clean separation, no JNI complexity
**Cons:** IPC overhead, complex deployment

#### 3.2.2 Arrow Flight (Airport Pattern)

The [Airport extension](https://airport.query.farm/) demonstrates remote function execution via Arrow Flight:

```java
// Thunderduck as Arrow Flight server for UDFs
public class UDFFlightServer extends FlightServer {
    @Override
    public void doExchange(CallContext context, FlightStream reader,
                           ServerStreamListener writer) {
        while (reader.next()) {
            VectorSchemaRoot batch = reader.getRoot();
            VectorSchemaRoot result = executeUDF(udf, batch);
            writer.putNext(result);
        }
        writer.completed();
    }
}
```

**When to use:** Sharing UDF execution across multiple DuckDB instances, acceptable network latency.

#### 3.2.3 SQL Macro Translation (Simple Cases)

For simple UDFs expressible as SQL, translate to DuckDB macros:

```python
# Simple UDF
@udf(returnType=IntegerType())
def double_it(x):
    return x * 2

# Translate to DuckDB macro
# CREATE MACRO double_it(x) AS x * 2;
```

| Pattern | Translatable | Example |
|---------|--------------|---------|
| Arithmetic | Yes | `x * 2 + 1` |
| String concat | Yes | `first \|\| ' ' \|\| last` |
| CASE/WHEN | Yes | `CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END` |
| Built-in functions | Yes | `UPPER(name)` |
| Python stdlib | No | `math.sqrt(x)` |
| External libraries | No | `numpy.sin(x)` |

**Limitation:** Only works for simple expressions, not general Python code.

### 3.3 Alternative Python Runtimes

#### 3.3.1 Jep (Java Embedded Python)

[Jep](https://github.com/ninia/jep) embeds CPython via JNI:

```java
try (Interpreter interp = new SharedInterpreter()) {
    interp.exec("import cloudpickle");
    interp.set("pickled_func", pickledBytes);
    interp.exec("func = cloudpickle.loads(pickled_func)");
    interp.set("data", arrowData);
    Object result = interp.getValue("func(data)");
}
```

**Pros:** Full CPython compatibility, all packages work
**Cons:** Native dependency (libpython), GIL contention, platform-specific

#### 3.3.2 WebAssembly (Not Recommended)

| Tool | Performance | Notes |
|------|-------------|-------|
| [Pyodide](https://pyodide.org/) | 3-10x slower | CPython to WASM |
| [Chicory](https://github.com/dylibso/chicory) | Moderate | Pure JVM WASM runtime |

**Not recommended:** CloudPickle deserialization is complex, 3-10x slower than native.

### 3.4 Alternative Native API Access Methods (if FFM not viable)

If Java 22+ is not available (Java 17 compatibility required), these alternatives exist:

#### 3.4.1 JNI Bridge (Fallback)

Build a native library that exposes DuckDB's scalar function registration:

```c
// libthunderduck_udf.c
JNIEXPORT void JNICALL Java_UDFBridge_registerScalarFunction(
    JNIEnv *env, jclass cls, jlong conn_ptr, jstring name, jlong callback_id) {

    duckdb_connection conn = (duckdb_connection)conn_ptr;
    duckdb_scalar_function func = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(func, name);
    duckdb_scalar_function_set_function(func, udf_callback);
    duckdb_register_scalar_function(conn, func);
}

void udf_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    // JNI upcall to Java
    (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
    jbyteArray arrow_input = convert_to_arrow_ipc(env, input);
    jbyteArray result = (*env)->CallStaticObjectMethod(env, executorClass, executeMethod, arrow_input);
    copy_arrow_to_vector(env, result, output);
}
```

**Challenges:**
- Build complexity (CMake, native compilation)
- Thread safety across JNI boundary
- Manual Arrow IPC serialization (no zero-copy)

#### 3.4.2 Reflection to Access JDBC Connection Pointer

Extract native pointer from DuckDB JDBC connection:

```java
DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
Field connRefField = DuckDBConnection.class.getDeclaredField("conn_ref");
connRefField.setAccessible(true);
ByteBuffer connRef = (ByteBuffer) connRefField.get(conn);
long nativePtr = connRef.getLong(0);  // Extract raw pointer
```

**Pros:** Works with existing JDBC driver
**Cons:** Fragile, depends on internal API, may break between versions

#### 3.4.3 JNA / JNR-FFI

Create bindings to DuckDB C API without writing native code:

```java
// JNA example
public interface DuckDBLib extends Library {
    DuckDBLib INSTANCE = Native.load("duckdb", DuckDBLib.class);
    Pointer duckdb_create_scalar_function();
    void duckdb_scalar_function_set_name(Pointer func, String name);
    int duckdb_register_scalar_function(Pointer connection, Pointer func);
}
```

**Pros:** No native code to compile, portable
**Cons:** Separate DuckDB connection (can't share with JDBC), callback complexity

---

## 4. References

### Spark Connect & UDFs
- [Spark Connect App Development](https://spark.apache.org/docs/4.0.0/app-dev-spark-connect.html)
- [Arrow-optimized Python UDFs (Spark 3.5+)](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35)
- [CloudPickle](https://github.com/cloudpipe/cloudpickle)

### DuckDB
- [DuckDB C API - Complete Reference](https://duckdb.org/docs/stable/clients/c/api)
- [DuckDB Python UDF Blog Post](https://duckdb.org/2023/07/07/python-udf)
- [DuckDB CREATE MACRO](https://duckdb.org/docs/stable/sql/statements/create_macro)
- [Airport Extension](https://airport.query.farm/) - Arrow Flight for remote functions

### Java FFM (Panama)
- [JEP 454: Foreign Function & Memory API](https://openjdk.org/jeps/454)
- [Oracle FFM Guide](https://docs.oracle.com/en/java/javase/22/core/foreign-function-and-memory-api.html)
- [jextract Tool](https://jdk.java.net/jextract/)
- [sqlite4clj](https://github.com/andersmurphy/sqlite4clj) - SQLite FFM bindings example

### Python Runtimes
- [GraalPy](https://github.com/oracle/graalpython) - Python 3.12 on JVM
- [GraalVM Polyglot Programming](https://www.graalvm.org/latest/reference-manual/polyglot-programming/)
- [Jep (Java Embedded Python)](https://github.com/ninia/jep)
