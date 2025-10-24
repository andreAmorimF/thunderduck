# Thunderduck Spark Connect Server

A minimal Spark Connect-compatible server backed by DuckDB for 5-10x performance improvement over Spark local mode.

## Status

**Week 11 MVP**: ✅ Functional with Arrow streaming support

**Current Features**:
- ✅ gRPC server accepting Spark Connect protocol requests
- ✅ Single-session state management with automatic timeout
- ✅ SQL query execution via DuckDB
- ✅ Arrow-formatted result streaming
- ✅ Graceful shutdown with resource cleanup

**Limitations** (MVP):
- **Single session**: Only one client can connect at a time
- **SQL only**: Only direct SQL queries supported (no DataFrame API plan translation yet)
- **No UDFs**: User-defined functions not supported
- **No artifacts**: JAR/file upload not supported

## Quick Start

### Prerequisites

- Java 11 or later
- Maven 3.9+
- Build thunderduck core module first

### Build

```bash
# From project root
mvn clean install -pl connect-server

# Or build all modules
mvn clean install
```

### Run Server

```bash
# Default configuration (port 15002, in-memory, 5-minute timeout)
java -cp connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar \
  com.thunderduck.connect.server.SparkConnectServer

# Custom port
java -cp connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar \
  com.thunderduck.connect.server.SparkConnectServer 50051

# Persistent database
java -cp connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar \
  com.thunderduck.connect.server.SparkConnectServer 15002 300000 /data/my.duckdb
```

### Connect with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("TestClient") \
    .getOrCreate()

# Execute query
df = spark.sql("SELECT 1 AS col")
df.show()

# Load Parquet data
df = spark.sql("SELECT * FROM read_parquet('data.parquet') WHERE age > 25")
df.show()
```

## Configuration

Edit `src/main/resources/connect-server.properties`:

```properties
# Server port
server.port=15002

# Session timeout (seconds)
server.session.timeout.seconds=300

# DuckDB connection string
duckdb.connection.string=:memory:
```

## Architecture

```
┌─────────────────────────────────────────┐
│    PySpark/Spark Client Application    │
└──────────────┬──────────────────────────┘
               │ gRPC (Spark Connect Protocol)
┌──────────────▼──────────────────────────┐
│      SparkConnectServer (port 15002)    │
│  ┌──────────────────────────────────┐   │
│  │   SessionManager (Single-Session)│   │
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │   SparkConnectServiceImpl        │   │
│  │   - executePlan (SQL + Arrow)    │   │
│  │   - analyzePlan                  │   │
│  │   - config                       │   │
│  └──────────────────────────────────┘   │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│        QueryExecutor (from core)        │
│    DuckDB Singleton Connection          │
└─────────────────────────────────────────┘
```

## Troubleshooting

### Server Busy Error

**Error**: `RESOURCE_EXHAUSTED: Server busy: another session 'xyz' is active`

**Solution**:
- Wait for session timeout (default: 5 minutes)
- Or restart the server to clear the session

### Connection Refused

**Solution**:
- Verify server is running: `netstat -an | grep 15002`
- Check firewall rules
- Verify port in PySpark connection string matches server port

### SQL Execution Errors

**Solution**:
- Test SQL directly in DuckDB CLI first
- Check DuckDB documentation for supported features
- Enable DEBUG logging: edit `logback.xml`, set level to DEBUG

## Development

### Running Tests

```bash
# Unit tests
mvn test -pl connect-server

# Integration tests
mvn verify -pl connect-server
```

### Logging

**Default level**: INFO

**Enable DEBUG logging**: Edit `src/main/resources/logback.xml`

```xml
<logger name="com.thunderduck.connect" level="DEBUG" />
```

## Upcoming Features

See `IMPLEMENTATION_PLAN.md` for roadmap:
- **Week 12**: TPC-H Q1 integration, plan deserialization
- **Week 13**: Joins, window functions, subqueries
- **Week 14**: Production hardening, monitoring
- **Week 15**: Query plan caching, performance optimization

## License

Apache License 2.0 - see root LICENSE file
