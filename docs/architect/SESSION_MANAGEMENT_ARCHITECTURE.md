# Session Management Architecture

**Last Updated:** 2025-12-16
**Status:** Production

## Overview

This document describes the session management architecture for Thunderduck's Spark Connect server. Each client session receives an isolated DuckDB runtime with its own in-memory database.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SESSION ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ Session (sessionId = "abc-123-def")                            │    │
│  │  ├── DuckDBRuntime (jdbc:duckdb::memory:abc_123_def)           │    │
│  │  │    └── DuckDBConnection (owned by runtime)                  │    │
│  │  ├── config: Map<String, String>                               │    │
│  │  ├── tempViews: Map<String, LogicalPlan>                       │    │
│  │  └── createdAt: long                                           │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ Session (sessionId = "xyz-789-uvw")                            │    │
│  │  ├── DuckDBRuntime (jdbc:duckdb::memory:xyz_789_uvw)           │    │
│  │  │    └── DuckDBConnection (owned by runtime)                  │    │
│  │  ├── config: Map<String, String>                               │    │
│  │  ├── tempViews: Map<String, LogicalPlan>                       │    │
│  │  └── createdAt: long                                           │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### Session (`Session.java`)

Each session maintains:
- **Unique session ID** - UUID from the PySpark client
- **DuckDB runtime** - Owns the database connection (session-scoped isolation)
- **Configuration** - Session-scoped Spark configuration
- **Temporary views** - Registered temp views for this session
- **Creation timestamp** - For tracking session age

```java
public class Session implements AutoCloseable {
    private final String sessionId;
    private final DuckDBRuntime runtime;
    private final Map<String, String> config;
    private final Map<String, LogicalPlan> tempViews;
    private final long createdAt;
}
```

**Key design decisions:**
1. **Named in-memory databases**: Each session creates a DuckDB database with name derived from session ID
   - Session ID: `40c928db-7bce-4836-83ad-a375704ec597`
   - JDBC URL: `jdbc:duckdb::memory:40c928db_7bce_4836_83ad_a375704ec597`

2. **Session owns runtime**: The Session class owns its DuckDBRuntime and closes it when the session ends

3. **AutoCloseable**: Proper resource cleanup via try-with-resources or explicit close()

### SessionManager (`SessionManager.java`)

Manages session lifecycle and execution slots:

```java
public class SessionManager {
    private final AtomicReference<ExecutionState> executionState;
    private final BlockingQueue<WaitingClient> waitQueue;
    private final ConcurrentHashMap<String, Session> sessionCache;
    private final ScheduledExecutorService watchdog;
}
```

**Design principles:**
- **Single active execution** - Only one query runs at a time (DuckDB limitation)
- **Idle session replacement** - New sessions replace idle sessions seamlessly
- **Wait queue** - Clients wait if execution in progress (FIFO, max 10)
- **Session caching** - Same session ID reuses same DuckDB runtime
- **Lock-free CAS** - Atomic operations for thread-safe state transitions

### ExecutionState (`ExecutionState.java`)

Immutable snapshot of current execution state:

```java
public class ExecutionState {
    final String sessionId;
    final Session session;
    final Thread executionThread;
    final long startTime;

    static final ExecutionState IDLE = new ExecutionState(null, null, null, 0);
}
```

### WaitingClient (`WaitingClient.java`)

Represents a client waiting in the queue:

```java
public class WaitingClient {
    final String sessionId;
    final CountDownLatch latch;
    final Context grpcContext;
}
```

## Request Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        REQUEST FLOW                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  New Request (sessionId=X)                                              │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────────┐                                                    │
│  │ Active Execution│                                                    │
│  │    Exists?      │                                                    │
│  └────────┬────────┘                                                    │
│           │                                                             │
│     ┌─────┴─────┐                                                       │
│     │           │                                                       │
│    NO          YES                                                      │
│     │           │                                                       │
│     ▼           ▼                                                       │
│  ┌──────────┐  ┌─────────────────┐                                     │
│  │ CAS:     │  │ Queue.size < 10?│                                     │
│  │ Acquire  │  └────────┬────────┘                                     │
│  │ slot,    │           │                                              │
│  │ start    │     ┌─────┴─────┐                                        │
│  │ execution│    YES          NO                                       │
│  └──────────┘     │           │                                        │
│                   ▼           ▼                                        │
│            ┌───────────┐  ┌────────────────────┐                       │
│            │ Add to    │  │ RESOURCE_EXHAUSTED │                       │
│            │ queue,    │  │ "Queue full (10)"  │                       │
│            │ wait      │  └────────────────────┘                       │
│            └───────────┘                                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Execution Lifecycle

```
startExecution(sessionId)
       │
       ▼
┌─────────────────────────────────────┐
│ CAS: Set ExecutionState             │
│  - sessionId                        │
│  - session (new or cached)          │
│  - executionThread = Thread.current │
│  - startTime = System.currentTimeMs │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│        QUERY EXECUTION              │
│  (protected by try/finally)         │
└─────────────────┬───────────────────┘
                  │
       ┌──────────┼──────────┐
       │          │          │
       ▼          ▼          ▼
   [Success]  [Error]   [Timeout]
       │          │          │
       └──────────┴──────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│ completeExecution() [finally block] │
│  - CAS: Clear ExecutionState        │
│  - Signal next waiter via latch     │
└─────────────────────────────────────┘
```

## Configuration

| Parameter | Default | System Property | Description |
|-----------|---------|-----------------|-------------|
| `MAX_QUEUE_SIZE` | 10 | `thunderduck.maxQueueSize` | Max clients waiting in queue |
| `MAX_EXECUTION_TIME_MS` | 1,800,000 (30 min) | `thunderduck.maxExecutionTimeMs` | Query timeout |

## Error Handling

| Scenario | gRPC Status | Description |
|----------|-------------|-------------|
| Queue full | `RESOURCE_EXHAUSTED` | "Server busy, queue full (10 waiters)" |
| Server shutdown | `UNAVAILABLE` | "Server is shutting down" |
| Execution timeout | `DEADLINE_EXCEEDED` | "Query execution timeout" |
| Client cancelled | `CANCELLED` | "Client disconnected" |

## Client Disconnect Handling

### Disconnect while waiting in queue
1. gRPC Context.CancellationListener fires
2. Remove WaitingClient from queue
3. Free queue slot for other clients

### Disconnect while executing query
1. gRPC Context.CancellationListener fires
2. Interrupt execution thread
3. completeExecution() runs in finally block
4. Signal next waiter in queue

## Timeout Enforcement

A background watchdog thread monitors execution time:

```java
watchdog.scheduleAtFixedRate(() -> {
    ExecutionState state = executionState.get();
    if (state != null && !state.isIdle()) {
        long elapsed = System.currentTimeMillis() - state.startTime;
        if (elapsed > maxExecutionTimeMs) {
            logger.warn("Execution timeout after {}ms", elapsed);
            cancelCurrentExecution();
        }
    }
}, 10, 10, TimeUnit.SECONDS);
```

## Usage Example

```java
// In SparkConnectServiceImpl
@Override
public void executePlan(ExecutePlanRequest request,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
    String sessionId = request.getSessionId();
    Context grpcContext = Context.current();
    Session session = null;

    try {
        // Acquire slot (may block if queue)
        session = sessionManager.startExecution(sessionId, grpcContext);

        // Get runtime and execute query
        DuckDBRuntime runtime = session.getRuntime();
        QueryExecutor executor = new QueryExecutor(runtime);
        VectorSchemaRoot result = executor.executeQuery(sql);

        // Stream results...

    } catch (StatusRuntimeException e) {
        responseObserver.onError(e);
    } finally {
        if (session != null) {
            sessionManager.completeExecution(sessionId);
        }
    }
}
```

## Files

| Class | Location | Responsibility |
|-------|----------|----------------|
| `Session` | `connect-server/.../session/Session.java` | Owns DuckDBRuntime, manages temp views and config |
| `SessionManager` | `connect-server/.../session/SessionManager.java` | Manages execution slots, session cache |
| `ExecutionState` | `connect-server/.../session/ExecutionState.java` | Immutable execution state snapshot |
| `WaitingClient` | `connect-server/.../session/WaitingClient.java` | Client waiting in queue |
| `SparkDefaults` | `connect-server/.../session/SparkDefaults.java` | Default Spark configuration values |
| `DuckDBRuntime` | `core/.../runtime/DuckDBRuntime.java` | DuckDB connection factory |

## Benefits

- **Session isolation**: Each session has its own database namespace
- **Resource management**: Proper cleanup when sessions end
- **Thread safety**: Lock-free CAS operations
- **Fairness**: FIFO queue for waiting clients
- **Timeout protection**: Watchdog prevents runaway queries
- **Testability**: Constructor injection for mock runtimes

---

**See Also:**
- [Arrow Streaming Architecture](ARROW_STREAMING_ARCHITECTURE.md)
- [Differential Testing Architecture](DIFFERENTIAL_TESTING_ARCHITECTURE.md)
