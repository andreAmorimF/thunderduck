package com.catalyst2sql.connect.service;

import com.catalyst2sql.connect.session.Session;
import com.catalyst2sql.connect.session.SessionManager;
import com.catalyst2sql.runtime.DuckDBConnectionManager;
import com.catalyst2sql.runtime.QueryExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.spark.connect.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Spark Connect gRPC service.
 *
 * This service bridges Spark Connect protocol to DuckDB via catalyst2sql.
 * Key features:
 * - Single-session management (DuckDB is single-user)
 * - SQL query execution via QueryExecutor
 * - Minimal viable implementation for MVP
 */
public class SparkConnectServiceImpl extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SparkConnectServiceImpl.class);

    private final SessionManager sessionManager;
    private final DuckDBConnectionManager connectionManager;

    /**
     * Create Spark Connect service with session manager and DuckDB connection manager.
     *
     * @param sessionManager Manager for single-session lifecycle
     * @param connectionManager DuckDB connection manager (pool size 1)
     */
    public SparkConnectServiceImpl(SessionManager sessionManager,
                                  DuckDBConnectionManager connectionManager) {
        this.sessionManager = sessionManager;
        this.connectionManager = connectionManager;
        logger.info("SparkConnectServiceImpl initialized");
    }

    /**
     * Execute a Spark plan and stream results back to client.
     *
     * This is the main RPC for query execution. For MVP:
     * 1. Extract session ID from request
     * 2. Validate/create session
     * 3. Deserialize plan to SQL
     * 4. Execute via QueryExecutor
     * 5. Stream results as Arrow batches
     *
     * @param request ExecutePlanRequest containing the plan
     * @param responseObserver Stream observer for responses
     */
    @Override
    public void executePlan(ExecutePlanRequest request,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("executePlan called for session: {}", sessionId);

        try {
            // Update activity or create session
            updateOrCreateSession(sessionId);

            // Extract the plan
            if (!request.hasPlan()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must contain a plan")
                    .asRuntimeException());
                return;
            }

            Plan plan = request.getPlan();
            logger.debug("Plan type: {}", plan.getOpTypeCase());

            // For MVP, handle simple SQL queries
            if (plan.hasRoot() && plan.getRoot().hasSql()) {
                String sql = plan.getRoot().getSql().getQuery();
                logger.info("Executing SQL: {}", sql);

                // Execute query using QueryExecutor
                executeSQL(sql, sessionId, responseObserver);
            } else {
                // For MVP, only support direct SQL
                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("Only SQL queries are supported in MVP. " +
                        "Plan type: " + plan.getOpTypeCase())
                    .asRuntimeException());
            }

        } catch (Exception e) {
            logger.error("Error executing plan for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Execution failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Analyze a plan and return metadata (schema, execution plan, etc.).
     *
     * For MVP, return minimal response.
     *
     * @param request AnalyzePlanRequest
     * @param responseObserver Response observer
     */
    @Override
    public void analyzePlan(AnalyzePlanRequest request,
                           StreamObserver<AnalyzePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("analyzePlan called for session: {}", sessionId);

        try {
            updateOrCreateSession(sessionId);

            // For MVP, return minimal analyze response
            AnalyzePlanResponse response = AnalyzePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error analyzing plan for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Analysis failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Get or set configuration values.
     *
     * @param request ConfigRequest
     * @param responseObserver Response observer
     */
    @Override
    public void config(ConfigRequest request,
                      StreamObserver<ConfigResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("config called for session: {}", sessionId);

        try {
            updateOrCreateSession(sessionId);

            // For MVP, return empty config response
            ConfigResponse response = ConfigResponse.newBuilder()
                .setSessionId(sessionId)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling config for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Config operation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Add artifacts (JARs, files) to session.
     * NOT IMPLEMENTED in MVP.
     */
    @Override
    public StreamObserver<AddArtifactsRequest> addArtifacts(
            StreamObserver<AddArtifactsResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact management not implemented in MVP")
            .asRuntimeException());
        return null;
    }

    /**
     * Check artifact status.
     * NOT IMPLEMENTED in MVP.
     */
    @Override
    public void artifactStatus(ArtifactStatusesRequest request,
                              StreamObserver<ArtifactStatusesResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact status not implemented in MVP")
            .asRuntimeException());
    }

    /**
     * Interrupt running execution.
     * NOT IMPLEMENTED in MVP (single-threaded execution for now).
     */
    @Override
    public void interrupt(InterruptRequest request,
                         StreamObserver<InterruptResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Query interruption not implemented in MVP")
            .asRuntimeException());
    }

    /**
     * Reattach to existing execution.
     * NOT IMPLEMENTED in MVP (no reattachable execution support).
     */
    @Override
    public void reattachExecute(ReattachExecuteRequest request,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Reattachable execution not implemented in MVP")
            .asRuntimeException());
    }

    /**
     * Release reattachable execution.
     * NOT IMPLEMENTED in MVP.
     */
    @Override
    public void releaseExecute(ReleaseExecuteRequest request,
                              StreamObserver<ReleaseExecuteResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Release execution not implemented in MVP")
            .asRuntimeException());
    }

    // ========== Helper Methods ==========

    /**
     * Update activity for existing session or create new session.
     *
     * @param sessionId Session ID from request
     * @throws Exception if session creation fails (server busy)
     */
    private void updateOrCreateSession(String sessionId) throws Exception {
        SessionManager.SessionInfo info = sessionManager.getSessionInfo();

        if (info.hasActiveSession) {
            // Update activity for existing session
            sessionManager.updateActivity(sessionId);
        } else {
            // Create new session (will throw if server is busy)
            Session session = sessionManager.createSession(sessionId);
            logger.info("New session created: {}", session);
        }
    }

    /**
     * Execute SQL query and stream results as Arrow batches.
     *
     * @param sql SQL query string
     * @param sessionId Session ID
     * @param responseObserver Response stream
     */
    private void executeSQL(String sql, String sessionId,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            logger.info("[{}] Executing SQL for session {}: {}", operationId, sessionId, sql);

            // Create QueryExecutor with connection manager
            QueryExecutor executor = new QueryExecutor(connectionManager);

            // Execute query and get Arrow results
            org.apache.arrow.vector.VectorSchemaRoot results = executor.executeQuery(sql);

            // Stream Arrow results
            streamArrowResults(results, sessionId, operationId, responseObserver);

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[{}] Query completed in {}ms, {} rows",
                operationId, durationMs, results.getRowCount());

            // Clean up Arrow resources
            results.close();

        } catch (Exception e) {
            logger.error("[{}] SQL execution failed", operationId, e);

            // Determine appropriate gRPC status code based on exception type
            Status status;
            if (e instanceof java.sql.SQLException) {
                status = Status.INVALID_ARGUMENT.withDescription("SQL error: " + e.getMessage());
            } else if (e instanceof IllegalArgumentException) {
                status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            } else {
                status = Status.INTERNAL.withDescription("Execution failed: " + e.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }
    }

    /**
     * Stream Arrow results to client.
     *
     * For MVP, we stream all results in a single batch.
     * Multi-batch streaming will be added in Week 12.
     *
     * @param root Arrow VectorSchemaRoot with results
     * @param sessionId Session ID
     * @param operationId Operation ID for logging
     * @param responseObserver Response stream
     */
    private void streamArrowResults(org.apache.arrow.vector.VectorSchemaRoot root,
                                    String sessionId,
                                    String operationId,
                                    StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            // Serialize Arrow data to bytes using Arrow IPC format
            java.io.ByteArrayOutputStream dataOut = new java.io.ByteArrayOutputStream();
            org.apache.arrow.vector.ipc.ArrowStreamWriter writer =
                new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                    root,
                    null,  // DictionaryProvider
                    java.nio.channels.Channels.newChannel(dataOut)
                );

            writer.writeBatch();
            writer.end();
            writer.close();

            byte[] arrowData = dataOut.toByteArray();

            // Build gRPC response with Arrow data
            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setArrowBatch(org.apache.spark.connect.proto.ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(root.getRowCount())
                    .setData(com.google.protobuf.ByteString.copyFrom(arrowData))
                    .build())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.debug("[{}] Streamed {} rows ({} bytes)",
                operationId, root.getRowCount(), arrowData.length);

        } catch (java.io.IOException e) {
            logger.error("[{}] Arrow serialization failed", operationId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Result serialization failed: " + e.getMessage())
                .asRuntimeException());
        }
    }
}
