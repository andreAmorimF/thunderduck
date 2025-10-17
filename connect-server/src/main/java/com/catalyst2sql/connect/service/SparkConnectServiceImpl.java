package com.catalyst2sql.connect.service;

import com.catalyst2sql.connect.session.Session;
import com.catalyst2sql.connect.session.SessionManager;
import com.catalyst2sql.runtime.DuckDBConnectionManager;
import com.catalyst2sql.runtime.QueryExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.spark.connect.proto.*;
import org.apache.spark.connect.proto.Relation;
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

            // For MVP, handle SQL queries (both direct and command-wrapped)
            String sql = null;

            if (plan.hasRoot()) {
                Relation root = plan.getRoot();
                logger.debug("Root relation type: {}", root.getRelTypeCase());

                // Check for ShowString wrapping a SQL relation
                if (root.hasShowString() && root.getShowString().hasInput()) {
                    Relation input = root.getShowString().getInput();
                    if (input.hasSql()) {
                        sql = input.getSql().getQuery();
                        logger.debug("Found SQL in ShowString.input: {}", sql);
                    }
                } else if (root.hasSql()) {
                    // Direct SQL relation
                    sql = root.getSql().getQuery();
                    logger.debug("Found direct SQL relation: {}", sql);
                }
            } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                // SQL command (e.g., spark.sql(...))
                sql = plan.getCommand().getSqlCommand().getSql();
                logger.debug("Found SQL command: {}", sql);
            }

            if (sql != null) {
                logger.info("Executing SQL: {}", sql);
                executeSQL(sql, sessionId, responseObserver);
            } else {
                // Log more details for debugging
                String details = String.format("Plan type: %s", plan.getOpTypeCase());
                if (plan.hasRoot()) {
                    details += String.format(", Root type: %s", plan.getRoot().getRelTypeCase());
                }

                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("Only SQL queries are supported in MVP. " + details)
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
     * For MVP: Handle GetWithDefault operations by returning the default values.
     * This is required for PySpark client compatibility.
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

            ConfigResponse.Builder responseBuilder = ConfigResponse.newBuilder()
                .setSessionId(sessionId);

            // Handle different operation types
            if (request.hasOperation()) {
                ConfigRequest.Operation op = request.getOperation();

                switch (op.getOpTypeCase()) {
                    case GET_WITH_DEFAULT:
                        // Return the default values provided by the client
                        ConfigRequest.GetWithDefault getWithDefault = op.getGetWithDefault();
                        for (KeyValue kv : getWithDefault.getPairsList()) {
                            responseBuilder.addPairs(KeyValue.newBuilder()
                                .setKey(kv.getKey())
                                .setValue(kv.getValue())
                                .build());
                        }
                        logger.debug("Returning default configs: {} pairs", getWithDefault.getPairsCount());
                        break;

                    case GET:
                        // For MVP, return empty values for GET requests
                        ConfigRequest.Get get = op.getGet();
                        logger.debug("Config GET requested for {} keys (returning empty)", get.getKeysCount());
                        break;

                    default:
                        logger.debug("Config operation type {} not implemented", op.getOpTypeCase());
                        break;
                }
            }

            responseObserver.onNext(responseBuilder.build());
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
     *
     * For MVP: Since we execute queries synchronously and don't maintain execution state,
     * we return NOT_FOUND to indicate the execution has already completed.
     * This allows clients with reattachable execution enabled to work properly.
     */
    @Override
    public void reattachExecute(ReattachExecuteRequest request,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("reattachExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // For MVP, we don't maintain execution state. Queries complete immediately.
        // Return NOT_FOUND to indicate the execution is already complete.
        responseObserver.onError(Status.NOT_FOUND
            .withDescription("Operation " + operationId + " not found. " +
                "Queries complete immediately in MVP and cannot be reattached.")
            .asRuntimeException());
    }

    /**
     * Release reattachable execution.
     *
     * For MVP: Since we don't maintain execution state (queries complete immediately),
     * this is a no-op that returns success to satisfy the client cleanup flow.
     */
    @Override
    public void releaseExecute(ReleaseExecuteRequest request,
                              StreamObserver<ReleaseExecuteResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("releaseExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // Return success response (no-op, we don't maintain execution state)
        ReleaseExecuteResponse response = ReleaseExecuteResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.debug("Released operation: {} (no-op in MVP)", operationId);
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
            ExecutePlanResponse.Builder arrowResponseBuilder = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(java.util.UUID.randomUUID().toString())
                .setArrowBatch(org.apache.spark.connect.proto.ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(root.getRowCount())
                    .setData(com.google.protobuf.ByteString.copyFrom(arrowData))
                    .build());

            // Add schema information for client compatibility
            // Note: Schema is embedded in Arrow IPC format, but some clients expect it separately
            // For now, we'll send without explicit schema field as it's optional

            ExecutePlanResponse arrowResponse = arrowResponseBuilder.build();

            responseObserver.onNext(arrowResponse);

            // Send ResultComplete to indicate execution is finished (required for reattachable execution)
            ExecutePlanResponse completeResponse = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(java.util.UUID.randomUUID().toString())
                .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
                .build();

            responseObserver.onNext(completeResponse);
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
