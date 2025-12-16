package com.thunderduck.connect.service;

import com.thunderduck.connect.session.Session;
import com.thunderduck.runtime.QueryExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.connect.proto.CacheTable;
import org.apache.spark.connect.proto.Catalog;
import org.apache.spark.connect.proto.ClearCache;
import org.apache.spark.connect.proto.CreateTable;
import org.apache.spark.connect.proto.CurrentCatalog;
import org.apache.spark.connect.proto.CurrentDatabase;
import org.apache.spark.connect.proto.DataType;
import org.apache.spark.connect.proto.DatabaseExists;
import org.apache.spark.connect.proto.DropGlobalTempView;
import org.apache.spark.connect.proto.DropTempView;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.IsCached;
import org.apache.spark.connect.proto.ListCatalogs;
import org.apache.spark.connect.proto.ListColumns;
import org.apache.spark.connect.proto.ListDatabases;
import org.apache.spark.connect.proto.ListTables;
import org.apache.spark.connect.proto.RecoverPartitions;
import org.apache.spark.connect.proto.RefreshByPath;
import org.apache.spark.connect.proto.RefreshTable;
import org.apache.spark.connect.proto.SetCurrentCatalog;
import org.apache.spark.connect.proto.SetCurrentDatabase;
import org.apache.spark.connect.proto.TableExists;
import org.apache.spark.connect.proto.UncacheTable;

import com.thunderduck.connect.converter.SparkDataTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.thunderduck.generator.SQLQuoting.quoteIdentifier;

/**
 * Handles Spark Connect Catalog operations by delegating to DuckDB.
 *
 * <p>This handler maps Spark Catalog operations to DuckDB's information_schema
 * and system tables. See docs/architect/CATALOG_OPERATIONS.md for details.
 *
 * <p>Implemented operations:
 * <ul>
 *   <li>DROP_TEMP_VIEW - Drop a temporary view</li>
 *   <li>DROP_GLOBAL_TEMP_VIEW - Drop a global temporary view (same as DROP_TEMP_VIEW)</li>
 *   <li>TABLE_EXISTS - Check if table/view exists</li>
 *   <li>LIST_TABLES - List all tables/views</li>
 *   <li>LIST_COLUMNS - List columns for a table</li>
 *   <li>LIST_DATABASES - List all schemas</li>
 *   <li>DATABASE_EXISTS - Check if schema exists</li>
 *   <li>CURRENT_DATABASE - Get current schema</li>
 *   <li>SET_CURRENT_DATABASE - Set current schema</li>
 *   <li>IS_CACHED - Check if table is cached (always false)</li>
 *   <li>CACHE_TABLE - Cache table (no-op)</li>
 *   <li>UNCACHE_TABLE - Uncache table (no-op)</li>
 *   <li>CLEAR_CACHE - Clear cache (no-op)</li>
 *   <li>REFRESH_TABLE - Refresh table metadata (no-op)</li>
 *   <li>REFRESH_BY_PATH - Refresh by path (no-op)</li>
 *   <li>RECOVER_PARTITIONS - Recover partitions (no-op)</li>
 *   <li>CURRENT_CATALOG - Get current catalog (always "default")</li>
 *   <li>SET_CURRENT_CATALOG - Set current catalog (only "default" supported)</li>
 *   <li>LIST_CATALOGS - List catalogs (returns ["default"])</li>
 *   <li>CREATE_TABLE - Create a persistent table (internal tables only)</li>
 * </ul>
 */
public class CatalogOperationHandler {

    private static final Logger logger = LoggerFactory.getLogger(CatalogOperationHandler.class);

    // Schema filter to exclude system schemas
    private static final String SCHEMA_FILTER =
        "table_schema NOT IN ('information_schema', 'pg_catalog')";

    /**
     * Execute a catalog operation.
     *
     * @param catalog the catalog operation to execute
     * @param session the session
     * @param responseObserver the gRPC response observer
     */
    public void execute(Catalog catalog, Session session,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            logger.debug("Handling CATALOG operation: {}", catalog.getCatTypeCase());

            switch (catalog.getCatTypeCase()) {
                case DROP_TEMP_VIEW:
                    handleDropTempView(catalog.getDropTempView(), session, responseObserver);
                    break;

                case TABLE_EXISTS:
                    handleTableExists(catalog.getTableExists(), session, responseObserver);
                    break;

                case LIST_TABLES:
                    handleListTables(catalog.getListTables(), session, responseObserver);
                    break;

                case LIST_COLUMNS:
                    handleListColumns(catalog.getListColumns(), session, responseObserver);
                    break;

                case LIST_DATABASES:
                    handleListDatabases(catalog.getListDatabases(), session, responseObserver);
                    break;

                case DATABASE_EXISTS:
                    handleDatabaseExists(catalog.getDatabaseExists(), session, responseObserver);
                    break;

                case CURRENT_DATABASE:
                    handleCurrentDatabase(session, responseObserver);
                    break;

                case SET_CURRENT_DATABASE:
                    handleSetCurrentDatabase(catalog.getSetCurrentDatabase(), session, responseObserver);
                    break;

                case IS_CACHED:
                    handleIsCached(catalog.getIsCached(), session, responseObserver);
                    break;

                case CACHE_TABLE:
                    handleCacheTable(catalog.getCacheTable(), session, responseObserver);
                    break;

                case UNCACHE_TABLE:
                    handleUncacheTable(catalog.getUncacheTable(), session, responseObserver);
                    break;

                case CLEAR_CACHE:
                    handleClearCache(session, responseObserver);
                    break;

                case REFRESH_TABLE:
                    handleRefreshTable(catalog.getRefreshTable(), session, responseObserver);
                    break;

                case REFRESH_BY_PATH:
                    handleRefreshByPath(catalog.getRefreshByPath(), session, responseObserver);
                    break;

                case RECOVER_PARTITIONS:
                    handleRecoverPartitions(catalog.getRecoverPartitions(), session, responseObserver);
                    break;

                case CURRENT_CATALOG:
                    handleCurrentCatalog(session, responseObserver);
                    break;

                case SET_CURRENT_CATALOG:
                    handleSetCurrentCatalog(catalog.getSetCurrentCatalog(), session, responseObserver);
                    break;

                case LIST_CATALOGS:
                    handleListCatalogs(catalog.getListCatalogs(), session, responseObserver);
                    break;

                case DROP_GLOBAL_TEMP_VIEW:
                    handleDropGlobalTempView(catalog.getDropGlobalTempView(), session, responseObserver);
                    break;

                case CREATE_TABLE:
                    handleCreateTable(catalog.getCreateTable(), session, responseObserver);
                    break;

                default:
                    responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("Unsupported catalog operation: " + catalog.getCatTypeCase())
                        .asRuntimeException());
            }

        } catch (Exception e) {
            logger.error("Catalog operation failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Catalog operation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Handle DROP_TEMP_VIEW operation.
     */
    private void handleDropTempView(DropTempView dropTempView, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String viewName = dropTempView.getViewName();
        logger.info("Dropping temp view: '{}'", viewName);

        // Remove from session's temp view registry
        boolean existed = session.dropTempView(viewName);

        // Drop the view from DuckDB
        String dropViewSQL = String.format("DROP VIEW IF EXISTS %s", quoteIdentifier(viewName));
        logger.debug("Executing DuckDB: {}", dropViewSQL);

        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        executor.execute(dropViewSQL);

        // Return boolean result
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamBooleanResult(existed);

        logger.info("✓ View dropped from DuckDB: '{}' (existed={}, session: {})",
            viewName, existed, session.getSessionId());
    }

    /**
     * Handle TABLE_EXISTS operation.
     */
    private void handleTableExists(TableExists tableExists, Session session,
                                   StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = tableExists.getTableName();
        String dbName = tableExists.hasDbName() ? tableExists.getDbName() : null;

        logger.info("Checking table exists: '{}' (db={})", tableName, dbName);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '");
        sql.append(escapeSql(tableName)).append("'");
        sql.append(" AND ").append(SCHEMA_FILTER);
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        sql.append(") AS result");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql.toString())) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                // DuckDB returns boolean as BitVector
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            // Return boolean result
            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamBooleanResult(exists);

            logger.info("✓ tableExists('{}') = {}", tableName, exists);
        }
    }

    /**
     * Handle LIST_TABLES operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - catalog: string (always "default")
     * - namespace: array<string> (schema path)
     * - description: string (empty)
     * - tableType: string (MANAGED, VIEW, EXTERNAL)
     * - isTemporary: boolean
     */
    private void handleListTables(ListTables listTables, Session session,
                                  StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = listTables.hasDbName() ? listTables.getDbName() : null;
        String pattern = listTables.hasPattern() ? listTables.getPattern() : null;

        logger.info("Listing tables (db={}, pattern={})", dbName, pattern);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE ");
        sql.append(SCHEMA_FILTER);
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        if (pattern != null) {
            sql.append(" AND table_name LIKE '").append(escapeSql(pattern)).append("'");
        }
        sql.append(" ORDER BY table_schema, table_name");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListTablesSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector catalogVec = (VarCharVector) result.getVector("catalog");
                VarCharVector namespaceVec = (VarCharVector) result.getVector("namespace");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector typeVec = (VarCharVector) result.getVector("tableType");
                BitVector tempVec = (BitVector) result.getVector("isTemporary");

                nameVec.allocateNew(rowCount);
                catalogVec.allocateNew(rowCount);
                namespaceVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                typeVec.allocateNew(rowCount);
                tempVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("table_name");
                VarCharVector srcSchema = (VarCharVector) queryResult.getVector("table_schema");
                VarCharVector srcType = (VarCharVector) queryResult.getVector("table_type");

                for (int i = 0; i < rowCount; i++) {
                    String tableName = new String(srcName.get(i), StandardCharsets.UTF_8);
                    String schemaName = new String(srcSchema.get(i), StandardCharsets.UTF_8);
                    String tableType = new String(srcType.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, tableName.getBytes(StandardCharsets.UTF_8));
                    catalogVec.set(i, "default".getBytes(StandardCharsets.UTF_8));
                    namespaceVec.set(i, ("[\"" + schemaName + "\"]").getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));

                    // Map table type
                    String sparkType = mapTableType(tableType);
                    typeVec.set(i, sparkType.getBytes(StandardCharsets.UTF_8));

                    // Check if temporary (in session registry)
                    boolean isTemp = session.getTempView(tableName).isPresent();
                    tempVec.set(i, isTemp ? 1 : 0);
                }

                nameVec.setValueCount(rowCount);
                catalogVec.setValueCount(rowCount);
                namespaceVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                typeVec.setValueCount(rowCount);
                tempVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listTables returned {} tables", rowCount);
            }
        }
    }

    /**
     * Handle LIST_COLUMNS operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - description: string (empty)
     * - dataType: string
     * - nullable: boolean
     * - isPartition: boolean (always false)
     * - isBucket: boolean (always false)
     */
    private void handleListColumns(ListColumns listColumns, Session session,
                                   StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = listColumns.getTableName();
        String dbName = listColumns.hasDbName() ? listColumns.getDbName() : null;

        logger.info("Listing columns for table: '{}' (db={})", tableName, dbName);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT column_name, data_type, is_nullable FROM information_schema.columns ");
        sql.append("WHERE table_name = '").append(escapeSql(tableName)).append("'");
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        sql.append(" ORDER BY ordinal_position");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            if (queryResult.getRowCount() == 0) {
                // Table not found
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Table not found: " + tableName)
                    .asRuntimeException());
                return;
            }

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListColumnsSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector dataTypeVec = (VarCharVector) result.getVector("dataType");
                BitVector nullableVec = (BitVector) result.getVector("nullable");
                BitVector partitionVec = (BitVector) result.getVector("isPartition");
                BitVector bucketVec = (BitVector) result.getVector("isBucket");
                BitVector clusterVec = (BitVector) result.getVector("isCluster");

                nameVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                dataTypeVec.allocateNew(rowCount);
                nullableVec.allocateNew(rowCount);
                partitionVec.allocateNew(rowCount);
                bucketVec.allocateNew(rowCount);
                clusterVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("column_name");
                VarCharVector srcType = (VarCharVector) queryResult.getVector("data_type");
                VarCharVector srcNullable = (VarCharVector) queryResult.getVector("is_nullable");

                for (int i = 0; i < rowCount; i++) {
                    String colName = new String(srcName.get(i), StandardCharsets.UTF_8);
                    String dataType = new String(srcType.get(i), StandardCharsets.UTF_8);
                    String nullable = new String(srcNullable.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, colName.getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                    dataTypeVec.set(i, dataType.getBytes(StandardCharsets.UTF_8));
                    nullableVec.set(i, "YES".equals(nullable) ? 1 : 0);
                    partitionVec.set(i, 0);  // Always false
                    bucketVec.set(i, 0);     // Always false
                    clusterVec.set(i, 0);    // Always false
                }

                nameVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                dataTypeVec.setValueCount(rowCount);
                nullableVec.setValueCount(rowCount);
                partitionVec.setValueCount(rowCount);
                bucketVec.setValueCount(rowCount);
                clusterVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listColumns('{}') returned {} columns", tableName, rowCount);
            }
        }
    }

    /**
     * Handle LIST_DATABASES operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - catalog: string (always "default")
     * - description: string (empty)
     * - locationUri: string (empty)
     */
    private void handleListDatabases(ListDatabases listDatabases, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String pattern = listDatabases.hasPattern() ? listDatabases.getPattern() : null;

        logger.info("Listing databases (pattern={})", pattern);

        // Build query (DISTINCT to avoid duplicates in DuckDB's information_schema)
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE ");
        sql.append("schema_name NOT IN ('information_schema', 'pg_catalog')");
        if (pattern != null) {
            sql.append(" AND schema_name LIKE '").append(escapeSql(pattern)).append("'");
        }
        sql.append(" ORDER BY schema_name");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListDatabasesSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector catalogVec = (VarCharVector) result.getVector("catalog");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector locVec = (VarCharVector) result.getVector("locationUri");

                nameVec.allocateNew(rowCount);
                catalogVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                locVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("schema_name");

                for (int i = 0; i < rowCount; i++) {
                    String schemaName = new String(srcName.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, schemaName.getBytes(StandardCharsets.UTF_8));
                    catalogVec.set(i, "default".getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                    locVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                }

                nameVec.setValueCount(rowCount);
                catalogVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                locVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listDatabases returned {} databases", rowCount);
            }
        }
    }

    /**
     * Handle DATABASE_EXISTS operation.
     */
    private void handleDatabaseExists(DatabaseExists databaseExists, Session session,
                                      StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = databaseExists.getDbName();

        logger.info("Checking database exists: '{}'", dbName);

        // Build query
        String sql = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata " +
                    "WHERE schema_name = '" + escapeSql(dbName) + "' " +
                    "AND schema_name NOT IN ('information_schema', 'pg_catalog')) AS result";

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            // Return boolean result
            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamBooleanResult(exists);

            logger.info("✓ databaseExists('{}') = {}", dbName, exists);
        }
    }

    /**
     * Handle CURRENT_DATABASE operation.
     */
    private void handleCurrentDatabase(Session session,
                                       StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        // Get current database from session config, default to "main"
        String currentDb = session.getConfig("spark.catalog.currentDatabase");
        if (currentDb == null) {
            currentDb = "main";
        }

        logger.info("Current database: '{}'", currentDb);

        // Return as single-row Arrow table with "value" column
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("value", ArrowType.Utf8.INSTANCE)
            ));
            VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
            result.setRowCount(1);

            VarCharVector vec = (VarCharVector) result.getVector("value");
            vec.allocateNew(1);
            vec.set(0, currentDb.getBytes(StandardCharsets.UTF_8));
            vec.setValueCount(1);

            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamArrowResult(result);

            logger.info("✓ currentDatabase() = '{}'", currentDb);
        }
    }

    /**
     * Handle SET_CURRENT_DATABASE operation.
     */
    private void handleSetCurrentDatabase(SetCurrentDatabase setCurrentDb, Session session,
                                          StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = setCurrentDb.getDbName();

        logger.info("Setting current database to: '{}'", dbName);

        // Validate database exists
        String sql = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata " +
                    "WHERE schema_name = '" + escapeSql(dbName) + "') AS result";

        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            if (!exists) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Database not found: " + dbName)
                    .asRuntimeException());
                return;
            }
        }

        // Set in session config
        session.setConfig("spark.catalog.currentDatabase", dbName);

        // Set search path in DuckDB
        executor.execute("SET search_path TO " + quoteIdentifier(dbName));

        // Return empty success response
        String operationId = UUID.randomUUID().toString();
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(session.getSessionId())
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.info("✓ setCurrentDatabase('{}') completed", dbName);
    }

    /**
     * Handle IS_CACHED operation.
     * DuckDB doesn't have Spark-like caching, so always return false.
     */
    private void handleIsCached(IsCached isCached, Session session,
                                StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = isCached.getTableName();
        logger.info("isCached('{}') - DuckDB has no caching, returning false", tableName);

        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamBooleanResult(false);

        logger.info("✓ isCached('{}') = false", tableName);
    }

    /**
     * Handle CACHE_TABLE operation.
     * DuckDB doesn't have Spark-like caching, so this is a no-op.
     */
    private void handleCacheTable(CacheTable cacheTable, Session session,
                                  StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = cacheTable.getTableName();
        logger.warn("cacheTable('{}') - DuckDB has no caching, operation ignored", tableName);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ cacheTable('{}') completed (no-op)", tableName);
    }

    /**
     * Handle UNCACHE_TABLE operation.
     * DuckDB doesn't have Spark-like caching, so this is a no-op.
     */
    private void handleUncacheTable(UncacheTable uncacheTable, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = uncacheTable.getTableName();
        logger.warn("uncacheTable('{}') - DuckDB has no caching, operation ignored", tableName);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ uncacheTable('{}') completed (no-op)", tableName);
    }

    /**
     * Handle CLEAR_CACHE operation.
     * DuckDB doesn't have Spark-like caching, so this is a no-op.
     */
    private void handleClearCache(Session session,
                                  StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        logger.warn("clearCache() - DuckDB has no caching, operation ignored");

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ clearCache() completed (no-op)");
    }

    /**
     * Handle REFRESH_TABLE operation.
     * DuckDB doesn't require table refresh, so this is a no-op.
     */
    private void handleRefreshTable(RefreshTable refreshTable, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = refreshTable.getTableName();
        logger.info("refreshTable('{}') - no-op for DuckDB", tableName);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ refreshTable('{}') completed (no-op)", tableName);
    }

    /**
     * Handle REFRESH_BY_PATH operation.
     * DuckDB doesn't require path-based refresh, so this is a no-op.
     */
    private void handleRefreshByPath(RefreshByPath refreshByPath, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String path = refreshByPath.getPath();
        logger.info("refreshByPath('{}') - no-op for DuckDB", path);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ refreshByPath('{}') completed (no-op)", path);
    }

    /**
     * Handle RECOVER_PARTITIONS operation.
     * DuckDB doesn't have Hive-style partitions, so this is a no-op.
     */
    private void handleRecoverPartitions(RecoverPartitions recoverPartitions, Session session,
                                         StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = recoverPartitions.getTableName();
        logger.info("recoverPartitions('{}') - no-op for DuckDB", tableName);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ recoverPartitions('{}') completed (no-op)", tableName);
    }

    /**
     * Handle CURRENT_CATALOG operation.
     * DuckDB doesn't have multiple catalogs, so always return "default".
     */
    private void handleCurrentCatalog(Session session,
                                      StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String currentCatalog = "default";
        logger.info("Current catalog: '{}'", currentCatalog);

        // Return as single-row Arrow table with "value" column
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("value", ArrowType.Utf8.INSTANCE)
            ));
            VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
            result.setRowCount(1);

            VarCharVector vec = (VarCharVector) result.getVector("value");
            vec.allocateNew(1);
            vec.set(0, currentCatalog.getBytes(StandardCharsets.UTF_8));
            vec.setValueCount(1);

            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamArrowResult(result);

            logger.info("✓ currentCatalog() = '{}'", currentCatalog);
        }
    }

    /**
     * Handle SET_CURRENT_CATALOG operation.
     * DuckDB doesn't have multiple catalogs, so only accept "default".
     */
    private void handleSetCurrentCatalog(SetCurrentCatalog setCurrentCatalog, Session session,
                                         StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String catalogName = setCurrentCatalog.getCatalogName();
        logger.info("Setting current catalog to: '{}'", catalogName);

        // Only accept "default"
        if (!"default".equalsIgnoreCase(catalogName)) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Catalog not found: " + catalogName + " (only 'default' is supported)")
                .asRuntimeException());
            return;
        }

        // Store in session config (for consistency)
        session.setConfig("spark.catalog.currentCatalog", catalogName);

        // Return void result (empty Arrow batch)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ setCurrentCatalog('{}') completed", catalogName);
    }

    /**
     * Handle LIST_CATALOGS operation.
     * DuckDB doesn't have multiple catalogs, so return single row with "default".
     *
     * Returns Arrow table with columns:
     * - name: string
     * - description: string (empty)
     */
    private void handleListCatalogs(ListCatalogs listCatalogs, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String pattern = listCatalogs.hasPattern() ? listCatalogs.getPattern() : null;
        logger.info("Listing catalogs (pattern={})", pattern);

        // Check if pattern matches "default"
        boolean includeDefault = pattern == null || "default".matches(pattern.replace("%", ".*"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("name", ArrowType.Utf8.INSTANCE),
                Field.nullable("description", ArrowType.Utf8.INSTANCE)
            ));
            VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

            int rowCount = includeDefault ? 1 : 0;
            result.setRowCount(rowCount);

            VarCharVector nameVec = (VarCharVector) result.getVector("name");
            VarCharVector descVec = (VarCharVector) result.getVector("description");

            nameVec.allocateNew(rowCount);
            descVec.allocateNew(rowCount);

            if (includeDefault) {
                nameVec.set(0, "default".getBytes(StandardCharsets.UTF_8));
                descVec.set(0, "".getBytes(StandardCharsets.UTF_8));
            }

            nameVec.setValueCount(rowCount);
            descVec.setValueCount(rowCount);

            // Stream result
            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamArrowResult(result);

            logger.info("✓ listCatalogs returned {} catalogs", rowCount);
        }
    }

    /**
     * Handle DROP_GLOBAL_TEMP_VIEW operation.
     * DuckDB doesn't have global temp views, so treat this the same as DROP_TEMP_VIEW.
     */
    private void handleDropGlobalTempView(DropGlobalTempView dropGlobalTempView, Session session,
                                          StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String viewName = dropGlobalTempView.getViewName();
        logger.info("Dropping global temp view: '{}' (treating as regular temp view)", viewName);

        // Remove from session's temp view registry
        boolean existed = session.dropTempView(viewName);

        // Drop the view from DuckDB
        String dropViewSQL = String.format("DROP VIEW IF EXISTS %s", quoteIdentifier(viewName));
        logger.debug("Executing DuckDB: {}", dropViewSQL);

        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        executor.execute(dropViewSQL);

        // Return boolean result
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamBooleanResult(existed);

        logger.info("✓ Global temp view dropped from DuckDB: '{}' (existed={}, session: {})",
            viewName, existed, session.getSessionId());
    }

    /**
     * Handle CREATE_TABLE operation.
     *
     * <p>Creates a persistent table in the session's DuckDB database.
     * Internal tables (no path/source) are stored directly in DuckDB.
     * External tables (with path/source) are not yet supported.
     */
    private void handleCreateTable(CreateTable createTable, Session session,
                                   StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = createTable.getTableName();
        boolean hasPath = createTable.hasPath();
        boolean hasSource = createTable.hasSource();

        logger.info("CREATE TABLE: '{}' (path={}, source={})",
            tableName,
            hasPath ? createTable.getPath() : "none",
            hasSource ? createTable.getSource() : "none");

        // External tables (with path or source) are not yet supported
        if (hasPath || hasSource) {
            responseObserver.onError(Status.UNIMPLEMENTED
                .withDescription("External tables (with path or source) are not yet supported. " +
                    "Use internal tables without path/source specification.")
                .asRuntimeException());
            return;
        }

        // Schema is required for internal tables
        if (!createTable.hasSchema()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("CREATE TABLE requires a schema for internal tables")
                .asRuntimeException());
            return;
        }

        // Validate schema is a STRUCT type
        DataType schemaType = createTable.getSchema();
        if (schemaType.getKindCase() != DataType.KindCase.STRUCT) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Schema must be a STRUCT type, got: " + schemaType.getKindCase())
                .asRuntimeException());
            return;
        }

        // Get current database (schema in DuckDB terms)
        String currentDb = session.getConfig("spark.catalog.currentDatabase");
        if (currentDb == null || currentDb.isEmpty()) {
            currentDb = "main";
        }

        // Generate CREATE TABLE DDL
        String ddl = SparkDataTypeConverter.generateCreateTableDDL(
            currentDb,
            tableName,
            schemaType.getStruct()
        );

        logger.debug("Executing DuckDB: {}", ddl);

        // Execute the DDL
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        executor.execute(ddl);

        // Return void result (Spark's createTable returns void)
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamVoidResult();

        logger.info("✓ Table created: '{}.{}' (session: {})", currentDb, tableName, session.getSessionId());
    }

    // ==================== Helper Methods ====================

    /**
     * Create Arrow schema for listTables result.
     */
    private Schema createListTablesSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("catalog", ArrowType.Utf8.INSTANCE),
            Field.nullable("namespace", ArrowType.Utf8.INSTANCE),  // JSON array string
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("tableType", ArrowType.Utf8.INSTANCE),
            Field.nullable("isTemporary", ArrowType.Bool.INSTANCE)
        ));
    }

    /**
     * Create Arrow schema for listColumns result.
     */
    private Schema createListColumnsSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("dataType", ArrowType.Utf8.INSTANCE),
            Field.nullable("nullable", ArrowType.Bool.INSTANCE),
            Field.nullable("isPartition", ArrowType.Bool.INSTANCE),
            Field.nullable("isBucket", ArrowType.Bool.INSTANCE),
            Field.nullable("isCluster", ArrowType.Bool.INSTANCE)
        ));
    }

    /**
     * Create Arrow schema for listDatabases result.
     */
    private Schema createListDatabasesSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("catalog", ArrowType.Utf8.INSTANCE),
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("locationUri", ArrowType.Utf8.INSTANCE)
        ));
    }

    /**
     * Map DuckDB table type to Spark table type.
     */
    private String mapTableType(String duckdbType) {
        if (duckdbType == null) return "MANAGED";
        switch (duckdbType.toUpperCase()) {
            case "VIEW":
                return "VIEW";
            case "BASE TABLE":
                return "MANAGED";
            case "EXTERNAL TABLE":
                return "EXTERNAL";
            default:
                return "MANAGED";
        }
    }

    /**
     * Escape single quotes in SQL strings.
     */
    private String escapeSql(String value) {
        if (value == null) return null;
        return value.replace("'", "''");
    }
}
