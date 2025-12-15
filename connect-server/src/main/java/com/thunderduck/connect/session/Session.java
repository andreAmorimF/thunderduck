package com.thunderduck.connect.session;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.runtime.DuckDBRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a single Spark Connect session.
 *
 * <p>Each session maintains:
 * <ul>
 *   <li>Unique session ID</li>
 *   <li>DuckDB runtime (owns the database connection)</li>
 *   <li>Session-scoped configuration</li>
 *   <li>Temporary view registry</li>
 *   <li>Creation timestamp</li>
 * </ul>
 *
 * <p>The session owns its DuckDBRuntime and is responsible for closing it
 * when the session is closed. This ensures proper resource cleanup and
 * isolation between sessions.
 */
public class Session implements AutoCloseable {
    private final String sessionId;
    private final DuckDBRuntime runtime;
    private final long createdAt;
    private final Map<String, String> config;
    private final Map<String, LogicalPlan> tempViews;
    private volatile boolean closed = false;

    /**
     * Create a new session with the given ID and its own DuckDB runtime.
     *
     * <p>Creates a new in-memory DuckDB database named after the session ID.
     *
     * @param sessionId Unique session identifier (typically UUID from client)
     */
    public Session(String sessionId) {
        this(sessionId, DuckDBRuntime.create("jdbc:duckdb::memory:" + sanitizeSessionId(sessionId)));
    }

    /**
     * Create a new session with the given ID and DuckDB runtime.
     *
     * <p>This constructor allows injecting a custom runtime, useful for testing.
     *
     * @param sessionId Unique session identifier
     * @param runtime DuckDB runtime for this session
     */
    public Session(String sessionId, DuckDBRuntime runtime) {
        this.sessionId = sessionId;
        this.runtime = runtime;
        this.createdAt = System.currentTimeMillis();
        this.config = new HashMap<>();
        this.tempViews = new ConcurrentHashMap<>();

        // Set default configuration
        config.putAll(SparkDefaults.getDefaults());
        config.put("spark.app.name", "thunderduck-connect");
        config.put("spark.sql.dialect", "spark");
    }

    /**
     * Sanitize session ID for use in DuckDB database name.
     *
     * <p>DuckDB database names have restrictions, so we sanitize the session ID
     * to ensure it's valid.
     *
     * @param sessionId Original session ID
     * @return Sanitized session ID safe for database naming
     */
    private static String sanitizeSessionId(String sessionId) {
        // Replace non-alphanumeric chars with underscore, limit length
        return sessionId.replaceAll("[^a-zA-Z0-9]", "_").substring(0, Math.min(sessionId.length(), 50));
    }

    /**
     * Get session ID.
     *
     * @return Session identifier
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get the DuckDB runtime for this session.
     *
     * @return DuckDB runtime
     * @throws IllegalStateException if session is closed
     */
    public DuckDBRuntime getRuntime() {
        if (closed) {
            throw new IllegalStateException("Session is closed: " + sessionId);
        }
        return runtime;
    }

    /**
     * Get session creation timestamp.
     *
     * @return Timestamp in milliseconds since epoch
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Get configuration value.
     *
     * @param key Configuration key
     * @return Configuration value, or null if not set
     */
    public String getConfig(String key) {
        return config.get(key);
    }

    /**
     * Set configuration value.
     *
     * @param key Configuration key
     * @param value Configuration value
     */
    public void setConfig(String key, String value) {
        config.put(key, value);
    }

    /**
     * Get all configuration entries.
     *
     * @return Immutable copy of configuration map
     */
    public Map<String, String> getAllConfig() {
        return new HashMap<>(config);
    }

    /**
     * Register a temporary view.
     *
     * @param name View name
     * @param plan Logical plan representing the view
     */
    public void registerTempView(String name, LogicalPlan plan) {
        tempViews.put(name, plan);
    }

    /**
     * Get a temporary view by name.
     *
     * @param name View name
     * @return Logical plan if view exists, empty otherwise
     */
    public Optional<LogicalPlan> getTempView(String name) {
        return Optional.ofNullable(tempViews.get(name));
    }

    /**
     * Drop a temporary view.
     *
     * @param name View name
     * @return true if view existed and was dropped, false otherwise
     */
    public boolean dropTempView(String name) {
        return tempViews.remove(name) != null;
    }

    /**
     * Get all temporary view names.
     *
     * @return Set of view names
     */
    public Set<String> getTempViewNames() {
        return tempViews.keySet();
    }

    /**
     * Clear all temporary views.
     */
    public void clearTempViews() {
        tempViews.clear();
    }

    /**
     * Check if this session is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Close the session and release all resources.
     *
     * <p>Closes the DuckDB runtime and clears all temp views.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Clear temp views
        tempViews.clear();

        // Close DuckDB runtime
        if (runtime != null) {
            runtime.close();
        }
    }

    @Override
    public String toString() {
        return String.format("Session[id=%s, created=%d, views=%d, closed=%s]",
            sessionId, createdAt, tempViews.size(), closed);
    }
}
