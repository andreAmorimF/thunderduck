package com.thunderduck.connect.session;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single Spark Connect session.
 *
 * Each session maintains:
 * - Unique session ID
 * - Session-scoped configuration
 * - Creation timestamp
 */
public class Session {
    private final String sessionId;
    private final long createdAt;
    private final Map<String, String> config;

    /**
     * Create a new session with the given ID.
     *
     * @param sessionId Unique session identifier (typically UUID from client)
     */
    public Session(String sessionId) {
        this.sessionId = sessionId;
        this.createdAt = System.currentTimeMillis();
        this.config = new HashMap<>();

        // Set default configuration
        config.put("spark.app.name", "thunderduck-connect");
        config.put("spark.sql.dialect", "spark");
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

    @Override
    public String toString() {
        return String.format("Session[id=%s, created=%d]", sessionId, createdAt);
    }
}
