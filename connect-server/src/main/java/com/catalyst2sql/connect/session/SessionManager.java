package com.catalyst2sql.connect.session;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages single-session lifecycle for DuckDB Spark Connect Server.
 *
 * Since DuckDB is single-user, we only allow one active session at a time.
 * State transitions: IDLE <-> ACTIVE
 *
 * Features:
 * - Singleton pattern for DuckDB connection
 * - Single active session enforcement
 * - Automatic session timeout
 * - Connection rejection when busy
 */
public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    /** Server state */
    private enum ServerState {
        IDLE,   // No active session, ready to accept connections
        ACTIVE  // Session active, reject new connections
    }

    /** Current server state */
    private ServerState state = ServerState.IDLE;

    /** Currently active session ID (null when IDLE) */
    private String currentSessionId = null;

    /** Last activity timestamp (milliseconds since epoch) */
    private long lastActivityTime = 0;

    /** Session timeout in milliseconds (default: 300 seconds) */
    private final long sessionTimeoutMs;

    /** Background thread for checking session timeout */
    private final ScheduledExecutorService timeoutChecker;

    /** Lock for synchronization */
    private final Object lock = new Object();

    /**
     * Create SessionManager with default timeout (300 seconds).
     */
    public SessionManager() {
        this(300_000); // 5 minutes default
    }

    /**
     * Create SessionManager with custom timeout.
     *
     * @param sessionTimeoutMs Session timeout in milliseconds
     */
    public SessionManager(long sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.timeoutChecker = Executors.newScheduledThreadPool(1);

        // Check for timeout every 10 seconds
        timeoutChecker.scheduleAtFixedRate(
            this::checkTimeout,
            10, 10, TimeUnit.SECONDS
        );

        logger.info("SessionManager initialized with timeout: {}ms", sessionTimeoutMs);
    }

    /**
     * Create a new session. Throws RESOURCE_EXHAUSTED if server is busy.
     *
     * @param sessionId Unique session identifier from client
     * @return Session object
     * @throws StatusRuntimeException if server is busy with another session
     */
    public Session createSession(String sessionId) throws StatusRuntimeException {
        synchronized (lock) {
            if (state == ServerState.ACTIVE) {
                logger.warn("Rejecting session creation for '{}' - server busy with session '{}'",
                    sessionId, currentSessionId);
                throw new StatusRuntimeException(
                    Status.RESOURCE_EXHAUSTED
                        .withDescription("Server busy: another session '" + currentSessionId + "' is active")
                );
            }

            // Transition to ACTIVE state
            state = ServerState.ACTIVE;
            currentSessionId = sessionId;
            lastActivityTime = System.currentTimeMillis();

            logger.info("Session created: {} (state: IDLE -> ACTIVE)", sessionId);
            return new Session(sessionId);
        }
    }

    /**
     * Update last activity time for current session.
     * Called on every RPC to prevent timeout.
     *
     * @param sessionId Session ID making the request
     * @throws StatusRuntimeException if session ID doesn't match current session
     */
    public void updateActivity(String sessionId) throws StatusRuntimeException {
        synchronized (lock) {
            if (state == ServerState.IDLE) {
                throw new StatusRuntimeException(
                    Status.FAILED_PRECONDITION
                        .withDescription("No active session - session may have timed out")
                );
            }

            if (!sessionId.equals(currentSessionId)) {
                throw new StatusRuntimeException(
                    Status.PERMISSION_DENIED
                        .withDescription("Session ID mismatch: expected '" + currentSessionId +
                            "' but got '" + sessionId + "'")
                );
            }

            lastActivityTime = System.currentTimeMillis();
            logger.debug("Activity updated for session: {}", sessionId);
        }
    }

    /**
     * Close the current session and return to IDLE state.
     *
     * @param sessionId Session ID to close
     * @throws StatusRuntimeException if session ID doesn't match
     */
    public void closeSession(String sessionId) throws StatusRuntimeException {
        synchronized (lock) {
            if (state == ServerState.IDLE) {
                logger.warn("Attempted to close session '{}' but server is already IDLE", sessionId);
                return;
            }

            if (!sessionId.equals(currentSessionId)) {
                throw new StatusRuntimeException(
                    Status.PERMISSION_DENIED
                        .withDescription("Cannot close session: expected '" + currentSessionId +
                            "' but got '" + sessionId + "'")
                );
            }

            // Transition to IDLE state
            state = ServerState.IDLE;
            currentSessionId = null;
            lastActivityTime = 0;

            logger.info("Session closed: {} (state: ACTIVE -> IDLE)", sessionId);
        }
    }

    /**
     * Check if session has timed out and close it if necessary.
     * Called periodically by background thread.
     */
    private void checkTimeout() {
        synchronized (lock) {
            if (state == ServerState.ACTIVE) {
                long idleTime = System.currentTimeMillis() - lastActivityTime;

                if (idleTime > sessionTimeoutMs) {
                    logger.warn("Session '{}' timed out after {}ms of inactivity (timeout: {}ms)",
                        currentSessionId, idleTime, sessionTimeoutMs);

                    // Force close the session
                    state = ServerState.IDLE;
                    String oldSessionId = currentSessionId;
                    currentSessionId = null;
                    lastActivityTime = 0;

                    logger.info("Session '{}' forcibly closed due to timeout (state: ACTIVE -> IDLE)",
                        oldSessionId);
                }
            }
        }
    }

    /**
     * Get current server state (for monitoring/testing).
     *
     * @return Current state and session info
     */
    public SessionInfo getSessionInfo() {
        synchronized (lock) {
            return new SessionInfo(
                state == ServerState.ACTIVE,
                currentSessionId,
                lastActivityTime,
                state == ServerState.ACTIVE
                    ? System.currentTimeMillis() - lastActivityTime
                    : 0
            );
        }
    }

    /**
     * Shutdown the session manager and cleanup resources.
     */
    public void shutdown() {
        logger.info("Shutting down SessionManager");
        timeoutChecker.shutdown();
        try {
            if (!timeoutChecker.awaitTermination(5, TimeUnit.SECONDS)) {
                timeoutChecker.shutdownNow();
            }
        } catch (InterruptedException e) {
            timeoutChecker.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Immutable snapshot of session state (for monitoring).
     */
    public static class SessionInfo {
        public final boolean hasActiveSession;
        public final String sessionId;
        public final long lastActivityTime;
        public final long idleTimeMs;

        public SessionInfo(boolean hasActiveSession, String sessionId,
                          long lastActivityTime, long idleTimeMs) {
            this.hasActiveSession = hasActiveSession;
            this.sessionId = sessionId;
            this.lastActivityTime = lastActivityTime;
            this.idleTimeMs = idleTimeMs;
        }

        @Override
        public String toString() {
            if (hasActiveSession) {
                return String.format("Session[id=%s, idle=%dms]", sessionId, idleTimeMs);
            } else {
                return "Session[IDLE]";
            }
        }
    }
}
