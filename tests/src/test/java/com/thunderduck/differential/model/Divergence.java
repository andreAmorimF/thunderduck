package com.thunderduck.differential.model;

/**
 * Represents a divergence between Spark and thunderduck execution.
 */
public class Divergence {
    public enum Type {
        SCHEMA_MISMATCH,
        ROW_COUNT_MISMATCH,
        DATA_MISMATCH,
        NULL_HANDLING,
        NUMERICAL_PRECISION,
        EXECUTION_ERROR
    }

    public enum Severity {
        CRITICAL,  // Completely wrong results
        HIGH,      // Significant differences
        MEDIUM,    // Minor differences that might matter
        LOW        // Cosmetic differences
    }

    private final Type type;
    private final Severity severity;
    private final String description;
    private final Object sparkValue;
    private final Object thunderduckValue;

    public Divergence(Type type, Severity severity, String description,
                      Object sparkValue, Object thunderduckValue) {
        this.type = type;
        this.severity = severity;
        this.description = description;
        this.sparkValue = sparkValue;
        this.thunderduckValue = thunderduckValue;
    }

    public Type getType() {
        return type;
    }

    public Severity getSeverity() {
        return severity;
    }

    public String getDescription() {
        return description;
    }

    public Object getSparkValue() {
        return sparkValue;
    }

    public Object getCatalyst2sqlValue() {
        return thunderduckValue;
    }

    @Override
    public String toString() {
        return String.format("[%s - %s] %s\n  Spark: %s\n  Thunderduck: %s",
            severity, type, description, sparkValue, thunderduckValue);
    }
}
