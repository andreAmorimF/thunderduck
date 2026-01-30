package com.thunderduck.schema;

/**
 * Exception thrown when schema inference fails.
 */
public class SchemaInferenceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SchemaInferenceException(String message) {
        super(message);
    }

    public SchemaInferenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
