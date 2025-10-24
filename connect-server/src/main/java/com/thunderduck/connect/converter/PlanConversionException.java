package com.thunderduck.connect.converter;

/**
 * Exception thrown when plan conversion fails.
 *
 * <p>This exception is thrown when converting from Spark Connect protocol
 * buffers to thunderduck logical plans encounters an error or unsupported feature.
 */
public class PlanConversionException extends RuntimeException {

    public PlanConversionException(String message) {
        super(message);
    }

    public PlanConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}