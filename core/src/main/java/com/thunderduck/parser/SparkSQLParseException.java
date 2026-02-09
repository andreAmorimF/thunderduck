package com.thunderduck.parser;

/**
 * Exception thrown when SparkSQL parsing fails.
 *
 * <p>Contains position information (line, column) and the offending token
 * to help users locate and fix syntax errors in their SQL.
 */
public class SparkSQLParseException extends RuntimeException {

    private final int line;
    private final int charPositionInLine;
    private final String offendingToken;

    /**
     * Creates a parse exception.
     *
     * @param line the line number (1-based)
     * @param charPositionInLine the column position (0-based)
     * @param offendingToken the token that caused the error
     * @param message the error message
     */
    public SparkSQLParseException(int line, int charPositionInLine,
                                   String offendingToken, String message) {
        super(message);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
        this.offendingToken = offendingToken;
    }

    /**
     * Creates a parse exception with a cause.
     *
     * @param line the line number (1-based)
     * @param charPositionInLine the column position (0-based)
     * @param offendingToken the token that caused the error
     * @param message the error message
     * @param cause the underlying cause
     */
    public SparkSQLParseException(int line, int charPositionInLine,
                                   String offendingToken, String message, Throwable cause) {
        super(message, cause);
        this.line = line;
        this.charPositionInLine = charPositionInLine;
        this.offendingToken = offendingToken;
    }

    public int line() {
        return line;
    }

    public int charPositionInLine() {
        return charPositionInLine;
    }

    public String offendingToken() {
        return offendingToken;
    }
}
