package com.thunderduck.exception;

/**
 * Exception thrown when query validation fails.
 *
 * <p>Provides actionable error messages with context for debugging. This exception
 * is used by {@link com.thunderduck.validation.QueryValidator} to report validation
 * failures during logical plan analysis before SQL generation.
 *
 * <p>Unlike {@link SQLGenerationException}, which is thrown during SQL generation,
 * {@link ValidationException} is thrown during the validation phase to catch errors
 * early and provide better error messages.
 *
 * <p>Common validation failures:
 * <ul>
 *   <li>JOIN without condition for non-CROSS joins</li>
 *   <li>JOIN condition referencing non-existent columns</li>
 *   <li>UNION with incompatible schemas (different column counts or types)</li>
 *   <li>Aggregate without aggregate expressions</li>
 *   <li>Non-grouped columns in SELECT not in GROUP BY clause</li>
 *   <li>Window functions missing required ORDER BY clause</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   try {
 *       QueryValidator.validate(plan);
 *       String sql = generator.generate(plan);
 *   } catch (ValidationException e) {
 *       System.err.println(e.getMessage());
 *       System.err.println("Phase: " + e.phase());
 *       System.err.println("Invalid element: " + e.invalidElement());
 *       System.err.println("Suggestion: " + e.suggestion());
 *   }
 * </pre>
 *
 * <p>Error message format:
 * <pre>
 * Validation failed during [phase]: [message]
 *   Invalid element: [invalidElement]
 *   Suggestion: [suggestion]
 * </pre>
 *
 * @see com.thunderduck.validation.QueryValidator
 * @see SQLGenerationException
 */
public class ValidationException extends RuntimeException {

    private final String phase;
    private final String invalidElement;
    private final String suggestion;

    /**
     * Creates a validation exception.
     *
     * <p>The complete error message is formatted automatically from the components.
     *
     * @param message the error message describing what failed
     * @param phase the validation phase where the error occurred (e.g., "join validation", "aggregate validation")
     * @param invalidElement the specific element that failed validation (may be null)
     * @param suggestion actionable advice for fixing the error (may be null)
     */
    public ValidationException(String message, String phase, String invalidElement, String suggestion) {
        super(formatMessage(message, phase, invalidElement, suggestion));
        this.phase = phase;
        this.invalidElement = invalidElement;
        this.suggestion = suggestion;
    }

    /**
     * Formats the complete error message from components.
     *
     * @param message the error message
     * @param phase the validation phase
     * @param invalidElement the invalid element
     * @param suggestion the suggestion for fixing
     * @return formatted error message
     */
    private static String formatMessage(String message, String phase, String invalidElement, String suggestion) {
        StringBuilder sb = new StringBuilder();
        sb.append("Validation failed during ").append(phase).append(": ");
        sb.append(message);

        if (invalidElement != null) {
            sb.append("\n  Invalid element: ").append(invalidElement);
        }

        if (suggestion != null) {
            sb.append("\n  Suggestion: ").append(suggestion);
        }

        return sb.toString();
    }

    /**
     * Returns the validation phase where the error occurred.
     *
     * <p>Examples: "join validation", "union validation", "aggregate validation",
     * "window function validation", "plan validation"
     *
     * @return the validation phase
     */
    public String phase() {
        return phase;
    }

    /**
     * Returns the specific element that failed validation.
     *
     * <p>This is typically a string representation of the invalid logical plan
     * node or expression.
     *
     * @return the invalid element, or null if not available
     */
    public String invalidElement() {
        return invalidElement;
    }

    /**
     * Returns actionable advice for fixing the validation error.
     *
     * <p>Examples:
     * <ul>
     *   <li>"Add ON clause with join condition or use CROSS JOIN"</li>
     *   <li>"Ensure both sides of UNION have same column count"</li>
     *   <li>"Add 'customer_id' to GROUP BY clause or wrap in aggregate function"</li>
     * </ul>
     *
     * @return the suggestion, or null if not available
     */
    public String suggestion() {
        return suggestion;
    }

    /**
     * Returns a user-friendly error message for display.
     *
     * <p>This is the same as {@link #getMessage()} since the message is already
     * formatted for user consumption.
     *
     * @return user-friendly error message
     */
    public String getUserMessage() {
        return getMessage();
    }

    /**
     * Returns a detailed technical message for debugging.
     *
     * <p>Includes all validation context and stack trace information.
     *
     * @return technical error message with full context
     */
    public String getTechnicalMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query Validation Failed\n");
        sb.append("Error: ").append(getMessage()).append("\n");
        sb.append("Phase: ").append(phase).append("\n");

        if (invalidElement != null) {
            sb.append("Invalid Element: ").append(invalidElement).append("\n");
        }

        if (suggestion != null) {
            sb.append("Suggestion: ").append(suggestion).append("\n");
        }

        if (getCause() != null) {
            sb.append("Cause: ").append(getCause().getClass().getName()).append("\n");
            sb.append("Cause Message: ").append(getCause().getMessage()).append("\n");
        }

        return sb.toString();
    }
}
