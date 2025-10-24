package com.thunderduck.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

import java.util.logging.Logger;

/**
 * Base class for all test classes.
 *
 * <p>Provides common test setup, teardown, and utility methods.
 * All test classes should extend this base class to inherit:
 * <ul>
 *   <li>Consistent test logging</li>
 *   <li>Resource cleanup</li>
 *   <li>Test lifecycle hooks</li>
 *   <li>Common assertion utilities</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * public class MyTest extends TestBase {
 *     {@literal @}Test
 *     public void testSomething() {
 *         // Test implementation
 *     }
 * }
 * </pre>
 */
public abstract class TestBase {

    protected static final Logger logger = Logger.getLogger(TestBase.class.getName());

    /**
     * Setup method run before each test.
     *
     * @param testInfo JUnit 5 test information
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) {
        String testName = testInfo.getDisplayName();
        String className = testInfo.getTestClass()
            .map(Class::getSimpleName)
            .orElse("UnknownClass");

        logger.info(String.format("Starting test: %s.%s", className, testName));

        // Subclasses can override this method to add custom setup
        doSetUp();
    }

    /**
     * Teardown method run after each test.
     *
     * @param testInfo JUnit 5 test information
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) {
        String testName = testInfo.getDisplayName();
        String className = testInfo.getTestClass()
            .map(Class::getSimpleName)
            .orElse("UnknownClass");

        logger.info(String.format("Completed test: %s.%s", className, testName));

        // Subclasses can override this method to add custom teardown
        doTearDown();
    }

    /**
     * Custom setup hook for subclasses.
     * Override this method to add test-specific setup logic.
     */
    protected void doSetUp() {
        // Default: no-op
    }

    /**
     * Custom teardown hook for subclasses.
     * Override this method to add test-specific cleanup logic.
     */
    protected void doTearDown() {
        // Default: no-op
    }

    /**
     * Helper method to create a test description in BDD style.
     *
     * @param given the given condition
     * @param when the action
     * @param then the expected result
     * @return a formatted test description
     */
    protected String bddDescription(String given, String when, String then) {
        return String.format("Given %s, when %s, then %s", given, when, then);
    }

    /**
     * Helper method to log a test step.
     *
     * @param step the step description
     */
    protected void logStep(String step) {
        logger.info("  Step: " + step);
    }

    /**
     * Helper method to log test data.
     *
     * @param label the data label
     * @param value the data value
     */
    protected void logData(String label, Object value) {
        logger.info(String.format("  Data: %s = %s", label, value));
    }

    /**
     * Executes a test scenario in Given-When-Then style.
     *
     * @param <T> the result type
     * @param given the given condition (setup)
     * @param when the action to perform
     * @param then the assertion to verify
     */
    protected <T> void executeScenario(
        Runnable given,
        java.util.function.Supplier<T> when,
        java.util.function.Consumer<T> then) {

        // Given
        logStep("Given: Setting up test conditions");
        if (given != null) {
            given.run();
        }

        // When
        logStep("When: Executing test action");
        T result = when.get();

        // Then
        logStep("Then: Verifying expectations");
        then.accept(result);
    }

    /**
     * Executes a simple test scenario without a given clause.
     *
     * @param <T> the result type
     * @param when the action to perform
     * @param then the assertion to verify
     */
    protected <T> void executeScenario(
        java.util.function.Supplier<T> when,
        java.util.function.Consumer<T> then) {
        executeScenario(null, when, then);
    }

    /**
     * Measures execution time of a test operation.
     *
     * @param operation the operation to measure
     * @return the execution time in milliseconds
     */
    protected long measureExecutionTime(Runnable operation) {
        long startTime = System.currentTimeMillis();
        operation.run();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        logData("Execution time", duration + " ms");
        return duration;
    }

    /**
     * Asserts that an operation completes within a time limit.
     *
     * @param operation the operation to execute
     * @param maxMillis the maximum allowed time in milliseconds
     */
    protected void assertCompletesWithin(Runnable operation, long maxMillis) {
        long duration = measureExecutionTime(operation);
        if (duration > maxMillis) {
            throw new AssertionError(
                String.format("Operation took %d ms, expected < %d ms", duration, maxMillis)
            );
        }
    }

    /**
     * Retries an operation a specified number of times if it fails.
     *
     * @param operation the operation to retry
     * @param maxAttempts the maximum number of attempts
     * @param delayMillis the delay between attempts in milliseconds
     * @throws AssertionError if all attempts fail
     */
    protected void retry(Runnable operation, int maxAttempts, long delayMillis) {
        AssertionError lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                logger.info(String.format("Attempt %d/%d", attempt, maxAttempts));
                operation.run();
                return; // Success
            } catch (AssertionError e) {
                lastError = e;
                if (attempt < maxAttempts) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("Retry interrupted", ie);
                    }
                }
            }
        }
        throw new AssertionError("All retry attempts failed", lastError);
    }
}
